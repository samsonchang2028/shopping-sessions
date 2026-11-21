# NOTES: Shopping Sessions pipeline

Read this cold. Spec is `rules.md`; where the build brief and `rules.md` differ, `rules.md` wins.

## How to test (all must be green before a commit is called submittable)

| What | Command | Notes |
| --- | --- | --- |
| Unit tests | `make unit` | ts macro (72 cases), price, byte-wise order, ASOF direction, credit/assist boundaries |
| 36 fixtures, local | `make fixtures-local` | `CHECK=1`, venv python |
| 36 fixtures, Docker | `make fixtures` | scorer flags via `scripts/docker_run.sh` |
| Reference on fixtures | `make fixtures-ref` | proves `ref/reference.py` against hand-written answers |
| Dev set | `make check-local` / `make run-dev check-dev` | `dev/` from `generate --seed 1 --events 1000000 --clicks 250000` |
| Fuzz | `make fuzz` | 10 seeds x 150k events, pipeline vs reference, exact diff; seeds 15 and 272 contain 2028-02-29 |

Setup: `python3 -m venv .venv && .venv/bin/pip install duckdb==1.5.6 numpy scipy polars pyarrow`;
generator: `cd generator && cargo build --release && cp target/release/generate ~/.local/bin/`
(`generator/` is the unzipped generator.zip, git-ignored; so are `dev/`, `dev-answer/`, all data).

## Interpretation calls (one line each, with the rules.md sentence)

- Header = first line of each file only; a later line equal to the header is data. "Every CSV file has a header row." (fixture L3 confirms)
- A line is valid iff it splits on `,` into exactly the family's arity and each field is quote-free or `"..."` with no inner quote. "every comma separates fields" / "with a quote anywhere except around a whole field, is malformed"
- After unquoting, `''` is NULL; `""` is NULL. "`\"\"` is an empty field ... An empty field means null."
- `\r` is data. "Only `\n` ends a line: a `\r` is an ordinary character of the field it is in"
- Text after the last `\n` is a (possibly unterminated) line; a trailing `\n` adds no line. Either way an empty line is malformed. "An empty line is malformed too."
- No trimming, no case folding anywhere, including `products.csv`/`campaigns.csv` lookups. "Strings are never normalized"
- F3 with more than 15 significant digits -> NULL: it is > year 33658, outside every window. "one that is very short ... or too long for a 64-bit integer is not malformed, only outside the window"
- F1/F2 years outside 1970..2100 -> NULL (outside every 2025-2028 window; keeps arithmetic non-negative). Same sentence.
- Window checks are after UTC conversion of F2. "subtract it to get UTC" + "An event's `ts` must satisfy `window_start <= ts < window_end`"
- Dedup key is the unquoted `event_id`, compared exactly (`e1` vs `e1 ` vs `e3\r` differ). "Dedup on `event_id`" + fixtures E19/E20
- `ingest_ts` compared by parsed ms in the reference and as a string in the fast path (always F1, so identical order). "`ingest_ts` is always valid F1"
- Price is validated only for `purchase`; for `view`/`cart` it is ignored even if malformed text. "For `view` and `cart`, `price_cents` is ignored whatever it holds."
- Price: digits only, leading zeros allowed, value 1..10,000,000. "`0250` is 250"
- Late = `ingest - ts > 86,400,000` ms strictly. "Exactly 24 hours is kept, and so is an `ingest_ts` before `ts`."
- Clicks dedup before validate; copies identical so any copy. "Copies of a click have identical values, so keep any one."
- Click `campaign_id` exact match against campaigns.csv; unknown drops the click (it neither credits nor assists). "Drop the click unless ... `campaign_id` is in `campaigns.csv`"
- Click window `[ws - 7d, we)`. "A click at exactly `window_start - 7 days` is in its window; one at `window_end` is not."
- Credit: latest click with `start - 7d <= ts <= start`; tie on ts -> byte-wise smallest click_id across all the person's IDs. Clicks rule 4.
- Assists include the crediting campaign; one per campaign per session; organic never. Clicks rule 7.
- campaign_daily has a row iff credited or assisted that day; assist-only rows have 0 credited. Outputs section.
- top_category: max summed purchase revenue in the person-day; tie -> byte-wise smallest; NULL with no purchase; `unknown` competes. Outputs table.
- Person name = byte-wise smallest ID of the link component, even if that ID never appears in a row. Linked IDs rule 3.
- Linked IDs: an ID appearing only in dropped events still joins via links (links are independent of events). "Links come before everything else"

No conflicts found between the brief and rules.md. Notes on the brief:
- Brief Stage A's `read_csv` raw-line trick: check (a) FAILS (DuckDB splits on bare `\r` in non-strict mode). Handled: strict mode raises on any `\r`, and we fall back to `read_text` + `string_split(chr(10))`. Generated data never has `\r` so the scored run takes the fast path. Probe results: empty lines -> NULL, header-only -> 0 rows, header skipped per file, spaces/tabs/`\x1f`/NUL/non-ASCII preserved, missing final newline fine, UTF-8 BOM stripped (only affects header line).
- `list_slice(x, 2, NULL)` returns NULL in DuckDB 1.5.6; use `x[2:]`.
- DuckDB INTEGER overflow raises an error (year >= 2038 seconds exceed int32), so all time arithmetic is BIGINT.

## Architecture (as built)

`pipeline/pipeline.py`, one in-memory DuckDB connection, every stage a `CREATE TEMP TABLE`, stage timings on stderr.
Stage A raw lines -> C events (parse+dedup via `arg_min(struct, ingest_ts)` hash aggregate, then validate) ->
F clicks (dedup, validate) -> D links -> universe of IDs ordered byte-wise (idx) -> scipy connected_components ->
min idx per component = person key, name = `uni.id[pk]` -> E events grouped by `(pk, ts)` -> lag/cumsum sessionize ->
sessions -> F credit: click points collapsed per `(pk, ts)` with `arg_min(campaign, click_id)`, `ASOF LEFT JOIN` ->
G assists: islands per `(pk, campaign)` (gap > 7 d starts a new island), lo/hi session via two ASOF joins, unnest range ->
H outputs as snappy Parquet, `day` DATE.

## Engines

- **native (default)**: `rust/src/main.rs`, binary `/app/ss`. Whole pipeline, writes Parquet itself. Exit code 3 means the
  dataset is outside its limits (window + 7 d >= 2^32 ms, campaigns x days > 100M cells, too many ids); `run.sh` then runs
  the DuckDB engine. Any other nonzero exit is a failure (no silent fallback). `ENGINE=duckdb` forces the DuckDB engine.
- **duckdb**: `pipeline/pipeline.py`. Slower, independent implementation; kept as fallback and as a third opinion in `make fuzz`
  (fuzz diffs BOTH engines against `ref/reference.py`).

Native design: mmap, chunk files at line boundaries (8 MB), own exact line grammar, records hash-partitioned into 256
buckets by event_id hash (dedup = sort by (hash, ingest), exact id comparison inside equal-hash groups, so collisions are
safe), user IDs hashed once and sharded into 256 cache-sized sets, byte-sorted universe, union-find with min-index root,
events packed as `person << 32 | (ts - ws)` and sorted once in parallel, one sequential pass makes sessions and user_daily,
clicks sorted by (person, ts, click_id bytes), credit by two-pointer merge, assists by per-(person, campaign) islands.

## Benchmarks (local, 8 threads, wall seconds)

| Version | 1M | 20M | 60M | peak RSS 20M / 60M |
| --- | --- | --- | --- | --- |
| duckdb engine, first correct | 4.4 | 65.6 | | 11.5 GB / |
| duckdb engine, sentinel reader | 4.4 | 38.8 | | 8.5 GB / |
| native, first | 0.35 (Docker 0.57) | 9.0 | | 5.1 GB / |
| native, sharded interning, thin LTO (ad91569) | Docker 0.26 | 6.3 (Docker 6.5) | 25.2 | 5.9 GB / 16.3 GB |
| native, hash-keyed IDs (d905796) | | 4.75 | | 5.1 GB / |
| native, 48 B records, hash-table dedup, prefix-sorted universe | Docker 0.25 | 3.7 (Docker 4.9) | not re-run | 4.9 GB / |

20M native stages: parse 1.2, dedup 1.0, clicks 0.3, universe 1.4, components 0.3, pack+sort events 1.3, sessions 0.2,
clicks sorted 0.4, credit+assists 0.1, write 0.1. 60M/20M = 4.0x (target <= 3.5x), parse is 4.3x: suspect page-cache and
memory pressure (5.4 GB input, 16 GB RSS); not investigated yet.

| native, SSE2 scanner + date cache (A/B, same load) | | 3.32 | | |
| native, middle-bit dedup buckets | | 3.10 | | |
| native, per-person-bucket pipeline + single-probe lookup (HEAD) | Docker 0.25 | 2.64 (Docker 2.95) | 11.6 | 5.0 GB / 13.8 GB |

NOTE: this laptop is shared (load average 5-8) and has 4 cores / 8 threads, so absolute timings drift by +-0.5 s between
runs. Compare builds only with A/B runs alternated in the same minute (min of 5), e.g. a loop over two binaries.

20M native stages now (cumulative s): parse 1.15, dedup 1.75, clicks 2.0, universe 2.3 (0.3 s: shard 0.15, tables 0.08, prefix sort 0.04),
components 2.4, events sorted 3.0, sessions+clicks sorted+credit+write 3.7. Biggest remaining: parse (about 1.1 s), events packed+sorted (0.65 s), dedup (0.6 s).

Latest 20M stage times (s): parse 0.85, dedup 0.44, clicks 0.18, universe 0.28, components 0.14, bucket by person 0.31,
per-bucket sort+sessions+credit+assists 0.34, write 0.12. 17 CPU-s user (was 23).

Single-thread per-line costs measured with a scratch level benchmark (pre-scanner): newline scan 17 ns, comma splitting
with one memchr per field ~105 ns (memchr call overhead on ~90-byte lines dominates), two timestamp parses 52 ns after the
exact date cache (was 83 ns each), product lookup 34 ns, price ~2 ns, record push ~0.

CPU per stage at 20M (cumulative user+sys CPU-s, logged by `ss`): events parse 5.6, dedup +2.9, clicks +1.2, universe +1.6,
components +0.2, bucket by person +2.3, per-bucket work +2.4, write +0.9, total about 17. The run is CPU-bound (about 6.7 busy
threads); this laptop has 4 cores / 8 threads so each thread runs at roughly half speed. Category lookup now only for
purchases (parse CPU 6.4 -> 5.6 s, wall 2.68 -> 2.55 s A/B). Estimated remaining headroom without a redesign: ~0.3 s
(SWAR timestamps, merged user hash, front cache for products). Sub-second at 20M needs ~2.5x less total work.

## Rejected ideas / why
- More/smaller buckets so each bucket's tables fit this CPU's 256 KB L2 (dedup 256->1024, universe shards 256->1024,
  person buckets 1024->4096): dedup 0.44 -> 0.36 s but parse, bucketing and the per-bucket stage got slower; net 2.65 -> 2.75 s
  (dedup alone) or 3.3 s (all three). Reverted.
- Rewriting quote validation as a line-level count while still using one memchr per comma: no gain (the per-call overhead
  was the cost, not the quote scan). The SSE2 64-byte block mask scanner is what helped (parse 1.15 -> 0.85 s).
- mimalloc env knobs (MIMALLOC_LARGE_OS_PAGES, PURGE_DELAY): within noise (<2%).
- perf is installed but kernel.perf_event_paranoid=4 blocks sampling for non-root users, so profiling is manual.
- Sorting IDs by comparing mmapped bytes: 0.66 s for 1.5M IDs (two cache misses per compare). 8-byte big-endian prefix + full compare on ties: 0.04 s.

- Date-prefix lookup table for timestamps: in Rust exact days_from_civil + calendar checks cost about the same as a table
  lookup and cannot be wrong about leap years, so the table adds risk for no gain.
- DuckDB regexp_extract with 14 capture groups for fields: 11 s vs 5.3 s for fullmatch+split at 20M.
- scipy connected_components: replaced by union-find in the native engine (no Python at all on the fast path).
- fat LTO + 1 codegen unit: Docker build 5m14s vs 56s with thin LTO and 16 units, identical run time.
