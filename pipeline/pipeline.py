#!/usr/bin/env python3
"""
Shopping Sessions pipeline (DuckDB + numpy/scipy). Spec: rules.md.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

Every stage is a CREATE TEMP TABLE so it can be timed and inspected alone.
Stage timings go to stderr. CHECK=1 runs the output invariants.
"""
import json
import os
import sys
import time

T0 = time.perf_counter()

import duckdb  # noqa: E402
import numpy as np  # noqa: E402

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 604_800_000

# One CSV field under rules.md: either fully quoted with no quote inside, or no quote at all.
# No value ever holds a comma, so every comma separates fields.
FIELD = '(?:"[^",]*"|[^",]*)'


def line_re(arity):
    return FIELD + (',' + FIELD) * (arity - 1)


def log(msg):
    print(f"[{time.perf_counter() - T0:7.3f}s] {msg}", file=sys.stderr, flush=True)


MACROS = r"""
-- Calendar date/time -> epoch ms, NULL if any component is out of range.
-- Inputs are already known to be ASCII-digit substrings, so the casts cannot fail.
-- Hinnant days_from_civil; y >= 1970 so every operand is non-negative and // is floor division.
CREATE OR REPLACE TEMP MACRO civil_days(yp, mo, d) AS
  (yp // 400) * 146097
  + (yp % 400) * 365 + (yp % 400) // 4 - (yp % 400) // 100
  + (153 * (mo + CASE WHEN mo > 2 THEN -3 ELSE 9 END) + 2) // 5 + d - 1
  - 719468;
CREATE OR REPLACE TEMP MACRO civil_ms(y, mo, d, h, mi, se) AS
  CASE WHEN y BETWEEN 1970 AND 2100 AND mo BETWEEN 1 AND 12 AND h <= 23 AND mi <= 59 AND se <= 59
        AND d >= 1
        AND d <= CASE WHEN mo = 2 THEN (CASE WHEN (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0 THEN 29 ELSE 28 END)
                      WHEN mo IN (4, 6, 9, 11) THEN 30 ELSE 31 END
  THEN ((civil_days(CAST(y AS BIGINT) - CASE WHEN mo <= 2 THEN 1 ELSE 0 END, CAST(mo AS BIGINT), CAST(d AS BIGINT)) * 24
         + h) * 60 + mi) * 60000 + CAST(se AS BIGINT) * 1000
  END;

-- ts string -> epoch ms (BIGINT) or NULL. Forms F1, F2, F3 of rules.md, nothing else.
CREATE OR REPLACE TEMP MACRO ts_ms(s) AS
  CASE
    WHEN regexp_full_match(s, '[0-9]+') THEN
      CASE WHEN length(ltrim(s, '0')) > 15 THEN NULL
           WHEN ltrim(s, '0') = '' THEN 0::BIGINT
           ELSE CAST(ltrim(s, '0') AS BIGINT) END
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})') THEN
      CASE WHEN length(s) = 20 THEN
             civil_ms(CAST(substr(s, 1, 4) AS INTEGER), CAST(substr(s, 6, 2) AS INTEGER), CAST(substr(s, 9, 2) AS INTEGER),
                      CAST(substr(s, 12, 2) AS INTEGER), CAST(substr(s, 15, 2) AS INTEGER), CAST(substr(s, 18, 2) AS INTEGER))
           WHEN CAST(substr(s, 21, 2) AS INTEGER) <= 23 AND CAST(substr(s, 24, 2) AS INTEGER) <= 59 THEN
             civil_ms(CAST(substr(s, 1, 4) AS INTEGER), CAST(substr(s, 6, 2) AS INTEGER), CAST(substr(s, 9, 2) AS INTEGER),
                      CAST(substr(s, 12, 2) AS INTEGER), CAST(substr(s, 15, 2) AS INTEGER), CAST(substr(s, 18, 2) AS INTEGER))
             - (CASE WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END)
               * (CAST(substr(s, 21, 2) AS BIGINT) * 3600000 + CAST(substr(s, 24, 2) AS BIGINT) * 60000)
           ELSE NULL END
    ELSE NULL
  END;

-- price_cents for a purchase: ASCII digits only, value 1..10,000,000, else NULL.
CREATE OR REPLACE TEMP MACRO price_val(p) AS
  CASE WHEN regexp_full_match(p, '[0-9]+') AND length(ltrim(p, '0')) BETWEEN 1 AND 8
            AND CAST(ltrim(p, '0') AS BIGINT) <= 10000000
       THEN CAST(ltrim(p, '0') AS BIGINT) END;
"""


def connect(ncpu, mem_gb, scratch):
    con = duckdb.connect(config={"threads": ncpu})
    con.execute(f"SET memory_limit = '{max(1, int(mem_gb * 0.75))}GB'")
    con.execute(f"SET temp_directory = '{os.path.join(scratch, 'duckdb_tmp')}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET default_collation = ''")
    con.execute(MACROS)
    return con


def lines_sql(con, folder, alias):
    """Register a view `alias(l VARCHAR)` with every data line (header excluded) of every *.csv in folder.

    Fast path: DuckDB's CSV reader with no delimiter/quote, strict mode. It treats a bare \\r as a line
    break, which rules.md forbids, but in strict mode it raises on any \\r instead of splitting. On any
    error we fall back to an exact byte split (read_text + string_split on \\n), which keeps \\r.
    """
    glob = os.path.join(folder, "*.csv").replace("'", "''")
    if not os.path.isdir(folder) or not any(n.endswith(".csv") for n in os.listdir(folder)):
        con.execute(f"CREATE OR REPLACE TEMP VIEW {alias} AS SELECT NULL::VARCHAR AS l WHERE false")
        return "empty"
    fast = (f"SELECT l FROM read_csv('{glob}', columns={{'l': 'VARCHAR'}}, header=true, auto_detect=false, "
            f"delim='\\x1f', quote='', escape='', new_line='\\n', comment='', skip=0, strict_mode=true, "
            f"null_padding=false, ignore_errors=false)")
    try:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {alias} AS {fast}")
        return "fast"
    except duckdb.Error as error:
        log(f"{alias}: fast line reader refused ({str(error).splitlines()[0][:90]}); exact split")
    exact = (f"SELECT unnest((string_split(content, chr(10)))[2:]) AS l "
             f"FROM read_text('{glob}')")
    con.execute(f"CREATE OR REPLACE TEMP TABLE {alias} AS {exact}")
    return "exact"


def fields_sql(src, arity, names):
    """SELECT list of NULLIF'd unquoted fields for valid lines of `src`."""
    # On a valid line quotes appear only around whole fields, so deleting every quote unquotes all fields.
    cols = ", ".join(f"NULLIF(f[{i + 1}], '') AS {n}" for i, n in enumerate(names))
    return (f"SELECT {cols} FROM (SELECT string_split(replace(l, '\"', ''), ',') AS f FROM {src} "
            f"WHERE l IS NOT NULL AND regexp_full_match(l, '{line_re(arity)}'))")


def window_ms(con, s):
    v = con.execute("SELECT ts_ms(?)", [s]).fetchone()[0]
    if v is None:
        raise ValueError(f"bad manifest timestamp {s!r}")
    return int(v)


def connected_min_labels(n, a, b):
    """For n nodes and edges (a, b), return for each node the smallest node index in its component."""
    if n == 0:
        return np.zeros(0, dtype=np.int64)
    if len(a) == 0:
        return np.arange(n, dtype=np.int64)
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    g = coo_matrix((np.ones(len(a), dtype=np.int8), (a, b)), shape=(n, n))
    ncomp, labels = connected_components(g, directed=False)
    rep = np.full(ncomp, n, dtype=np.int64)
    np.minimum.at(rep, labels, np.arange(n, dtype=np.int64))
    return rep[labels]


def run(data, out, scratch, ncpu, mem_gb, check=False):
    con = connect(ncpu, mem_gb, scratch)
    t = [time.perf_counter()]

    def stage(name, sql):
        con.execute(sql)
        now = time.perf_counter()
        log(f"stage {name:<14} {now - t[0]:7.3f}s")
        t[0] = now

    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    ws = window_ms(con, manifest["window_start"])
    we = window_ms(con, manifest["window_end"])
    cws = ws - WEEK_MS

    def q(p):
        return p.replace("'", "''")

    # ---- small clean tables
    stage("dims", f"""
        CREATE TEMP TABLE products AS SELECT product_id, category FROM read_csv('{q(os.path.join(data, 'products.csv'))}',
            columns={{'product_id': 'VARCHAR', 'category': 'VARCHAR'}}, header=true, auto_detect=false, delim=',', quote='"');
        CREATE TEMP TABLE campaigns AS SELECT campaign_id, channel FROM read_csv('{q(os.path.join(data, 'campaigns.csv'))}',
            columns={{'campaign_id': 'VARCHAR', 'channel': 'VARCHAR'}}, header=true, auto_detect=false, delim=',', quote='"');
    """)

    # ---- Stage A: raw lines
    modes = [lines_sql(con, os.path.join(data, fam), f"raw_{fam}") for fam in ("events", "clicks", "links")]
    stage("read_lines", "SELECT 1")
    log(f"line readers: {modes}")

    # ---- Stage C: events. Parse -> dedup on event_id (earliest ingest_ts) -> validate -> late -> category
    stage("ev_dedup", f"""
        CREATE TEMP TABLE ev_kept AS
        SELECT event_id, min(ingest_ts) AS ingest_ts,
               arg_min({{'u': user_id, 'ts': ts, 'et': event_type, 'p': product_id, 'pr': price_cents}}, ingest_ts) AS k
        FROM ({fields_sql('raw_events', 7, ['event_id', 'user_id', 'ts', 'ingest_ts', 'event_type', 'product_id', 'price_cents'])})
        WHERE event_id IS NOT NULL
        GROUP BY event_id
    """)
    stage("ev_valid", f"""
        CREATE TEMP TABLE ev AS
        SELECT user_id, ts, is_p, price, COALESCE(pr.category, 'unknown') AS category
        FROM (
            SELECT k.u AS user_id, ts_ms(k.ts) AS ts, ts_ms(ingest_ts) AS its, k.et = 'purchase' AS is_p,
                   CASE WHEN k.et = 'purchase' THEN price_val(k.pr) END AS price, k.p AS product_id, k.et AS et
            FROM ev_kept
        ) e LEFT JOIN products pr USING (product_id)
        WHERE user_id IS NOT NULL AND ts IS NOT NULL AND ts >= {ws} AND ts < {we}
          AND et IN ('view', 'cart', 'purchase') AND product_id IS NOT NULL
          AND (NOT is_p OR price IS NOT NULL)
          AND its - ts <= {DAY_MS}
    """)

    # ---- Stage F (part 1): clicks. Parse -> dedup -> validate
    stage("clicks", f"""
        CREATE TEMP TABLE clk AS
        SELECT click_id, user_id, campaign_id, ts FROM (
            SELECT click_id, first(user_id) AS user_id, first(campaign_id) AS campaign_id, ts_ms(first(ts)) AS ts
            FROM ({fields_sql('raw_clicks', 4, ['click_id', 'user_id', 'campaign_id', 'ts'])})
            WHERE click_id IS NOT NULL GROUP BY click_id
        ) c SEMI JOIN campaigns USING (campaign_id)
        WHERE user_id IS NOT NULL AND ts IS NOT NULL AND ts >= {cws} AND ts < {we}
    """)

    # ---- Stage D: identity resolution
    stage("links", f"""
        CREATE TEMP TABLE lnk AS SELECT a, b FROM ({fields_sql('raw_links', 2, ['a', 'b'])}) WHERE a IS NOT NULL AND b IS NOT NULL
    """)
    stage("universe", """
        CREATE TEMP TABLE uni AS
        SELECT id, (row_number() OVER (ORDER BY id) - 1)::INTEGER AS idx FROM (
            SELECT user_id AS id FROM ev UNION SELECT user_id FROM clk UNION SELECT a FROM lnk UNION SELECT b FROM lnk
        )
    """)
    n = con.execute("SELECT count(*) FROM uni").fetchone()[0]
    edges = con.execute("""
        SELECT ua.idx AS a, ub.idx AS b FROM lnk JOIN uni ua ON lnk.a = ua.id JOIN uni ub ON lnk.b = ub.id
    """).fetchnumpy()
    pk = connected_min_labels(n, edges["a"], edges["b"]).astype(np.int32)
    idmap = {"idx": np.arange(n, dtype=np.int32), "pk": pk}  # noqa: F841 (scanned by DuckDB)
    con.register("idmap_np", idmap)
    stage("components", """
        CREATE TEMP TABLE idmap AS SELECT u.id, m.pk FROM uni u JOIN idmap_np m ON u.idx = m.idx
    """)
    con.unregister("idmap_np")

    # ---- Stage E: sessionization, by person and distinct ts
    stage("ev_groups", """
        CREATE TEMP TABLE evg AS
        SELECT m.pk, e.ts, count(*) AS n, COALESCE(sum(e.price), 0) AS rev, bool_or(e.is_p) AS has_p
        FROM ev e JOIN idmap m ON e.user_id = m.id GROUP BY m.pk, e.ts
    """)
    stage("sessionize", f"""
        CREATE TEMP TABLE evs AS
        SELECT pk, ts, n, rev, has_p, (sum(nw) OVER (PARTITION BY pk ORDER BY ts ROWS UNBOUNDED PRECEDING) - 1)::INTEGER AS sess_no
        FROM (SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY pk ORDER BY ts) <= {GAP_MS} THEN 0 ELSE 1 END AS nw FROM evg)
    """)
    stage("sessions", f"""
        CREATE TEMP TABLE sess AS
        SELECT pk, sess_no, min(ts) AS start, (min(ts) // {DAY_MS})::INTEGER AS dayn,
               sum(n) AS n, sum(rev) AS rev, bool_or(has_p) AS has_p
        FROM evs GROUP BY pk, sess_no
    """)
    stage("catrev", """
        CREATE TEMP TABLE catrev AS
        SELECT s.pk, s.dayn, c.category, sum(c.rev) AS rev FROM (
            SELECT m.pk, e.ts, e.category, sum(e.price) AS rev FROM ev e JOIN idmap m ON e.user_id = m.id
            WHERE e.is_p GROUP BY ALL
        ) c JOIN evs USING (pk, ts) JOIN sess s USING (pk, sess_no)
        GROUP BY ALL
    """)

    # ---- Stage F (part 2): credit via ASOF on collapsed click points
    stage("click_points", """
        CREATE TEMP TABLE clkp AS SELECT m.pk, c.ts, c.campaign_id, c.click_id FROM clk c JOIN idmap m ON c.user_id = m.id;
        CREATE TEMP TABLE cp AS SELECT pk, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clkp GROUP BY pk, ts;
    """)
    stage("credit", f"""
        CREATE TEMP TABLE cred AS
        SELECT s.dayn, s.has_p, s.rev,
               CASE WHEN cp.ts IS NOT NULL AND s.start - cp.ts <= {WEEK_MS} THEN cp.campaign_id ELSE 'organic' END AS campaign_id
        FROM sess s ASOF LEFT JOIN cp ON s.pk = cp.pk AND s.start >= cp.ts
    """)

    # ---- Stage G: assists via islands of each (person, campaign)'s clicks
    stage("islands", f"""
        CREATE TEMP TABLE isl AS
        SELECT pk, campaign_id, min(ts) AS f, max(ts) + {WEEK_MS} AS l FROM (
            SELECT pk, campaign_id, ts, sum(nw) OVER (PARTITION BY pk, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS island
            FROM (SELECT pk, campaign_id, ts,
                         CASE WHEN ts - lag(ts) OVER (PARTITION BY pk, campaign_id ORDER BY ts) <= {WEEK_MS} THEN 0 ELSE 1 END AS nw
                  FROM clkp)
        ) GROUP BY pk, campaign_id, island
    """)
    stage("assists", """
        CREATE TEMP TABLE assist AS
        WITH lo AS (
            SELECT i.pk, i.campaign_id, i.l, s.sess_no AS lo FROM isl i ASOF JOIN sess s ON i.pk = s.pk AND i.f <= s.start
        ), hi AS (
            SELECT lo.pk, lo.campaign_id, lo.lo, s.sess_no AS hi FROM lo ASOF JOIN sess s ON lo.pk = s.pk AND lo.l >= s.start
        ), ex AS (
            SELECT pk, campaign_id, unnest(range(lo, hi + 1)) AS sess_no FROM hi WHERE lo <= hi
        )
        SELECT ex.campaign_id, s.dayn, count(*) AS assisted FROM ex JOIN sess s USING (pk, sess_no) GROUP BY ALL
    """)

    # ---- Stage H: outputs
    ud_dir = os.path.join(out, "user_daily")
    cd_dir = os.path.join(out, "campaign_daily")
    os.makedirs(ud_dir, exist_ok=True)
    os.makedirs(cd_dir, exist_ok=True)
    stage("user_daily", f"""
        COPY (
            SELECT u.id AS user_id, (DATE '1970-01-01' + d.dayn) AS day, d.sessions, d.events, d.revenue_cents, t.top_category
            FROM (SELECT pk, dayn, count(*)::BIGINT AS sessions, sum(n)::BIGINT AS events, sum(rev)::BIGINT AS revenue_cents
                  FROM sess GROUP BY pk, dayn) d
            JOIN uni u ON u.idx = d.pk
            LEFT JOIN (SELECT pk, dayn, first(category ORDER BY rev DESC, category) AS top_category FROM catrev GROUP BY pk, dayn) t
              USING (pk, dayn)
        ) TO '{q(os.path.join(ud_dir, 'part-0.parquet'))}' (FORMAT parquet, COMPRESSION snappy)
    """)
    stage("campaign_daily", f"""
        COPY (
            SELECT campaign_id,
                   CASE WHEN campaign_id = 'organic' THEN 'organic' ELSE c.channel END AS channel,
                   (DATE '1970-01-01' + dayn) AS day,
                   COALESCE(cr.sessions, 0)::BIGINT AS sessions,
                   COALESCE(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
                   COALESCE(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
                   COALESCE(a.assisted, 0)::BIGINT AS assisted_sessions
            FROM (SELECT campaign_id, dayn, count(*) AS sessions, count_if(has_p) AS purchasing_sessions, sum(rev) AS revenue_cents
                  FROM cred GROUP BY ALL) cr
            FULL OUTER JOIN assist a USING (campaign_id, dayn)
            LEFT JOIN campaigns c USING (campaign_id)
        ) TO '{q(os.path.join(cd_dir, 'part-0.parquet'))}' (FORMAT parquet, COMPRESSION snappy)
    """)

    if check:
        check_invariants(con, ud_dir, cd_dir)
    log("done")


def check_invariants(con, ud_dir, cd_dir):
    u = con.execute(f"SELECT sum(sessions), sum(revenue_cents) FROM read_parquet('{ud_dir}/*.parquet')").fetchone()
    c = con.execute(f"""SELECT sum(sessions), sum(revenue_cents), sum(assisted_sessions),
                               sum(sessions) FILTER (WHERE campaign_id <> 'organic')
                        FROM read_parquet('{cd_dir}/*.parquet')""").fetchone()
    u = tuple(x or 0 for x in u)
    c = tuple(x or 0 for x in c)
    ok = u[0] == c[0] and u[1] == c[1] and c[2] >= c[3]
    log(f"CHECK sessions {u[0]}/{c[0]} revenue {u[1]}/{c[1]} assisted {c[2]} >= credited non-organic {c[3]}: "
        f"{'OK' if ok else 'FAILED'}")
    if not ok:
        raise SystemExit("invariant check failed")


def main():
    data = os.environ.get("DATA_DIR", "/data")
    out = os.environ.get("OUTPUT_DIR", "/output")
    scratch = os.environ.get("SCRATCH_DIR", "/scratch")
    if not os.path.isdir(scratch):
        import tempfile
        scratch = tempfile.gettempdir()
    ncpu = int(os.environ.get("NCPU") or os.cpu_count() or 1)
    mem_gb = int(os.environ.get("MEM_GB") or 8)
    run(data, out, scratch, ncpu, mem_gb, check=os.environ.get("CHECK") == "1")


if __name__ == "__main__":
    main()
