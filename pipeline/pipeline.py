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
def log(msg):
    print(f"[{time.perf_counter() - T0:7.3f}s] {msg}", file=sys.stderr, flush=True)


MACROS = r"""
-- ts string -> epoch ms (BIGINT) or NULL. Forms F1, F2, F3 of rules.md, nothing else.
-- The regex fixes the exact shape (ASCII digits, separators, zone); try_strptime then rejects impossible
-- calendar values (month 13, Feb 30, Feb 29 off leap years, hour 24, minute/second 60): see tests/test_units.py.
CREATE OR REPLACE TEMP MACRO ts_ms(s) AS
  CASE
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z') THEN
      epoch_ms(try_strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    WHEN regexp_full_match(s, '[0-9]+') THEN
      CASE WHEN length(ltrim(s, '0')) > 15 THEN NULL
           WHEN ltrim(s, '0') = '' THEN 0::BIGINT
           ELSE CAST(ltrim(s, '0') AS BIGINT) END
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-](?:[01][0-9]|2[0-3]):[0-5][0-9]') THEN
      epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
      - (CASE WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END)
        * (CAST(substr(s, 21, 2) AS BIGINT) * 3600000 + CAST(substr(s, 24, 2) AS BIGINT) * 60000)
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


def fok(f):
    """SQL: raw field f is well formed (not missing; no quote, or one quote pair around the whole field)."""
    return (f"({f} IS NOT NULL AND (NOT contains({f}, '\"') OR (length({f}) >= 2 AND starts_with({f}, '\"') "
            f"AND ends_with({f}, '\"') AND NOT contains(substr({f}, 2, length({f}) - 2), '\"'))))")


def funq(f):
    """SQL: unquoted value of a well-formed raw field, NULL if empty."""
    return f"NULLIF(CASE WHEN starts_with({f}, '\"') THEN substr({f}, 2, length({f}) - 2) ELSE {f} END, '')"


def family_sql(folder, names, mode):
    """SQL relation with columns `names` (unquoted, NULL for empty) for every well-formed data line.

    fast:  DuckDB's CSV reader, no quoting, one extra sentinel column `x`, nullstr = a newline (which no
           field can hold), null padding, strict mode, ignore_errors. A line of exactly len(names) fields is
           the only kind with every column non-NULL and x NULL: shorter lines pad with NULL, longer ones fill
           x (or are rejected), empty lines are skipped. Strict mode raises on any \\r (which DuckDB would
           otherwise treat as a line break); the caller then reruns with `exact`.
    exact: read_text + string_split on \\n, so \\r stays data.
    """
    glob = os.path.join(folder, "*.csv").replace("'", "''")
    k = len(names)
    if not os.path.isdir(folder) or not any(n.endswith(".csv") for n in os.listdir(folder)):
        return "SELECT " + ", ".join(f"NULL::VARCHAR AS {n}" for n in names) + " WHERE false"
    if mode == "fast":
        cols = ", ".join(f"'c{i}': 'VARCHAR'" for i in range(k)) + ", 'x': 'VARCHAR'"
        src = (f"read_csv('{glob}', columns={{{cols}}}, header=true, auto_detect=false, delim=',', quote='', "
               f"escape='', new_line='\\n', nullstr=chr(10), null_padding=true, ignore_errors=true, "
               f"strict_mode=true, comment='', skip=0)")
        fields = [f"c{i}" for i in range(k)]
        where = " AND ".join(["x IS NULL"] + [fok(f) for f in fields])
    else:
        src = (f"(SELECT string_split(l, ',') AS f FROM (SELECT unnest((string_split(content, chr(10)))[2:]) AS l "
               f"FROM read_text('{glob}')))")
        fields = [f"f[{i + 1}]" for i in range(k)]
        where = " AND ".join([f"len(f) = {k}"] + [fok(f) for f in fields])
    sel = ", ".join(f"{funq(f)} AS {n}" for f, n in zip(fields, names))
    return f"SELECT {sel} FROM {src} WHERE {where}"


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

    modes = {}

    def fam_stage(name, fam, names, build):
        """Run a stage whose input is CSV family `fam`; fast reader first, exact reader if it refuses."""
        folder = os.path.join(data, fam)
        try:
            stage(name, build(family_sql(folder, names, "fast")))
            modes[fam] = "fast"
        except duckdb.Error as error:
            log(f"{fam}: fast reader refused ({str(error).splitlines()[0][:80]}); exact byte split")
            con.execute(f"DROP TABLE IF EXISTS {name}")
            stage(name, build(family_sql(folder, names, "exact")))
            modes[fam] = "exact"

    # ---- Stages A+C: events. Parse -> dedup on event_id (earliest ingest_ts) -> validate -> late -> category
    ev_names = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
    fam_stage("ev", "events", ev_names, lambda src: f"""
        CREATE TEMP TABLE ev AS
        WITH kept AS MATERIALIZED (
            SELECT k.u AS user_id, ts_ms(k.ts) AS ts, epoch_ms(try_strptime(ing, '%Y-%m-%dT%H:%M:%SZ')) AS its,
                   k.et AS et, k.p AS product_id, k.pr AS pr
            FROM (
                SELECT min(ingest_ts) AS ing,
                       arg_min({{'u': user_id, 'ts': ts, 'et': event_type, 'p': product_id, 'pr': price_cents}}, ingest_ts) AS k
                FROM ({src}) WHERE event_id IS NOT NULL GROUP BY event_id
            )
            WHERE k.u IS NOT NULL AND k.p IS NOT NULL AND k.et IN ('view', 'cart', 'purchase')
        ), priced AS MATERIALIZED (
            SELECT user_id, ts, et = 'purchase' AS is_p, CASE WHEN et = 'purchase' THEN price_val(pr) END AS price, product_id
            FROM kept WHERE ts BETWEEN {ws} AND {we - 1} AND its - ts <= {DAY_MS}
        )
        SELECT user_id, ts, is_p, price, COALESCE(pr.category, 'unknown') AS category
        FROM priced LEFT JOIN products pr USING (product_id)
        WHERE NOT is_p OR price IS NOT NULL
    """)

    # ---- Stages A+F (part 1): clicks. Parse -> dedup -> validate
    fam_stage("clk", "clicks", ["click_id", "user_id", "campaign_id", "ts"], lambda src: f"""
        CREATE TEMP TABLE clk AS
        SELECT click_id, user_id, campaign_id, ts FROM (
            SELECT click_id, first(user_id) AS user_id, first(campaign_id) AS campaign_id, ts_ms(first(ts)) AS ts
            FROM ({src}) WHERE click_id IS NOT NULL GROUP BY click_id
        ) c SEMI JOIN campaigns USING (campaign_id)
        WHERE user_id IS NOT NULL AND ts BETWEEN {cws} AND {we - 1}
    """)

    # ---- Stages A+D: links, identity resolution
    fam_stage("lnk", "links", ["a", "b"], lambda src: f"""
        CREATE TEMP TABLE lnk AS SELECT a, b FROM ({src}) WHERE a IS NOT NULL AND b IS NOT NULL
    """)
    log(f"readers: {modes}")
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
        SELECT s.pk, s.sess_no, s.start, s.dayn, s.has_p, s.rev,
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
    return con


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
