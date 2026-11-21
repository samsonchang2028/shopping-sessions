#!/usr/bin/env python3
"""Unit tests for the fast path: timestamps, byte-wise order, ASOF direction, credit/assist boundaries.

Run: python -m unittest discover -s tests   (or `make unit`)
"""
import calendar
import datetime as dt
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import pipeline  # noqa: E402


def utc_ms(y, mo, d, h=0, mi=0, s=0):
    return calendar.timegm(dt.datetime(y, mo, d, h, mi, s).timetuple()) * 1000


TS_CASES = [
    # ---- F1
    ("2026-10-03T23:58:10Z", utc_ms(2026, 10, 3, 23, 58, 10)),
    ("2026-10-01T00:00:00Z", utc_ms(2026, 10, 1)),
    ("1970-01-01T00:00:00Z", 0),
    ("2028-02-29T12:00:00Z", utc_ms(2028, 2, 29, 12)),
    ("2024-02-29T00:00:00Z", utc_ms(2024, 2, 29)),
    ("2000-02-29T00:00:00Z", utc_ms(2000, 2, 29)),
    ("2027-12-31T23:59:59Z", utc_ms(2027, 12, 31, 23, 59, 59)),
    ("2025-04-30T00:00:00Z", utc_ms(2025, 4, 30)),
    ("2099-12-31T23:59:59Z", utc_ms(2099, 12, 31, 23, 59, 59)),
    ("2026-01-31T00:00:00Z", utc_ms(2026, 1, 31)),
    # ---- F2
    ("2026-10-03T19:58:10-04:00", utc_ms(2026, 10, 3, 23, 58, 10)),
    ("2026-10-04T01:30:00+02:00", utc_ms(2026, 10, 3, 23, 30)),
    ("2026-10-03T10:00:00-00:00", utc_ms(2026, 10, 3, 10)),
    ("2026-10-03T10:00:00+00:00", utc_ms(2026, 10, 3, 10)),
    ("2026-10-03T10:00:00+23:59", utc_ms(2026, 10, 3, 10) - (23 * 3600 + 59 * 60) * 1000),
    ("2026-10-03T10:00:00-23:59", utc_ms(2026, 10, 3, 10) + (23 * 3600 + 59 * 60) * 1000),
    ("2026-09-30T20:00:00-04:00", utc_ms(2026, 10, 1, 0)),          # crosses midnight onto window start
    ("2026-10-15T01:00:00+01:00", utc_ms(2026, 10, 15, 0)),         # lands exactly on window end
    ("2026-10-14T23:59:59-10:00", utc_ms(2026, 10, 15, 9, 59, 59)),
    ("2028-03-01T05:00:00+12:00", utc_ms(2028, 2, 29, 17)),         # back across leap day
    ("2027-01-01T00:30:00+05:30", utc_ms(2026, 12, 31, 19)),        # back across new year
    ("2026-12-31T20:00:00-05:00", utc_ms(2027, 1, 1, 1)),           # forward across new year
    # ---- F3
    ("1791071890000", 1791071890000),
    ("0001791071890000", 1791071890000),
    ("86400000", 86400000),
    ("0", 0),
    ("000", 0),
    ("123456789012345", 123456789012345),
    ("1234567890123456", None),     # > 15 significant digits: out of any window, NULL is fine
    ("000000000000000000001791071890000", 1791071890000),
    ("123456789012345678901", None),
    # ---- bad
    ("", None),
    ("2026-10-03T23:58:10z", None),
    ("2026-10-03t23:58:10Z", None),
    ("2026-10-03 23:58:10Z", None),
    ("2026-10-03T23:58:10.000Z", None),
    ("2026-10-03T23:58:10", None),
    ("2026-10-03T23:58:10+0200", None),
    ("2026-10-03T23:58:10+24:00", None),
    ("2026-10-03T23:58:10+05:60", None),
    ("2026-10-03T23:58:10+5:00", None),
    ("2026-10-03T23:58:10 +05:00", None),
    ("1791071890000.0", None),
    ("1.79e12", None),
    ("-5", None),
    ("+5", None),
    (" 1791071890000", None),
    ("1791071890000 ", None),
    ("2026-13-01T00:00:00Z", None),
    ("2026-00-01T00:00:00Z", None),
    ("2026-02-30T00:00:00Z", None),
    ("2027-02-29T00:00:00Z", None),
    ("2100-02-29T00:00:00Z", None),
    ("2026-04-31T00:00:00Z", None),
    ("2026-10-00T00:00:00Z", None),
    ("2026-10-03T24:00:00Z", None),
    ("2026-10-03T23:60:00Z", None),
    ("2026-10-03T23:59:60Z", None),
    ("2026-10-03T23:58:10Z\r", None),
    ("1791071890000\r", None),
    ("٢٠٢٦-10-03T23:58:10Z", None),
    ("２026-10-03T23:58:10Z", None),
    ("１７９１０７１８９００００", None),
    ("١٧٩١٠٧١٨٩٠٠٠٠", None),
    ("2026-10-3T23:58:10Z", None),
    ("26-10-03T23:58:10Z", None),
    ("2026/10/03T23:58:10Z", None),
    ("2026-10-03T23:58Z", None),
    ("2026-10-03", None),
    ("2026-10-03T23:58:10ZZ", None),
    ("2026-10-03T23:58:10Z+00:00", None),
    ("2026-10-03T23:58:10-00:00:00", None),
]


class Units(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = pipeline.connect(2, 2, tempfile.gettempdir())

    def test_timestamp_macro(self):
        self.assertGreaterEqual(len(TS_CASES), 60)
        con = self.con
        con.execute("CREATE OR REPLACE TEMP TABLE tcases (i INTEGER, s VARCHAR)")
        con.executemany("INSERT INTO tcases VALUES (?, ?)", [(i, s) for i, (s, _) in enumerate(TS_CASES)])
        got = dict(con.execute("SELECT i, ts_ms(s) FROM tcases").fetchall())   # vectorized path
        for i, (s, want) in enumerate(TS_CASES):
            with self.subTest(ts=s):
                self.assertEqual(got[i], want)
                self.assertEqual(con.execute("SELECT ts_ms(?)", [s]).fetchone()[0], want)  # constant path

    def test_price(self):
        cases = {"1": 1, "0250": 250, "10000000": 10000000, "010000000": 10000000, "10000001": None, "0": None,
                 "000": None, "19.99": None, "-5": None, "+5": None, "": None, "２５０": None, "٢٥٠": None,
                 "1000\r": None, " 100": None, "99999999999999999999999": None}
        for s, want in cases.items():
            with self.subTest(price=s):
                self.assertEqual(self.con.execute("SELECT price_val(?)", [s]).fetchone()[0], want)

    def test_bytewise_order(self):
        ids = ["u1", "U1", " u1", "u1 ", "\tx", "é", "z", "ü", "Z", "a", "u10", "u2", "éa", "日本", "~", ""]
        got = [r[0] for r in self.con.execute(
            "SELECT id FROM (SELECT unnest(?::VARCHAR[]) AS id) ORDER BY id", [ids]).fetchall()]
        self.assertEqual(got, sorted(ids, key=lambda s: s.encode("utf-8")))
        self.assertEqual(self.con.execute("SELECT min(id) FROM (SELECT unnest(?::VARCHAR[]) AS id)", [ids]).fetchone()[0],
                         min(ids, key=lambda s: s.encode("utf-8")))
        self.assertEqual(self.con.execute("SELECT current_setting('default_collation')").fetchone()[0], "")

    def test_asof_direction(self):
        con = self.con
        con.execute("CREATE OR REPLACE TEMP TABLE s AS SELECT * FROM (VALUES (1, 0, 100), (1, 1, 200), (1, 2, 300)) t(pk, sess_no, start)")
        con.execute("CREATE OR REPLACE TEMP TABLE i AS SELECT * FROM (VALUES (1, 150), (1, 200), (1, 50), (1, 301)) t(pk, x)")
        # i.x <= s.start: the SMALLEST start >= x
        ge = dict(con.execute("SELECT i.x, s.sess_no FROM i ASOF LEFT JOIN s ON i.pk = s.pk AND i.x <= s.start").fetchall())
        self.assertEqual(ge, {150: 1, 200: 1, 50: 0, 301: None})
        # i.x >= s.start: the LARGEST start <= x
        le = dict(con.execute("SELECT i.x, s.sess_no FROM i ASOF LEFT JOIN s ON i.pk = s.pk AND i.x >= s.start").fetchall())
        self.assertEqual(le, {150: 0, 200: 1, 50: None, 301: 2})

    def test_credit_and_assist_boundaries(self):
        S = utc_ms(2026, 10, 3, 10)
        W = 604_800_000
        # one user per case, each with one session at S
        clicks = {
            "u_week": [("k1", "c_1", S - W)],             # exactly 7 d before: credits
            "u_week1": [("k1b", "c_1", S - W - 1)],       # 7 d + 1 ms before: organic
            "u_at": [("k2", "c_1", S)],                   # at start: credits
            "u_after": [("k3", "c_1", S + 1)],            # 1 ms after: organic
            "u_tie": [("kb", "c_2", S - 5), ("ka", "c_1", S - 5)],   # same ms: smallest click_id (ka -> c_1)
            "u_latest": [("kz", "c_1", S - 100), ("ky", "c_2", S - 50)],  # latest wins (c_2), both assist
        }
        want_credit = {"u_week": "c_1", "u_week1": "organic", "u_at": "c_1", "u_after": "organic",
                       "u_tie": "c_1", "u_latest": "c_2"}
        want_assists = {"u_week": {"c_1"}, "u_week1": set(), "u_at": {"c_1"}, "u_after": set(),
                        "u_tie": {"c_1", "c_2"}, "u_latest": {"c_1", "c_2"}}
        with tempfile.TemporaryDirectory() as d:
            data = os.path.join(d, "data")
            for fam in ("events", "clicks", "links"):
                os.makedirs(os.path.join(data, fam))
            with open(os.path.join(data, "manifest.json"), "w") as f:
                f.write('{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}')
            with open(os.path.join(data, "products.csv"), "w") as f:
                f.write("product_id,category\np1,shoes\n")
            with open(os.path.join(data, "campaigns.csv"), "w") as f:
                f.write("campaign_id,channel\nc_1,search\nc_2,social\n")
            with open(os.path.join(data, "links", "p.csv"), "w") as f:
                f.write("user_id_a,user_id_b\n")
            with open(os.path.join(data, "events", "p.csv"), "w") as f:
                f.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
                for n, u in enumerate(clicks):
                    f.write(f"e{n},{u},{S},2026-10-03T11:00:00Z,view,p1,\n")
            with open(os.path.join(data, "clicks", "p.csv"), "w") as f:
                f.write("click_id,user_id,campaign_id,ts\n")
                for u, cl in clicks.items():
                    for cid, camp, ts in cl:
                        f.write(f"{cid},{u},{camp},{ts}\n")
            con = pipeline.run(data, os.path.join(d, "out"), d, 2, 2, check=True)
            credit = dict(con.execute("SELECT u.id, c.campaign_id FROM cred c JOIN uni u ON u.idx = c.pk").fetchall())
            self.assertEqual(credit, want_credit)
            # assists per user via islands: recompute (pk, campaign) pairs covered
            pairs = con.execute("""
                SELECT u.id, i.campaign_id FROM isl i JOIN sess s ON s.pk = i.pk AND s.start BETWEEN i.f AND i.l
                JOIN uni u ON u.idx = i.pk""").fetchall()
            got = {u: set() for u in clicks}
            for u, c in pairs:
                got[u].add(c)
            self.assertEqual(got, want_assists)
            total = con.execute("SELECT sum(assisted) FROM assist").fetchone()[0]
            self.assertEqual(total, sum(len(v) for v in want_assists.values()))


if __name__ == "__main__":
    unittest.main()
