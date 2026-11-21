#!/usr/bin/env python3
"""
Independent reference implementation of rules.md, in plain Python.

Shares no code with pipeline/: its own CSV grammar (bytes, split on b'\\n'), its own
timestamp parser, its own union-find, sessionizer and attribution. Slow; meant for
fixtures and for fuzzing at 50k-200k events.

Usage: reference.py DATA_DIR OUTPUT_DIR   (writes OUTPUT_DIR/{user_daily,campaign_daily}/part-0.parquet)
"""
import bisect
import calendar
import datetime
import glob
import json
import os
import sys

DAY = 86_400_000
GAP = 1_800_000
WEEK = 604_800_000
QUOTE = 0x22


def is_digits(b):
    return len(b) > 0 and all(0x30 <= c <= 0x39 for c in b)


def read_family(folder, arity):
    """Yield the field tuples (bytes or None) of every well-formed data line of every *.csv in folder."""
    for path in sorted(glob.glob(os.path.join(glob.escape(folder), "*.csv"))):
        with open(path, "rb") as fh:
            raw = fh.read()
        lines = raw.split(b"\n")
        if raw.endswith(b"\n"):
            lines.pop()  # text after the final newline is no line at all
        for line in lines[1:]:  # first line of every file is its header
            parts = line.split(b",")
            if len(parts) != arity:
                continue  # wrong field count (an empty line has one field)
            values = []
            ok = True
            for p in parts:
                if QUOTE in p:
                    if len(p) >= 2 and p[0] == QUOTE and p[-1] == QUOTE and QUOTE not in p[1:-1]:
                        p = p[1:-1]
                    else:
                        ok = False
                        break
                values.append(p if p != b"" else None)
            if ok:
                yield tuple(values)


def read_clean(path):
    """products.csv / campaigns.csv: clean two-column files."""
    out = {}
    with open(path, "rb") as fh:
        lines = fh.read().split(b"\n")
    for line in lines[1:]:
        if line == b"":
            continue
        a, b = line.split(b",")
        if a[:1] == b'"' and a[-1:] == b'"':
            a = a[1:-1]
        if b[:1] == b'"' and b[-1:] == b'"':
            b = b[1:-1]
        out[a] = b
    return out


def parse_ts(s):
    """bytes -> epoch ms (int) or None, per the three forms of rules.md."""
    if s is None:
        return None
    if is_digits(s):
        return int(s.decode("ascii"))  # leading zeros fine; arbitrarily large is fine
    n = len(s)
    if n not in (20, 25):
        return None

    def num(lo, hi):
        part = s[lo:hi]
        return int(part.decode("ascii")) if is_digits(part) else None

    y, mo, d, h, mi, se = num(0, 4), num(5, 7), num(8, 10), num(11, 13), num(14, 16), num(17, 19)
    if None in (y, mo, d, h, mi, se):
        return None
    if s[4:5] != b"-" or s[7:8] != b"-" or s[10:11] != b"T" or s[13:14] != b":" or s[16:17] != b":":
        return None
    if n == 20:
        if s[19:20] != b"Z":
            return None
        offset_ms = 0
    else:
        sign = s[19:20]
        oh, om = num(20, 22), num(23, 25)
        if sign not in (b"+", b"-") or s[22:23] != b":" or oh is None or om is None:
            return None
        if oh > 23 or om > 59:
            return None
        offset_ms = (oh * 3600 + om * 60) * 1000 * (1 if sign == b"+" else -1)
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59 or y < 1:
        return None
    if not (1 <= d <= calendar.monthrange(y, mo)[1]):
        return None
    dt = datetime.datetime(y, mo, d, h, mi, se, tzinfo=datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    delta = dt - epoch
    local_ms = (delta.days * 86400 + delta.seconds) * 1000
    return local_ms - offset_ms


class DSU:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        p = self.parent
        root = x
        while p.get(root, root) != root:
            root = p[root]
        while p.get(x, x) != root:
            nxt = p[x]
            p[x] = root
            x = nxt
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # keep the byte-wise smaller root, so a root is its component's name
        if rb < ra:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.parent.setdefault(ra, ra)


def main(data, out):
    with open(os.path.join(data, "manifest.json"), "rb") as fh:
        manifest = json.load(fh)
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())

    products = read_clean(os.path.join(data, "products.csv"))
    campaigns = read_clean(os.path.join(data, "campaigns.csv"))

    # ---- links
    dsu = DSU()
    for a, b in read_family(os.path.join(data, "links"), 2):
        if a is None or b is None:
            continue
        dsu.union(a, b)

    def person(uid):
        return dsu.find(uid)  # byte-wise smallest ID of the component (or uid itself)

    # ---- events: parse, dedup on earliest ingest_ts
    kept = {}
    for row in read_family(os.path.join(data, "events"), 7):
        event_id, user_id, ts, ingest_ts, event_type, product_id, price = row
        if event_id is None:
            continue
        ing = parse_ts(ingest_ts)
        prev = kept.get(event_id)
        if prev is None or ing < prev[0]:
            kept[event_id] = (ing, row)

    events_by_person = {}
    for ing, row in kept.values():
        event_id, user_id, ts, ingest_ts, event_type, product_id, price = row
        if user_id is None or product_id is None:
            continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we):
            continue
        if event_type not in (b"view", b"cart", b"purchase"):
            continue
        cents = 0
        if event_type == b"purchase":
            if price is None or not is_digits(price):
                continue
            cents = int(price.decode("ascii"))
            if not (1 <= cents <= 10_000_000):
                continue
        if ing - t > DAY:
            continue
        category = products.get(product_id, b"unknown")
        events_by_person.setdefault(person(user_id), []).append((t, event_type == b"purchase", cents, category))

    # ---- clicks
    clicks_seen = {}
    for row in read_family(os.path.join(data, "clicks"), 4):
        click_id, user_id, campaign_id, ts = row
        if click_id is None:
            continue
        clicks_seen.setdefault(click_id, row)
    clicks_by_person = {}
    for click_id, user_id, campaign_id, ts in clicks_seen.values():
        if user_id is None or campaign_id not in campaigns:
            continue
        t = parse_ts(ts)
        if t is None or not (ws - WEEK <= t < we):
            continue
        clicks_by_person.setdefault(person(user_id), []).append((t, click_id, campaign_id))
    for lst in clicks_by_person.values():
        lst.sort()
    click_ts = {p: [c[0] for c in lst] for p, lst in clicks_by_person.items()}

    # ---- sessions
    user_daily = {}   # (person, day) -> [sessions, events, revenue, {category: rev}]
    camp_daily = {}   # (campaign, day) -> [sessions, purchasing, revenue, assisted]
    epoch = datetime.date(1970, 1, 1)
    for p, evs in events_by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        last = None
        for e in evs:
            if last is None or e[0] - last > GAP:
                sessions.append([e[0], []])
            sessions[-1][1].append(e)
            last = e[0]
        clist = clicks_by_person.get(p, [])
        cts = click_ts.get(p, [])
        for start, members in sessions:
            day = epoch + datetime.timedelta(days=start // DAY)
            rev = sum(e[2] for e in members if e[1])
            has_p = any(e[1] for e in members)
            ud = user_daily.setdefault((p, day), [0, 0, 0, {}])
            ud[0] += 1
            ud[1] += len(members)
            ud[2] += rev
            for e in members:
                if e[1]:
                    ud[3][e[3]] = ud[3].get(e[3], 0) + e[2]
            # qualifying clicks: start - WEEK <= ts <= start
            lo = bisect.bisect_left(cts, start - WEEK)
            hi = bisect.bisect_right(cts, start)
            window = clist[lo:hi]
            if window:
                latest = max(c[0] for c in window)
                credit = min(c[1] for c in window if c[0] == latest)
                credit_campaign = next(c[2] for c in window if c[0] == latest and c[1] == credit)
            else:
                credit_campaign = b"organic"
            cd = camp_daily.setdefault((credit_campaign, day), [0, 0, 0, 0])
            cd[0] += 1
            cd[1] += 1 if has_p else 0
            cd[2] += rev
            for camp in {c[2] for c in window}:
                camp_daily.setdefault((camp, day), [0, 0, 0, 0])[3] += 1

    write(out, user_daily, camp_daily, campaigns)


def write(out, user_daily, camp_daily, campaigns):
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows_u = []
    for (p, day), (s, e, r, cats) in user_daily.items():
        top = None
        if cats:
            best = max(cats.values())
            top = min(c for c, v in cats.items() if v == best).decode("utf-8")
        rows_u.append((p.decode("utf-8"), day, s, e, r, top))
    rows_c = []
    for (c, day), (s, ps, r, a) in camp_daily.items():
        channel = b"organic" if c == b"organic" else campaigns[c]
        rows_c.append((c.decode("utf-8"), channel.decode("utf-8"), day, s, ps, r, a))

    def table(rows, names, types):
        cols = list(zip(*rows)) if rows else [[] for _ in names]
        return pa.table({n: pa.array(list(col), type=t) for n, col, t in zip(names, cols, types)})

    tu = table(rows_u, ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
               [pa.string(), pa.date32(), pa.int64(), pa.int64(), pa.int64(), pa.string()])
    tc = table(rows_c, ["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents",
                        "assisted_sessions"],
               [pa.string(), pa.string(), pa.date32(), pa.int64(), pa.int64(), pa.int64(), pa.int64()])
    for name, t in (("user_daily", tu), ("campaign_daily", tc)):
        os.makedirs(os.path.join(out, name), exist_ok=True)
        pq.write_table(t, os.path.join(out, name, "part-0.parquet"))


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        main(sys.argv[-2], sys.argv[-1])
    else:
        main(os.environ["DATA_DIR"], os.environ["OUTPUT_DIR"])
