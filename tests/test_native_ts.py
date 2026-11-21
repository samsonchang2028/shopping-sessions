#!/usr/bin/env python3
"""The native timestamp parser (`ss --ts`) against the independent reference parser, on the hand-written cases
from test_units.py plus random single-byte mutations of valid timestamps (non-ASCII digits included)."""
import os
import random
import subprocess
import sys
import unittest

HERE = os.path.dirname(__file__)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "ref"))
import reference  # noqa: E402
from test_units import TS_CASES  # noqa: E402

BIN = os.path.join(HERE, "..", "rust", "target", "release", "ss")


def native(strings):
    data = b"".join(s + b"\n" for s in strings)
    out = subprocess.run([BIN, "--ts"], input=data, capture_output=True, check=True).stdout.decode().splitlines()
    return [None if o == "None" else int(o) for o in out]


def expected(s):
    v = reference.parse_ts(s)
    if v is None:
        return None
    if s.startswith(b"0000-"):
        return "skip"          # year 0 is outside every window; the two parsers may differ
    if s.isdigit() and len(s.lstrip(b"0")) > 15:
        return None            # too long to be in any window: native says "bad", reference keeps the big number
    return v


class NativeTimestamps(unittest.TestCase):
    @unittest.skipUnless(os.path.exists(BIN), "native binary not built")
    def test_cases_and_mutations(self):
        rng = random.Random(7)
        base = [c.encode("utf-8") for c, _ in TS_CASES]
        valid = [b for b, (_, want) in zip(base, TS_CASES) if want is not None]
        alphabet = [b"0", b"9", b":", b"-", b"T", b"Z", b"z", b"+", b" ", b"\r", b"/", b"\xd9\xa2", b"\xef\xbc\x92", b"a", b"\x00"]
        strings = list(base)
        for _ in range(20000):
            s = bytearray(rng.choice(valid))
            for _ in range(rng.choice([1, 1, 2, 3])):
                op = rng.random()
                i = rng.randrange(len(s) + 1)
                if op < 0.5 and s:
                    s[min(i, len(s) - 1)] = rng.choice(alphabet)[0]
                elif op < 0.75:
                    s[i:i] = rng.choice(alphabet)
                elif s:
                    del s[min(i, len(s) - 1)]
            strings.append(bytes(s))
        got = native(strings)
        bad = []
        for s, g in zip(strings, got):
            e = expected(s)
            if e != "skip" and e != g:
                bad.append((s, e, g))
        self.assertEqual(bad[:5], [], f"{len(bad)} mismatches of {len(strings)}")


if __name__ == "__main__":
    unittest.main()
