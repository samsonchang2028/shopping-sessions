#!/usr/bin/env python3
"""Randomized differential test of the CSV line grammar: pipeline fast reader, pipeline exact reader,
and ref/reference.py's byte parser must yield the same multiset of field tuples."""
import collections
import os
import random
import sys
import tempfile
import unittest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "pipeline"))
sys.path.insert(0, os.path.join(HERE, "..", "ref"))
import pipeline  # noqa: E402
import reference  # noqa: E402

ALPHABET = ["a", "b", "1", ",", ",", '"', '"', " ", "\t", "é", "日", "\x1f", ""]


def rand_field(rng):
    kind = rng.random()
    body = "".join(rng.choice(["a", "b", "1", " ", "\t", "é"]) for _ in range(rng.randint(0, 3)))
    if kind < 0.5:
        return body
    if kind < 0.75:
        return f'"{body}"'
    return "".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 4)))


def rand_line(rng, arity):
    r = rng.random()
    if r < 0.05:
        return ""
    if r < 0.15:
        return "".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 12)))
    n = arity if r < 0.7 else max(1, arity + rng.choice([-2, -1, 1, 2, 5]))
    return ",".join(rand_field(rng) for _ in range(n))


class Grammar(unittest.TestCase):
    def check(self, arity, seed, n_lines=3000, with_cr=False):
        rng = random.Random(seed)
        with tempfile.TemporaryDirectory() as d:
            fam = os.path.join(d, "fam")
            os.makedirs(fam)
            for fi in range(3):
                lines = ["h" + ",h" * (arity - 1)] + [rand_line(rng, arity) for _ in range(n_lines // 3)]
                if with_cr:
                    lines = [ln + ("\r" if rng.random() < 0.1 else "") for ln in lines]
                text = "\n".join(lines) + ("\n" if fi != 1 else "")   # file 1 has no final newline
                with open(os.path.join(fam, f"p{fi}.csv"), "w", encoding="utf-8", newline="") as f:
                    f.write(text)
            with open(os.path.join(fam, "empty.csv"), "w") as f:
                f.write("h" + ",h" * (arity - 1) + "\n")
            want = collections.Counter(
                tuple(None if v is None else v.decode("utf-8") for v in row)
                for row in reference.read_family(fam, arity))
            con = pipeline.connect(2, 2, d)
            names = [f"n{i}" for i in range(arity)]
            got = {}
            for mode in ("fast", "exact"):
                try:
                    got[mode] = collections.Counter(con.execute(pipeline.family_sql(fam, names, mode)).fetchall())
                except Exception as error:  # the fast reader must refuse only when \r is present
                    self.assertTrue(with_cr and mode == "fast", f"{mode} failed: {error}")
            self.assertEqual(got["exact"], want)
            if "fast" in got:
                self.assertEqual(got["fast"], want)
            elif not with_cr:
                self.fail("fast reader refused input without \\r")
            return got

    def test_arity_7(self):
        for seed in range(4):
            self.check(7, seed)

    def test_arity_4(self):
        for seed in range(4):
            self.check(4, 100 + seed)

    def test_arity_2(self):
        for seed in range(4):
            self.check(2, 200 + seed)

    def test_carriage_return_refuses_fast(self):
        got = self.check(4, 300, with_cr=True)
        self.assertNotIn("fast", got)


if __name__ == "__main__":
    unittest.main()
