#!/usr/bin/env python3
"""
Multi-seed differential test: generator -> pipeline (CHECK=1) and ref/reference.py -> exact diff.

Usage: fuzz.py [--events N] [--work DIR] [SEED ...]
Default seeds include 15 and 272, whose windows contain 2028-02-29.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RUST = ROOT / "rust" / "target" / "release" / "ss"
DEFAULT_SEEDS = [2, 3, 4, 5, 7, 11, 15, 42, 272, 1001]


def run(cmd, env=None):
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if p.returncode != 0:
        sys.exit(f"command failed: {' '.join(map(str, cmd))}\n{p.stderr[-3000:]}")
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=150_000)
    ap.add_argument("--work", default=str(ROOT / "fuzz-data"))
    ap.add_argument("--generate", default=shutil.which("generate") or os.path.expanduser("~/.local/bin/generate"))
    ap.add_argument("seeds", nargs="*", type=int)
    args = ap.parse_args()
    seeds = args.seeds or DEFAULT_SEEDS
    py = sys.executable
    work = Path(args.work)
    failed = []
    for seed in seeds:
        d = work / f"s{seed}"
        shutil.rmtree(d, ignore_errors=True)
        events = args.events
        run([args.generate, "--seed", str(seed), "--events", str(events), "--clicks", str(events // 4),
             "--output", str(d / "data")])
        window = json.loads((d / "data" / "manifest.json").read_text())
        run([py, str(ROOT / "ref" / "reference.py"), str(d / "data"), str(d / "ref")])
        engines = {"duckdb": [py, str(ROOT / "pipeline" / "pipeline.py")]}
        if RUST.exists():
            engines["rust"] = [str(RUST)]
        ok = True
        verdicts = []
        for name, cmd in engines.items():
            env = dict(os.environ, DATA_DIR=str(d / "data"), OUTPUT_DIR=str(d / name), CHECK="1")
            run(cmd, env=env)
            p = subprocess.run([py, str(HERE / "compare.py"), str(d / name), str(d / "ref")],
                               capture_output=True, text=True)
            good = p.returncode == 0
            ok &= good
            verdicts.append(f"{name} {'MATCH' if good else 'DIFFER'}")
            if not good:
                print(p.stdout)
        print(f"seed {seed:>5} events {events} window {window['window_start'][:10]}..{window['window_end'][:10]}: "
              + ", ".join(verdicts))
        if not ok:
            failed.append(seed)
        else:
            shutil.rmtree(d, ignore_errors=True)
    print(f"{len(seeds) - len(failed)}/{len(seeds)} seeds match" + (f"; failed {failed}" if failed else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
