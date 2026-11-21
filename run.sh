#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
# Native engine first. It exits 3 only when the dataset is outside what it supports (very long window,
# oversized campaign x day table); then, and only then, the DuckDB engine runs. Any other failure is an error.
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
if [ "${ENGINE:-native}" = "native" ] && [ -x /app/ss ]; then
  rc=0
  /app/ss || rc=$?
  [ "$rc" -eq 0 ] && exit 0
  [ "$rc" -eq 3 ] || exit "$rc"
  echo "native engine declined this dataset; running the DuckDB engine" >&2
  rm -rf "$OUTPUT_DIR/user_daily" "$OUTPUT_DIR/campaign_daily"
fi
exec python3 /app/pipeline/pipeline.py
