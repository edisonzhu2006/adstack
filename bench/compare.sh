#!/usr/bin/env bash
# Diff streamkit output against the oracle, order-insensitively.
#   bench/compare.sh truth.csv results.csv
set -euo pipefail
if [ $# -ne 2 ]; then echo "usage: $0 truth.csv results.csv" >&2; exit 2; fi
a=$(mktemp); b=$(mktemp); trap 'rm -f "$a" "$b"' EXIT
tail -n +2 "$1" | LC_ALL=C sort > "$a"
tail -n +2 "$2" | LC_ALL=C sort > "$b"
if cmp -s "$a" "$b"; then
  echo "PASS: $(wc -l < "$a" | tr -d ' ') windows identical"
else
  echo "FAIL: outputs differ"
  diff "$a" "$b" | head -20
  echo "truth=$(wc -l < "$a" | tr -d ' ') rows, result=$(wc -l < "$b" | tr -d ' ') rows"
  exit 1
fi
