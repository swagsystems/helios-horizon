#!/usr/bin/env bash
set -euo pipefail

status=0
trap 'status=$?; if (( status == 0 )); then echo "PASS: quality gate"; else echo "FAIL: quality gate (exit $status)"; fi' EXIT

root_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root_dir"

pytest_args=(-q)
if (( $# > 1 )) || { (( $# == 1 )) && [[ $1 != --cov ]]; }; then
  echo "usage: $0 [--cov]" >&2
  exit 2
fi
if (( $# == 1 )); then
  pytest_args+=(--cov=game_control --cov-report=term)
fi

uv lock --check
uv run --frozen pytest "${pytest_args[@]}"
uv run --frozen python tools/quality/check_public_boundary.py --root "$root_dir"
node --check web/app.js web/palette.js web/commands.js
git diff --check
