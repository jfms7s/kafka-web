#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
# CI sets COVERAGE_DIR (an absolute path) to also write py.lcov and ts/lcov.info there; unset, nothing changes.
py_cov=() ts_cov=()
if [ -n "${COVERAGE_DIR:-}" ]; then
  mkdir -p "$COVERAGE_DIR"
  export COVERAGE_FILE="$COVERAGE_DIR/.coverage"
  py_cov=(--cov=kafka_web --cov-branch "--cov-report=lcov:$COVERAGE_DIR/py.lcov")
  ts_cov=(-- --coverage.enabled --coverage.provider=v8 --coverage.reporter=lcovonly
    "--coverage.reportsDirectory=$COVERAGE_DIR/ts" '--coverage.include=src/**')
fi
(cd "$root/backend" && uv run ruff check . && uv run ruff format --check . && uv run pytest -q ${py_cov[@]+"${py_cov[@]}"})
(cd "$root/frontend" && npm run lint && npm run typecheck && npm test ${ts_cov[@]+"${ts_cov[@]}"})
