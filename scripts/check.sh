#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
(cd "$root/backend" && uv run ruff check . && uv run ruff format --check . && uv run pytest -q)
(cd "$root/frontend" && npm run lint && npm run typecheck && npm test)
