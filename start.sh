#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
mode="${1:-dev}"
if [[ "$mode" == "prod" ]]; then
  (cd "$root/frontend" && npm run build)
  cd "$root/backend"
  KAFKA_WEB_STATIC_DIR="$root/frontend/dist" exec uv run uvicorn kafka_web.main:app --host 127.0.0.1 --port 8000
fi
trap 'kill 0' EXIT INT TERM
(cd "$root/backend" && uv run uvicorn kafka_web.main:app --reload --host 127.0.0.1 --port 8000) &
(cd "$root/frontend" && npm run dev) &
wait
