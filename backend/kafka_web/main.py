"""ASGI entry point: `uvicorn kafka_web.main:app --host 127.0.0.1 --port 8000`.

Set `KAFKA_WEB_STATIC_DIR` to also serve the built frontend (what `./start.sh prod` does).
"""

import os
from pathlib import Path

from kafka_web.api.app import create_app

_static_dir = os.environ.get("KAFKA_WEB_STATIC_DIR")
app = create_app(static_dir=Path(_static_dir) if _static_dir else None)
