"""ASGI entry point: `uvicorn kafka_web.main:app --host 127.0.0.1 --port 8000`."""

from kafka_web.api.app import create_app

app = create_app()
