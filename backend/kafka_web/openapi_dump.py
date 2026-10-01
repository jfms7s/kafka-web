"""Print the OpenAPI document to stdout: `uv run python -m kafka_web.openapi_dump`.

Feeds `npm run gen:api`. Builds the app with throwaway collaborators so it touches neither the
real config directory nor the OS keyring.
"""

import json
import sys
import tempfile
from pathlib import Path

from keyring.backends.null import Keyring as NullKeyring

from kafka_web.api.app import create_app
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = create_app(store=ClusterStore(Path(tmp), SecretStore(NullKeyring())))
        sys.stdout.write(json.dumps(app.openapi()))


if __name__ == "__main__":
    main()
