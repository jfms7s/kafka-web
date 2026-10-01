import os
from pathlib import Path


def config_dir() -> Path:
    """`$KAFKA_WEB_CONFIG_DIR` if set (and non-empty), else `~/.config/kafka-web`."""
    override = os.environ.get("KAFKA_WEB_CONFIG_DIR")
    if override:
        return Path(override)
    return Path.home() / ".config" / "kafka-web"
