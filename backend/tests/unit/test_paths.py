from pathlib import Path

from kafka_web.config.paths import config_dir


def test_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("KAFKA_WEB_CONFIG_DIR", str(tmp_path / "x"))
    assert config_dir() == tmp_path / "x"


def test_default_is_under_home(monkeypatch):
    monkeypatch.delenv("KAFKA_WEB_CONFIG_DIR", raising=False)
    assert config_dir() == Path.home() / ".config" / "kafka-web"
