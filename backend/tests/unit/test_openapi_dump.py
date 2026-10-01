import json

import pytest

from kafka_web import openapi_dump


def test_dump_prints_the_openapi_document(capsys: pytest.CaptureFixture[str]) -> None:
    openapi_dump.main()
    doc = json.loads(capsys.readouterr().out)
    assert "/api/clusters" in doc["paths"]
    assert "ClusterView" in doc["components"]["schemas"]
