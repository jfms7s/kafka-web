from kafka_web.errors import NotFound, ValidationFailed


def test_subclass_status_and_default_code():
    err = NotFound("x")

    assert err.status == 404
    assert err.code == "not_found"
    assert err.message == "x"


def test_code_override_and_field():
    err = ValidationFailed("bad", code="truststore_empty", field="truststore")

    assert err.to_body() == {
        "code": "truststore_empty",
        "message": "bad",
        "field": "truststore",
    }


def test_body_without_field_has_no_field_key():
    assert NotFound("x").to_body() == {"code": "not_found", "message": "x"}
