"""Cluster models: the persisted config and the API input, with always-valid invariants.

Every validation failure surfaces as `ValidationFailed` with `field` set, whether the model is
built directly, from a request body, or from the YAML file. `ValidationFailed` is deliberately not
a `ValueError`: pydantic re-raises such exceptions from validators unchanged instead of folding
them into a `ValidationError`.
"""

from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    StringConstraints,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    field_validator,
    model_validator,
)

from kafka_web.errors import ValidationFailed

SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
SaslMechanism = Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
NAME_PATTERN = r"^[a-z0-9][a-z0-9-]{0,62}$"
FORBIDDEN_EXTRA_PREFIXES = ("bootstrap.servers", "security.protocol", "sasl.", "ssl.ca.")

_FORBIDDEN_EXTRA_EXACT = FORBIDDEN_EXTRA_PREFIXES[:2]
_FORBIDDEN_EXTRA_STARTS = FORBIDDEN_EXTRA_PREFIXES[2:]

_NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def to_validation_failed(exc: ValidationError) -> ValidationFailed:
    """Map a pydantic `ValidationError` onto the first failing field.

    The message deliberately omits the offending input value (it may be a secret) and the
    original exception is not chained (its repr embeds the input).
    """
    first = exc.errors(include_url=False, include_input=False, include_context=False)[0]
    field = str(first["loc"][0]) if first["loc"] else None
    message = f"{field}: {first['msg']}" if field else first["msg"]
    return ValidationFailed(message, field=field)


class ClusterBase(BaseModel):
    """Fields shared by the stored config and the API input."""

    name: str = Field(pattern=NAME_PATTERN)
    env: _NonBlank
    region: str | None = None
    bootstrap_servers: _NonBlank
    security_protocol: SecurityProtocol = "PLAINTEXT"
    sasl_mechanism: SaslMechanism | None = None
    sasl_username: str | None = None
    read_only: bool = False
    extra: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="wrap")
    @classmethod
    def _failures_as_validation_failed(
        cls, data: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
    ) -> Self:
        try:
            return handler(data)
        except ValidationError as exc:
            raise to_validation_failed(exc) from None

    @field_validator("extra")
    @classmethod
    def _extra_must_not_shadow_typed_fields(cls, extra: dict[str, str]) -> dict[str, str]:
        for key in extra:
            if key in _FORBIDDEN_EXTRA_EXACT or key.startswith(_FORBIDDEN_EXTRA_STARTS):
                raise ValidationFailed(
                    f"extra: property {key!r} is managed by a dedicated field", field="extra"
                )
        return extra

    @model_validator(mode="after")
    def _sasl_fields_follow_protocol(self) -> Self:
        if not self.uses_sasl:
            self.sasl_mechanism = None
            self.sasl_username = None
        elif self.sasl_mechanism is None:
            raise ValidationFailed(
                "sasl_mechanism: required for SASL protocols", field="sasl_mechanism"
            )
        elif not self.sasl_username:
            raise ValidationFailed(
                "sasl_username: required for SASL protocols", field="sasl_username"
            )
        return self

    @property
    def uses_tls(self) -> bool:
        return self.security_protocol in ("SSL", "SASL_SSL")

    @property
    def uses_sasl(self) -> bool:
        return self.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL")


class ClusterConfig(ClusterBase):
    """What is persisted in clusters.yaml (never contains secrets)."""

    truststore: str | None = None  # path relative to the config dir, e.g. "truststores/stg-eu.pem"

    @model_validator(mode="after")
    def _truststore_follows_protocol(self) -> Self:
        if not self.uses_tls:
            self.truststore = None
        elif self.truststore is None:
            raise ValidationFailed(
                "truststore: required for SSL protocols",
                code="truststore_required",
                field="truststore",
            )
        return self


class ClusterInput(ClusterBase):
    """API create/update body; carries write-only secrets that are never persisted in YAML."""

    sasl_password: SecretStr | None = None
    truststore_base64: str | None = Field(default=None, repr=False)
    truststore_password: SecretStr | None = None
