import typing as t

from sqlmesh.utils.pydantic import PydanticModel, field_validator
from sqlglot.dialects.dialect import UNESCAPED_SEQUENCES
from sqlglot import exp


# Nested settings for properties fields (e.g., bootstrap server, sasl, ssl)
class PropertiesSettings(PydanticModel):
    connector: t.Optional[str] = None
    topic: t.Optional[str] = None
    primary_key: t.Optional[str] = None
    type: t.Optional[str] = None
    jdbc_url: t.Optional[str] = None
    table_name: t.Optional[str] = None
    properties_bootstrap_server: t.Optional[str] = None
    properties_sasl_mechanism: t.Optional[str] = None
    properties_sasl_username: t.Optional[str] = None
    properties_sasl_password: t.Optional[str] = None
    properties_security_protocol: t.Optional[str] = None
    properties_ssl_ca_location: t.Optional[str] = None
    properties_ssl_certificate_location: t.Optional[str] = None
    properties_ssl_key_location: t.Optional[str] = None
    properties_ssl_endpoint_identification_algorithm: t.Optional[str] = None


# Nested settings for properties fields (e.g., bootstrap server, sasl, ssl)
class FormatSettings(PydanticModel):
    format: t.Optional[str] = None
    encode: t.Optional[str] = None
    schema_registry_name_strategy: t.Optional[str] = None
    schema_registry: t.Optional[str] = None
    force_append_only: t.Optional[str] = None


class RwSinkSettings(PydanticModel):
    """Settings for a Sink that include connector, properties, topic, format, and encoding."""

    properties: t.Optional[PropertiesSettings] = None  # Nested model for properties
    # FORMAT and ENCODE fields with 'f_' prefix
    format: t.Optional[FormatSettings] = None

    @field_validator("properties", mode="before")
    @classmethod
    def _properties_validator(cls, v: t.Any) -> t.Optional[PropertiesSettings]:
        if v is None or not isinstance(v, exp.Expression):
            return v
        v = v.this
        return UNESCAPED_SEQUENCES.get(v, v)

    @field_validator("format", mode="before")
    @classmethod
    def _format_validator(cls, v: t.Any) -> t.Optional[FormatSettings]:
        if v is None or not isinstance(v, exp.Expression):
            return v
        v = v.this
        return UNESCAPED_SEQUENCES.get(v, v)
