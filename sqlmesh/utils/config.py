from typing import Any, Optional, Set

from sqlmesh.core.config.connection import ConnectionConfig
from sqlmesh.utils import yaml


# Fields that should be excluded from the configuration hash
excluded_fields: Set[str] = {
    "concurrent_tasks",
    "pre_ping",
    "register_comments",
}

# Sensitive fields that should be masked in the configuration print or hash
sensitive_fields: Set[str] = {
    "access_token",
    "api_key",
    "auth_token",
    "client_secret",
    "certificate",
    "credentials",
    "user",
    "password",
    "keytab",
    "keyfile",
    "keyfile_json",
    "principal",
    "private_key",
    "private_key_passphrase",
    "private_key_path",
    "refresh_token",
    "secret",
    "ssh_key",
    "token",
}


def is_sensitive_field(field_name: str, sensitive_fields: Set[str]) -> bool:
    """
    Check if a field name contains any sensitive keywords
    """
    field_lower = field_name.lower()
    return any(sensitive in field_lower for sensitive in sensitive_fields)


def mask_sensitive_value(value: Any) -> str:
    """
    Mask sensitive values with a placeholder
    Returns '****' for non-empty values and '' for empty ones
    """
    if value and str(value).strip():
        return "****"
    return "None"


def print_config(config: Optional[ConnectionConfig], console: Any, title: str) -> None:
    """
    Print configuration while masking sensitive information

    Args:
        config: Pydantic model containing configuration
        console: Console object with log_status_update method
    """
    if not config:
        console.log_status_update("No connection configuration found.")
        return

    config_dict = config.dict(mode="json")

    for field_name in config_dict:
        if is_sensitive_field(field_name, sensitive_fields):
            config_dict[field_name] = mask_sensitive_value(config_dict[field_name])

    configWithTitle = {title: config_dict}
    yaml_output = yaml.dump(configWithTitle)

    console.log_status_update("\n\n")
    console.log_status_update(yaml_output)
