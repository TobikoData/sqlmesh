from typing import Any, Optional, Set

from sqlmesh.core.config.connection import ConnectionConfig

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
        return "*" * len(str(value))
    return "None"


def print_config(config: Optional[ConnectionConfig], console: Any) -> None:
    """
    Print configuration while masking sensitive information

    Args:
        config: Pydantic model containing configuration
        console: Console object with log_status_update method
    """
    if not config:
        console.log_status_update("No connection configuration found.")
        return

    for field_name, value in config.dict().items():
        if is_sensitive_field(field_name, sensitive_fields):
            masked_value = mask_sensitive_value(value)
            console.log_status_update(f"{field_name}: {masked_value}")
        else:
            console.log_status_update(f"{field_name}: {value}")
