import typing as t
from urllib.parse import urlparse

from sqlmesh.utils.errors import SQLMeshError


def validate_s3_uri(
    value: str, base: bool = False, error_type: t.Type[Exception] = SQLMeshError
) -> str:
    if not value.startswith("s3://"):
        raise error_type(f"Location '{value}' must be a s3:// URI")

    if base and not value.endswith("/"):
        value = value + "/"

    # To avoid HIVE_METASTORE_ERROR: S3 resource path length must be less than or equal to 700.
    if len(value) > 700:
        raise error_type(f"Location '{value}' cannot be more than 700 characters")

    return value


def parse_s3_uri(s3_uri: str) -> t.Tuple[str, str]:
    """
    Given a s3:// URI, parse it into a pair of (bucket, key)

    Note that this could be any URI, including a file key, so we dont add a trailing / unlike validate_s3_base_uri
    """
    validate_s3_uri(s3_uri)

    parsed_uri = urlparse(s3_uri)

    bucket = parsed_uri.netloc
    key = parsed_uri.path

    if key:
        key = key[1:]  # trim off leading /

    return bucket, key
