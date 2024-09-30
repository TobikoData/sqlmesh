import pytest
from sqlmesh.utils.errors import SQLMeshError, ConfigError
from sqlmesh.utils.aws import validate_s3_uri, parse_s3_uri


def test_validate_s3_uri():
    with pytest.raises(SQLMeshError, match=r".*must be a s3://.*"):
        validate_s3_uri("hdfs://foo/bar")

    with pytest.raises(ConfigError, match=r".*must be a s3://.*"):
        validate_s3_uri("hdfs://foo/bar", error_type=ConfigError)

    with pytest.raises(SQLMeshError, match=r".*must be a s3://.*"):
        validate_s3_uri("/foo/bar")

    with pytest.raises(SQLMeshError, match=r".*cannot be more than 700 characters"):
        long_path = "foo/bar/" * 100
        assert len(long_path) > 700
        validate_s3_uri(f"s3://{long_path}")

    assert validate_s3_uri("s3://foo/bar/") == "s3://foo/bar/"
    assert validate_s3_uri("s3://foo/bar/baz") == "s3://foo/bar/baz"
    assert validate_s3_uri("s3://foo/bar/baz", base=True) == "s3://foo/bar/baz/"


def test_parse_s3_uri():
    with pytest.raises(SQLMeshError, match=r".*must be a s3://.*"):
        parse_s3_uri("hdfs://foo/bar")

    assert parse_s3_uri("s3://foo") == ("foo", "")
    assert parse_s3_uri("s3://foo/") == ("foo", "")
    assert parse_s3_uri("s3://foo/bar") == ("foo", "bar")
    assert parse_s3_uri("s3://foo/bar/") == ("foo", "bar/")
    assert parse_s3_uri("s3://foo/bar/baz/bing.txt") == ("foo", "bar/baz/bing.txt")
