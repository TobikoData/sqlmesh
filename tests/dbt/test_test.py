from sqlmesh.dbt.test import TestConfig


def test_multiline_test_kwarg() -> None:
    test = TestConfig(
        name="test",
        sql="{{ test(**_dbt_generic_test_kwargs) }}",
        test_kwargs={"test_field": "foo\nbar\n"},
    )
    assert test._kwargs() == 'test_field="foo\nbar"'
