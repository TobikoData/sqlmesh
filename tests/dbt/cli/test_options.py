import typing as t
import pytest
from sqlmesh_dbt.options import YamlParamType
from click.exceptions import BadParameter


@pytest.mark.parametrize(
    "input,expected",
    [
        (1, BadParameter("Input value '1' should be a string")),
        ("", BadParameter("String '' is not valid YAML")),
        ("['a', 'b']", BadParameter("String.*did not evaluate to a dict, got.*")),
        ("foo: bar", {"foo": "bar"}),
        ('{"key": "value", "date": 20180101}', {"key": "value", "date": 20180101}),
        ("{key: value, date: 20180101}", {"key": "value", "date": 20180101}),
    ],
)
def test_yaml_param_type(input: str, expected: t.Union[BadParameter, t.Dict[str, t.Any]]):
    if isinstance(expected, BadParameter):
        with pytest.raises(BadParameter, match=expected.message):
            YamlParamType().convert(input, None, None)
    else:
        assert YamlParamType().convert(input, None, None) == expected
