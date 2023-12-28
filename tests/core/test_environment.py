import pytest

from sqlmesh.core.environment import Environment, EnvironmentNamingInfo


def test_normalize_name():
    assert Environment.normalize_name("12foo!#%@") == "12foo____"
    assert Environment.normalize_name("__test@$@%") == "__test____"
    assert Environment.normalize_name("test") == "test"
    assert Environment.normalize_name("-test_") == "_test_"
    with pytest.raises(TypeError, match="Expected str or Environment, got int"):
        Environment.normalize_name(123)


@pytest.mark.parametrize(
    "mapping, name, expected",
    [
        # Match the first pattern
        ({"^prod$": "prod_catalog", "^dev$": "dev_catalog"}, "prod", "prod_catalog"),
        # Match the second pattern
        ({"^prod$": "prod_catalog", "^dev$": "dev_catalog"}, "dev", "dev_catalog"),
        # Match no pattern
        (
            {"^prod$": "prod_catalog", "^dev$": "dev_catalog"},
            "develop",
            None,
        ),
        # Match both patterns but take the first
        (
            {".*": "catchall", "^prod$": "will_never_happen"},
            "prod",
            "catchall",
        ),
        # Don't need to test invalid regex pattern since regex patterns are validated when the config is parsed
    ],
)
def test_from_environment_catalog_mapping(mapping, name, expected):
    assert (
        EnvironmentNamingInfo.from_environment_catalog_mapping(
            mapping,
            name,
        ).catalog_name_override
        == expected
    )
