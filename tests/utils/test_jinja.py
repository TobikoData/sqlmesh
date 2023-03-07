from __future__ import annotations

from sqlmesh.utils import AttributeDict
from sqlmesh.utils.jinja import (
    JinjaMacroRegistry,
    MacroExtractor,
    MacroReference,
    MacroReturnVal,
)


def test_macro_registry_render():
    package_a = "{% macro macro_a_a() %}macro_a_a{% endmacro %}"

    package_b = """
{% macro macro_b_a() %}macro_b_a{% endmacro %}

{% macro macro_b_b() %}
{{ package_a.macro_a_a() }}
{{ package_b.macro_b_a() }}
{{ macro_b_a() }}
macro_b_b
{% endmacro %}"""

    local_macros = "{% macro local_macro() %}{{ package_b.macro_b_b() }}{% endmacro %}"

    extractor = MacroExtractor()
    registry = JinjaMacroRegistry()

    registry.add_macros(extractor.extract(local_macros))
    registry.add_macros(extractor.extract(package_a), package="package_a")
    registry.add_macros(extractor.extract(package_b), package="package_b")

    rendered = (
        registry.build_environment()
        .from_string("{{ local_macro() }}{{ package_a.macro_a_a() }}")
        .render()
    )
    rendered = [r for r in rendered.split("\n") if r]

    assert rendered == [
        "macro_a_a",
        "macro_b_a",
        "macro_b_a",
        "macro_b_b",
        "macro_a_a",
    ]


def test_macro_registry_render_nested_self_package_references():
    package_a = """
{% macro macro_a_a() %}macro_a_a{% endmacro %}

{% macro macro_a_b() %}{{ package_a.macro_a_a() }}{% endmacro %}

{% macro macro_a_c() %}{{ package_a.macro_a_b() }}{% endmacro %}
"""

    extractor = MacroExtractor()
    registry = JinjaMacroRegistry()

    registry.add_macros(extractor.extract(package_a), package="package_a")

    rendered = registry.build_environment().from_string("{{ package_a.macro_a_c() }}").render()
    assert rendered == "macro_a_a"


def test_macro_registry_render_private_macros():
    package_a = """
{% macro _macro_a_a(flag) %}{% if not flag %}macro_a_a{% else %}{{ _macro_a_a(False) }}{% endif %}{% endmacro %}

{% macro macro_a_b() %}{{ package_a._macro_a_a(True) }}{% endmacro %}
"""

    extractor = MacroExtractor()
    registry = JinjaMacroRegistry()

    registry.add_macros(extractor.extract(package_a), package="package_a")

    rendered = registry.build_environment().from_string("{{ package_a.macro_a_b() }}").render()
    assert rendered == "macro_a_a"


def test_macro_registry_render_different_vars():
    package_a = "{% macro macro_a_a() %}{{ external() }}{% endmacro %}"

    local_macros = "{% macro local_macro() %}{{ package_a.macro_a_a() }}{% endmacro %}"

    extractor = MacroExtractor()
    registry = JinjaMacroRegistry()

    registry.add_macros(extractor.extract(local_macros))
    registry.add_macros(extractor.extract(package_a), package="package_a")

    rendered = (
        registry.build_environment(external=lambda: "test_a")
        .from_string("{{ local_macro() }}")
        .render()
    )
    assert rendered == "test_a"

    rendered = (
        registry.build_environment(external=lambda: "test_b")
        .from_string("{{ local_macro() }}")
        .render()
    )
    assert rendered == "test_b"


def test_macro_registry_trim():
    package_a = """
{% macro macro_a_a() %}macro_a_a{% endmacro %}
{% macro macro_a_b() %}macro_a_b{% endmacro %}
{% macro macro_a_c() %}macro_a_c{% endmacro %}
"""

    package_b = """
{% macro macro_b_a() %}{{ package_a.macro_a_a() }}{% endmacro %}

{% macro macro_b_b() %}{{ package_b.macro_b_a() }}{% endmacro %}

{% macro macro_b_c() %}{{ package_a.macro_a_c() }}{% endmacro %}
"""

    package_c = """
{% macro macro_c_a() %}macro_c_a{% endmacro %}
"""

    local_macros = """
{% macro local_macro_a() %}{{ package_b.macro_b_b() }}{% endmacro %}

{% macro local_macro_b() %}local_macro_b{% endmacro %}
"""

    extractor = MacroExtractor()
    registry = JinjaMacroRegistry()

    registry.add_macros(extractor.extract(local_macros))
    registry.add_macros(extractor.extract(package_a), package="package_a")
    registry.add_macros(extractor.extract(package_b), package="package_b")
    registry.add_macros(extractor.extract(package_c), package="package_c")

    trimmed_registry = registry.trim(
        [
            MacroReference(name="local_macro_a"),
            MacroReference(package="package_a", name="macro_a_b"),
        ]
    )

    assert set(trimmed_registry.packages) == {"package_a", "package_b"}
    assert set(trimmed_registry.packages["package_a"]) == {"macro_a_a", "macro_a_b"}
    assert set(trimmed_registry.packages["package_b"]) == {"macro_b_a", "macro_b_b"}
    assert set(trimmed_registry.root_macros) == {"local_macro_a"}

    rendered = (
        trimmed_registry.build_environment()
        .from_string("{{ local_macro_a() }} {{ package_a.macro_a_b() }}")
        .render()
    )
    assert rendered == "macro_a_a macro_a_b"


def test_macro_return():
    macros = "{% macro test_return() %}{{ macro_return([1, 2, 3]) }}{% endmacro %}"

    def macro_return(val):
        raise MacroReturnVal(val)

    extractor = MacroExtractor()
    registry = JinjaMacroRegistry()

    registry.add_macros(extractor.extract(macros))

    rendered = (
        registry.build_environment(macro_return=macro_return)
        .from_string("{{ test_return() }}")
        .render()
    )
    assert rendered == "[1, 2, 3]"


def test_global_objs():
    original_registry = JinjaMacroRegistry(global_objs={"target": AttributeDict({"test": "value"})})

    deserialized_registry = JinjaMacroRegistry.parse_raw(original_registry.json())
    assert deserialized_registry.global_objs["target"].test == "value"
