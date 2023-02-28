from __future__ import annotations

from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroExtractor, MacroReference


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

    rendered = registry.render("{{ local_macro() }}{{ package_a.macro_a_a() }}")
    rendered = [r for r in rendered.split("\n") if r]

    assert rendered == [
        "macro_a_a",
        "macro_b_a",
        "macro_b_a",
        "macro_b_b",
        "macro_a_a",
    ]


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

    rendered = trimmed_registry.render("{{ local_macro_a() }} {{ package_a.macro_a_b() }}")
    assert rendered == "macro_a_a macro_a_b"
