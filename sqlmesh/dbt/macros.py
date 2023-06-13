from pathlib import Path

from sqlmesh.dbt.package import MacroConfig, MacroInfo


def dbt_utils_star() -> MacroConfig:
    definition = """
{% macro star(from, relation_alias=False, except=[], prefix='', suffix='', quote_identifiers=True) -%}
    {%- if prefix != '' -%}
        {{ exceptions.raise_compiler_error("prefix argument not currently supported for dbt_utils.star macro") }}
    {%- elif suffix != '' -%}
        {{ exceptions.raise_compiler_error("suffix argument not currently supported for dbt_utils.star macro") }}
    {%- endif -%}

    {{ from }}.* 
    {%- if except|length > 0 %} EXCEPT (
        {%- for col in except -%}
            {%- if not loop.first %}, {% endif -%}
            {%- if quote_identifiers -%}{{ adapter.quote(col)|trim }}{%- else -%}{{ col|trim }}{%- endif -%}
        {%- endfor -%}
        )
    {%- endif -%}
{% endmacro %}
"""
    return MacroConfig(info=MacroInfo(definition=definition, depends_on=[]), path=Path())


MACRO_OVERRIDES = {
    "dbt_utils": {
        "star": dbt_utils_star(),
    }
}
