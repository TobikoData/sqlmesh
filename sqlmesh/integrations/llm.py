from __future__ import annotations

import typing as t

from langchain import LLMChain, PromptTemplate
from langchain.chat_models import ChatOpenAI

from sqlmesh.core.model import Model

_QUERY_PROMPT_TEMPLATE = """Given an input request, create a syntactically correct {dialect} SQL query.
Use full table names.
Convert string operands to lowercase in the WHERE clause.
Reply with a SQL query and nothing else.

Use the following tables and columns:

{table_info}

Request: {input}"""


class LLMIntegration:
    def __init__(
        self,
        models: t.Iterable[Model],
        dialect: str,
        temperature: float = 0.7,
        verbose: bool = False,
    ):
        query_prompt_template = PromptTemplate.from_template(_QUERY_PROMPT_TEMPLATE).partial(
            dialect=dialect, table_info=_to_table_info(models)
        )
        llm = ChatOpenAI(temperature=temperature)  # type: ignore
        self._query_chain = LLMChain(llm=llm, prompt=query_prompt_template, verbose=verbose)

    def query(self, prompt: str) -> str:
        result = self._query_chain.predict(input=prompt).strip()
        select_pos = result.find("SELECT")
        if select_pos >= 0:
            return result[select_pos:]
        return result


def _to_table_info(models: t.Iterable[Model]) -> str:
    infos = []
    for model in models:
        if not model.kind.is_materialized:
            continue

        columns_csv = ", ".join(model.columns_to_types_or_raise)
        infos.append(f"Table: {model.name}\nColumns: {columns_csv}\n")

    return "\n".join(infos)
