import typing as t
from types import MappingProxyType
from sqlmesh.core.model import Model
from jinja2 import Environment
import jinja2.nodes as j
from sqlmesh.dbt.converter.common import (
    SQLMESH_PREDEFINED_MACRO_VARIABLES,
    JinjaTransform,
    SQLGlotTransform,
)
from dbt.adapters.base.relation import BaseRelation
from sqlmesh.core.dialect import normalize_model_name
from sqlglot import exp
import sqlmesh.core.dialect as d
from functools import wraps


def _make_standalone_call_transform(fn_name: str, handler: JinjaTransform) -> JinjaTransform:
    """
    Creates a transform that identifies standalone Call nodes (that arent nested in other Call nodes) and replaces them with nodes
    containing the result of the handler() function
    """

    def _handle(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        if isinstance(node, j.Call):
            if isinstance(parent, (j.Call, j.List, j.Keyword)):
                return node

            if (name := node.find(j.Name)) and name.name == fn_name:
                return handler(node, prev, parent)

        return node

    return _handle


def _make_single_expression_transform(
    mapping: t.Union[
        t.Dict[str, str],
        t.Callable[[j.Node, t.Optional[j.Node], t.Optional[j.Node], str], t.Optional[str]],
    ],
) -> JinjaTransform:
    """
    Creates a transform that looks for standalone {{ expression }} nodes
    It then looks up 'expression' in the provided mapping and replaces it with a TemplateData node containing the value
    """

    def _handle(node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]) -> j.Node:
        # the assumption is that individual expressions are nested in between TemplateData
        if prev and not isinstance(prev, j.TemplateData):
            return node

        if isinstance(node, j.Name) and not isinstance(parent, j.Getattr):
            if isinstance(mapping, dict):
                result = mapping.get(node.name)
            else:
                result = mapping(node, prev, parent, node.name)
            if result is not None:
                return j.TemplateData(result)

        return node

    return _handle


def _dbt_relation_to_model_name(
    models: MappingProxyType[str, t.Union[Model, str]], relation: BaseRelation, dialect: str
) -> t.Optional[str]:
    model_fqn = normalize_model_name(
        table=relation.render(), default_catalog=relation.database, dialect=dialect
    )
    if resolved_value := models.get(model_fqn):
        return resolved_value if isinstance(resolved_value, str) else resolved_value.name
    return None


def _dbt_relation_to_kwargs(relation: BaseRelation) -> t.List[j.Keyword]:
    kwargs = []
    if database := relation.database:
        kwargs.append(j.Keyword("database", j.Const(database)))
    if schema := relation.schema:
        kwargs.append(j.Keyword("schema", j.Const(schema)))
    if identifier := relation.identifier:
        kwargs.append(j.Keyword("identifier", j.Const(identifier)))
    return kwargs


ASTTransform = t.TypeVar("ASTTransform", JinjaTransform, SQLGlotTransform)


def ast_transform(fn: t.Callable[..., ASTTransform]) -> t.Callable[..., ASTTransform]:
    """
    Decorator to mark functions as being Jinja or SQLGlot AST transforms

    The purpose is to set __name__ to be the outer function name so that the transforms have stable names for an exclude list
    The function itself as well as the ASTTransform returned by the function should have the same __name__ for this to work
    """

    @wraps(fn)
    def wrapper(*args: t.Any, **kwargs: t.Any) -> ASTTransform:
        result = fn(*args, **kwargs)
        result.__name__ = fn.__name__
        return result

    return wrapper


@ast_transform
def resolve_dbt_ref_to_model_name(
    models: MappingProxyType[str, t.Union[Model, str]], env: Environment, dialect: str
) -> JinjaTransform:
    """
    Takes an expression like "{{ ref('foo') }}"
    And turns it into "sqlmesh.foo" based on the provided list of models and resolver() function

    Args:
        models: A dict of models (or model names) keyed by model fqn
        jinja_env: Should contain an implementation of {{ ref() }} to turn a DBT relation name into a DBT relation object

    Returns:
        A string containing the **model name** (not fqn) of the model referenced by the DBT "{{ ref() }}" call
    """

    ref: t.Callable = env.globals["ref"]  # type: ignore

    def _resolve(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        if isinstance(node, j.Call) and node.args and isinstance(node.args[0], j.Const):
            ref_name = node.args[0].value
            version = None
            if version_kwarg := next((k for k in node.kwargs if k.key in ("version", "v")), None):
                if isinstance(version_kwarg.value, j.Const):
                    version = version_kwarg.value.value
                else:
                    # the version arg is present but its some kind of dynamic runtime value
                    # this means we cant resolve the ref to a model
                    return node

            if relation := ref(ref_name, version=version):
                if not isinstance(relation, BaseRelation):
                    raise ValueError(
                        f"ref() returned non-relation type for '{ref_name}': {relation}"
                    )
                if model_name := _dbt_relation_to_model_name(models, relation, dialect):
                    return j.TemplateData(model_name)
            return j.TemplateData(f"__unresolved_ref__.{ref_name}")

        return node

    return _make_standalone_call_transform("ref", _resolve)


@ast_transform
def rewrite_dbt_ref_to_migrated_ref(
    models: MappingProxyType[str, t.Union[Model, str]], env: Environment, dialect: str
) -> JinjaTransform:
    """
    Takes an expression like "{{ ref('foo') }}"
    And turns it into "{{ __migrated_ref(database='foo', schema='bar', identifier='baz', sqlmesh_model_name='') }}"
    so that the SQLMesh Native loader can construct a Relation instance without needing the Context

    Args:
        models: A dict of models (or model names) keyed by model fqn
        jinja_env: Should contain an implementation of {{ ref() }} to turn a DBT relation name into a DBT relation object

    Returns:
        A new Call node with enough data to reconstruct the Relation
    """

    ref: t.Callable = env.globals["ref"]  # type: ignore

    def _rewrite(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        if isinstance(node, j.Call) and isinstance(node.node, j.Name) and node.node.name == "ref":
            if node.args and isinstance(node.args[0], j.Const):
                ref_name = node.args[0].value
                version_kwarg = next((k for k in node.kwargs if k.key == "version"), None)
                if (relation := ref(ref_name)) and isinstance(relation, BaseRelation):
                    if model_name := _dbt_relation_to_model_name(models, relation, dialect):
                        kwargs = _dbt_relation_to_kwargs(relation)
                        if version_kwarg:
                            kwargs.append(version_kwarg)
                        kwargs.append(j.Keyword("sqlmesh_model_name", j.Const(model_name)))
                        return j.Call(j.Name("__migrated_ref", "load"), [], kwargs, None, None)

        return node

    return _rewrite


@ast_transform
def resolve_dbt_source_to_model_name(
    models: MappingProxyType[str, t.Union[Model, str]], env: Environment, dialect: str
) -> JinjaTransform:
    """
    Takes an expression like "{{ source('foo', 'bar') }}"
    And turns it into "foo.bar" based on the provided list of models and resolver() function

    Args:
        models: A dict of models (or model names) keyed by model fqn
        jinja_env: Should contain an implementation of {{ source() }} to turn a DBT source name / table name into a DBT relation object

    Returns:
        A string containing the table fqn of the external table referenced by the DBT "{{ source() }}" call
    """
    source: t.Callable = env.globals["source"]  # type: ignore

    def _resolve(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        if isinstance(node, j.Call) and isinstance(parent, (j.TemplateData, j.Output)):
            if (
                len(node.args) == 2
                and isinstance(node.args[0], j.Const)
                and isinstance(node.args[1], j.Const)
            ):
                source_name = node.args[0].value
                table_name = node.args[1].value
                if relation := source(source_name, table_name):
                    if not isinstance(relation, BaseRelation):
                        raise ValueError(
                            f"source() returned non-relation type for '{source_name}.{table_name}': {relation}"
                        )
                    if model_name := _dbt_relation_to_model_name(models, relation, dialect):
                        return j.TemplateData(model_name)
                    return j.TemplateData(relation.render())
                # source() didnt resolve anything, just pass through the arguments verbatim
                return j.TemplateData(f"{source_name}.{table_name}")

        return node

    return _make_standalone_call_transform("source", _resolve)


@ast_transform
def rewrite_dbt_source_to_migrated_source(
    models: MappingProxyType[str, t.Union[Model, str]], env: Environment, dialect: str
) -> JinjaTransform:
    """
    Takes an expression like "{{ source('foo', 'bar') }}"
    And turns it into "{{ __migrated_source(database='foo', identifier='bar') }}"
    so that the SQLMesh Native loader can construct a Relation instance without needing the Context

    Args:
        models: A dict of models (or model names) keyed by model fqn
        jinja_env: Should contain an implementation of {{ source() }} to turn a DBT source name / table name into a DBT relation object

    Returns:
        A new Call node with enough data to reconstruct the Relation
    """

    source: t.Callable = env.globals["source"]  # type: ignore

    def _rewrite(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        if (
            isinstance(node, j.Call)
            and isinstance(node.node, j.Name)
            and node.node.name == "source"
        ):
            if (
                len(node.args) == 2
                and isinstance(node.args[0], j.Const)
                and isinstance(node.args[1], j.Const)
            ):
                source_name = node.args[0].value
                table_name = node.args[1].value
                if (relation := source(source_name, table_name)) and isinstance(
                    relation, BaseRelation
                ):
                    kwargs = _dbt_relation_to_kwargs(relation)
                    return j.Call(j.Name("__migrated_source", "load"), [], kwargs, None, None)

        return node

    return _rewrite


@ast_transform
def resolve_dbt_this_to_model_name(model_name: str) -> JinjaTransform:
    """
    Takes an expression like "{{ this }}" and turns it into the provided "model_name" string
    """
    return _make_single_expression_transform({"this": model_name})


@ast_transform
def deduplicate_incremental_checks() -> JinjaTransform:
    """
    Some files may have been designed to run with both the SQLMesh DBT loader and DBT itself and contain sections like:

    ---
    select * from foo
    where
        {% if is_incremental() %}ds > (select max(ds)) from {{ this }}{% endif %}
        {% if sqlmesh_incremental is defined %}ds BETWEEN {{ start_ds }} and {{ end_ds }}{% endif %}
    ---

    This is transform detects usages of {% if sqlmesh_incremental ... %}
    If it finds them, it:
        - removes occurances of {% if is_incremental() %} in favour of the {% if sqlmesh_incremental %} check

    If no instances of {% if sqlmesh_incremental %} are found, nothing changes

    For for example, the above will be transformed into:
    ---
    select * from foo
    where
        ds BETWEEN {{ start_ds }} and {{ end_ds }}
    ---

    But if it didnt contain the {% if sqlmesh_incremental %} block, this transform would output:
    ---
    select * from foo
    where
        {% if is_incremental() %}ds > (select max(ds)) from {{ this }}){% endif %}
    ---

    """
    has_sqlmesh_incremental = False

    def _handle(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        nonlocal has_sqlmesh_incremental

        if isinstance(node, j.Template):
            for if_node in node.find_all(j.If):
                if test_name := if_node.test.find(j.Name):
                    if test_name.name == "sqlmesh_incremental":
                        has_sqlmesh_incremental = True

        # only remove the {% if is_incremental() %} checks in the present of {% sqlmesh_incremental is defined %} checks
        if has_sqlmesh_incremental:
            if isinstance(node, j.If) and node.test:
                if test_name := node.test.find(j.Name):
                    if test_name.name == "is_incremental":
                        return None

        return node

    return _handle


@ast_transform
def unpack_incremental_checks() -> JinjaTransform:
    """
    This takes queries like:

    > select * from foo where {% if sqlmesh_incremental is defined %}ds BETWEEN {{ start_ds }} and {{ end_ds }}{% endif %}
    > select * from foo where {% if is_incremental() %}ds > (select max(ds)) from foo.table){% endif %}

    And, if possible, removes the {% if sqlmesh_incremental is defined %} / {% is_incremental %} block to achieve:

    > select * from foo where ds BETWEEN {{ start_ds }} and {{ end_ds }}
    > select * from foo where ds > (select max(ds)) from foo.table)

    Note that if there is a {% else %} portion to the block, there is no SQLMesh equivalent so in that case the check is untouched.

    Also, if both may be present in a model, run the deduplicate_incremental_checks() transform first so only one gets unpacked by this transform
    """

    def _handle(node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]) -> j.Node:
        if isinstance(node, j.If) and node.test:
            if test_name := node.test.find(j.Name):
                if (
                    test_name.name in ("is_incremental", "sqlmesh_incremental")
                    and not node.elif_
                    and not node.else_
                ):
                    return j.Output(node.body)

        return node

    return _handle


@ast_transform
def rewrite_sqlmesh_predefined_variables_to_sqlmesh_macro_syntax() -> JinjaTransform:
    """
    If there are SQLMesh predefined variables in Jinja form, eg "{{ start_dt }}"
    Rewrite them to eg "@start_dt"

    Example:

    select * from foo where ds between {{ start_dt }} and {{ end_dt }}

    > select * from foo where ds between @start_dt and @end_dt
    """

    mapping = {v: f"@{v}" for v in SQLMESH_PREDEFINED_MACRO_VARIABLES}

    literal_remapping = {"dt": "ts", "date": "ds"}

    def _mapping_func(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node], name: str
    ) -> t.Optional[str]:
        wrapped_in_literal = False
        if prev and isinstance(prev, j.TemplateData):
            data = prev.data.strip()
            if data.endswith("'"):
                wrapped_in_literal = True

        if wrapped_in_literal:
            for original, new in literal_remapping.items():
                if name.endswith(original):
                    name = name.removesuffix(original) + new

        return mapping.get(name)

    return _make_single_expression_transform(_mapping_func)


@ast_transform
def append_dbt_package_kwarg_to_var_calls(package_name: t.Optional[str]) -> JinjaTransform:
    """ "
    If there are calls like:

    > {% if 'col_name' in var('history_columns') %}

    Assuming package_name=foo, change it to:

    > {% if 'col_name' in var('history_columns', __dbt_package="foo") %}

    The point of this is to give a hint to the "var" shim in SQLMesh Native so it knows which key
    under "__dbt_packages__" in the project variables to look for
    """

    def _append(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        if package_name and isinstance(node, j.Call):
            node.kwargs.append(j.Keyword("__dbt_package", j.Const(package_name)))
        return node

    return _make_standalone_call_transform("var", _append)


@ast_transform
def unwrap_macros_in_string_literals() -> SQLGlotTransform:
    """
    Given a query containing string literals *that match SQLMesh predefined macro variables* like:

    > select * from foo where ds between '@start_dt' and '@end_dt'

    Unwrap them into:

    > select * from foo where ds between @start_dt and @end_dt
    """
    values_to_check = {f"@{var}": var for var in SQLMESH_PREDEFINED_MACRO_VARIABLES}

    def _transform(e: exp.Expression) -> exp.Expression:
        if isinstance(e, exp.Literal) and e.is_string:
            if (value := e.text("this")) and value in values_to_check:
                return d.MacroVar(
                    this=values_to_check[value]
                )  # MacroVar adds in the @ so dont want to add it twice
        return e

    return _transform
