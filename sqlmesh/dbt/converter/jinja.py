import typing as t
import jinja2.nodes as j
import sqlmesh.core.dialect as d
from sqlmesh.core.context import Context
from sqlmesh.core.snapshot import Node
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlglot import exp
from sqlmesh.dbt.converter.common import JinjaTransform
from inspect import signature
from more_itertools import windowed
from itertools import chain
from sqlmesh.dbt.context import DbtContext
import sqlmesh.dbt.converter.jinja_transforms as jt
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import SQLMESH_DBT_COMPATIBILITY_PACKAGE

# for j.Operand.op
OPERATOR_MAP = {
    "eq": "==",
    "ne": "!=",
    "lt": "<",
    "gt": ">",
    "lteq": "<=",
    "gteq": ">=",
    "in": "in",
    "notin": "not in",
}


def lpad_windowed(iterable: t.Iterable[j.Node]) -> t.Iterator[t.Tuple[t.Optional[j.Node], j.Node]]:
    for prev, curr in windowed(chain([None], iterable), 2):
        if curr is None:
            raise ValueError("Current item cannot be None")
        yield prev, curr


class JinjaGenerator:
    def generate(
        self, node: j.Node, prev: t.Optional[j.Node] = None, parent: t.Optional[j.Node] = None
    ) -> str:
        if not isinstance(node, j.Node):
            raise ValueError(f"Generator only works with Jinja AST nodes, not: {type(node)}")

        acc = ""

        node_type = type(node)
        generator_fn_name = f"_generate_{node_type.__name__.lower()}"

        if generator_fn := getattr(self, generator_fn_name, None):
            sig = signature(generator_fn)
            kwargs: t.Dict[str, t.Optional[j.Node]] = {"node": node}
            if "prev" in sig.parameters:
                kwargs["prev"] = prev
            if "parent" in sig.parameters:
                kwargs["parent"] = parent
            acc += generator_fn(**kwargs)
        else:
            raise NotImplementedError(f"Generator for node type '{type(node)}' is not implemented")

        return acc

    def _generate_template(self, node: j.Template) -> str:
        acc = []
        for prev, curr in lpad_windowed(node.body):
            if curr:
                acc.append(self.generate(curr, prev, node))

        return "".join(acc)

    def _generate_output(self, node: j.Output) -> str:
        acc = []
        for prev, curr in lpad_windowed(node.nodes):
            acc.append(self.generate(curr, prev, node))

        return "".join(acc)

    def _generate_templatedata(self, node: j.TemplateData) -> str:
        return node.data

    def _generate_name(
        self, node: j.Name, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        return self._wrap_in_expression_if_necessary(node.name, prev, parent)

    def _generate_getitem(
        self, node: j.Getitem, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        item_name = self.generate(node.node, parent=node)
        if node.arg:
            if node.node.find(j.Filter):
                # for when someone has {{ (foo | bar | baz)[0] }}
                item_name = f"({item_name})"
            item_name = f"{item_name}[{self.generate(node.arg, parent=node)}]"

        return self._wrap_in_expression_if_necessary(item_name, prev, parent)

    def _generate_getattr(
        self, node: j.Getattr, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        what_str = self.generate(node.node, parent=node)

        return self._wrap_in_expression_if_necessary(f"{what_str}.{node.attr}", prev, parent)

    def _generate_const(
        self, node: j.Const, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        quotechar = ""
        node_value: str
        if isinstance(node.value, str):
            quotechar = "'" if "'" not in node.value else '"'
            node_value = node.value
        else:
            node_value = str(node.value)

        const_value = quotechar + node_value + quotechar

        return self._wrap_in_expression_if_necessary(const_value, prev, parent)

    def _generate_keyword(self, node: j.Keyword) -> str:
        return node.key + "=" + self.generate(node.value, parent=node)

    def _generate_test(self, node: j.Test, parent: t.Optional[j.Node]) -> str:
        var_name = self.generate(node.node, parent=node)
        test = "is" if not isinstance(parent, j.Not) else "is not"
        if node.name:
            return f"{var_name} {test} {node.name}"
        return var_name

    def _generate_assign(self, node: j.Assign) -> str:
        target_str = self.generate(node.target, parent=node)
        what_str = self.generate(node.node, parent=node)
        return "{% set " + target_str + " = " + what_str + " %}"

    def _generate_assignblock(self, node: j.AssignBlock) -> str:
        target_str = self.generate(node.target, parent=node)
        body_str = "".join(self.generate(c, parent=node) for c in node.body)
        # todo: node.filter?
        return "{% set " + target_str + " %}" + body_str + "{% endset %}"

    def _generate_call(
        self, node: j.Call, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        call_name = self.generate(node.node, parent=node)
        call_args = ", ".join(self.generate(a, parent=node) for a in node.args)
        call_kwargs = ", ".join(self.generate(a, parent=node) for a in node.kwargs)
        sep = ", " if call_args and call_kwargs else ""
        call_str = call_name + f"({call_args}{sep}{call_kwargs})"

        return self._wrap_in_expression_if_necessary(call_str, prev, parent)

    def _generate_if(self, node: j.If, parent: t.Optional[j.Node]) -> str:
        test_str = self.generate(node.test, parent=node)
        body_str = "".join(self.generate(c, parent=node) for c in node.body)
        elifs_str = "".join(self.generate(c, parent=node) for c in node.elif_)
        elses_str = "".join(self.generate(c, parent=node) for c in node.else_)

        end_block_name: t.Optional[str]
        block_name, end_block_name = "if", "endif"
        if isinstance(parent, j.If):
            if node in parent.elif_:
                block_name, end_block_name = "elif", None

        end_block = "{% " + end_block_name + " %}" if end_block_name else ""

        elses_str = "{% else %}" + elses_str if elses_str else ""

        return (
            "{% "
            + block_name
            + " "
            + test_str
            + " %}"
            + body_str
            + elifs_str
            + elses_str
            + end_block
        )

    def _generate_macro(self, node: j.Macro, prev: t.Optional[j.Node]) -> str:
        name_str = node.name
        rendered_defaults = list(reversed([self.generate(d, parent=node) for d in node.defaults]))
        rendered_args = [self.generate(a, parent=node) for a in node.args]

        # the defaults, if they exist, line up with the last arguments in the list
        # so we reverse the lists to match the arrays and then reverse the result to get the original order
        args_with_defaults = [
            (arg, next(iter(rendered_defaults[idx : idx + 1]), None))
            for idx, arg in enumerate(reversed(rendered_args))
        ]
        args_with_defaults = list(reversed(args_with_defaults))

        args_str = ", ".join(f"{a}={d}" if d is not None else a for a, d in args_with_defaults)
        body_str = "".join(self.generate(c, parent=node) for c in node.body)

        # crude sql comment detection that will cause false positives that hopefully shouldnt matter
        # this is to work around a WONTFIX bug in the SQLGlot tokenizer that if the macro body contains a SQL comment
        # and {% endmacro %} is on the same line, it gets included as comment instead of a proper token
        # the bug also occurs if the {% macro %} tag is on a line that starts with a SQL comment
        start_tag = "{% macro "
        if prev:
            prev_str = self.generate(prev)
            if "--" in prev_str and not prev_str.rstrip(" ").endswith("\n"):
                start_tag = "\n" + start_tag

        end_tag = "{% endmacro %}"
        if "--" in body_str and not body_str.rstrip(" ").endswith("\n"):
            end_tag = "\n" + end_tag

        return start_tag + name_str + "(" + args_str + ")" + " %}" + body_str + end_tag

    def _generate_for(self, node: j.For) -> str:
        target_str = self.generate(node.target, parent=node)
        iter_str = self.generate(node.iter, parent=node)
        test_str = "if " + self.generate(node.test, parent=node) if node.test else None
        body_str = "".join(self.generate(c, parent=node) for c in node.body)

        acc = "{% for " + target_str + " in " + iter_str
        if test_str:
            acc += f" {test_str}"
        acc += " %}"
        acc += body_str
        acc += "{% endfor %}"

        return acc

    def _generate_list(self, node: j.List, parent: t.Optional[j.Node]) -> str:
        items_str_array = [self.generate(i, parent=node) for i in node.items]
        items_on_newline = (
            not isinstance(parent, j.Pair)
            and len(items_str_array) > 1
            and any(len(i) > 50 for i in items_str_array)
        )
        item_separator = "\n\t" if items_on_newline else " "
        items_str = f",{item_separator}".join(items_str_array)
        start_separator = "\n\t" if items_on_newline else ""
        end_separator = "\n" if items_on_newline else ""
        return f"[{start_separator}{items_str}{end_separator}]"

    def _generate_dict(self, node: j.Dict) -> str:
        items_str = ", ".join(self.generate(c, parent=node) for c in node.items)
        return "{ " + items_str + " }"

    def _generate_pair(self, node: j.Pair) -> str:
        key_str = self.generate(node.key, parent=node)
        value_str = self.generate(node.value, parent=node)
        return f"{key_str}: {value_str}"

    def _generate_not(self, node: j.Not) -> str:
        if isinstance(node.node, j.Test):
            return self.generate(node.node, parent=node)

        return self.__generate_unaryexp(node)

    def _generate_neg(self, node: j.Neg) -> str:
        return self.__generate_unaryexp(node)

    def _generate_pos(self, node: j.Pos) -> str:
        return self.__generate_unaryexp(node)

    def _generate_compare(self, node: j.Compare) -> str:
        what_str = self.generate(node.expr, parent=node)

        # todo: is this correct? need to test with multiple ops
        ops_str = "".join(self.generate(o, parent=node) for o in node.ops)

        return f"{what_str} {ops_str}"

    def _generate_slice(self, node: j.Slice) -> str:
        start_str = self.generate(node.start, parent=node) if node.start else ""
        stop_str = self.generate(node.stop, parent=node) if node.stop else ""
        # todo: need a syntax example of step
        return f"{start_str}:{stop_str}"

    def _generate_operand(self, node: j.Operand) -> str:
        assert isinstance(node, j.Operand)
        value_str = self.generate(node.expr, parent=node)

        return f"{OPERATOR_MAP[node.op]} " + value_str

    def _generate_add(self, node: j.Add, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_mul(self, node: j.Mul, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_div(self, node: j.Div, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_sub(self, node: j.Sub, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_floordiv(self, node: j.FloorDiv, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_mod(self, node: j.Mod, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_pow(self, node: j.Pow, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_or(self, node: j.Or, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_and(self, node: j.And, parent: t.Optional[j.Node]) -> str:
        return self.__generate_binexp(node, parent)

    def _generate_concat(self, node: j.Concat) -> str:
        return " ~ ".join(self.generate(c, parent=node) for c in node.nodes)

    def _generate_tuple(self, node: j.Tuple, parent: t.Optional[j.Node]) -> str:
        parenthesis = isinstance(parent, (j.Operand, j.Call))
        items_str = ", ".join(self.generate(i, parent=node) for i in node.items)
        return items_str if not parenthesis else f"({items_str})"

    def _generate_filter(
        self, node: j.Filter, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        # node.node may be None if this Filter is part of a FilterBlock
        what_str = self.generate(node.node, parent=node) if node.node else None
        if isinstance(node.node, j.CondExpr):
            what_str = f"({what_str})"

        args_str = ", ".join(self.generate(a, parent=node) for a in node.args + node.kwargs)
        if args_str:
            args_str = f"({args_str})"

        filter_expr = f"{node.name}{args_str}"
        if what_str:
            filter_expr = f"{what_str} | {filter_expr}"

        return self._wrap_in_expression_if_necessary(filter_expr, prev=prev, parent=parent)

    def _generate_filterblock(self, node: j.FilterBlock) -> str:
        filter_str = self.generate(node.filter, parent=node)
        body_str = "".join(self.generate(c, parent=node) for c in node.body)
        return "{% filter " + filter_str + " %}" + body_str + "{% endfilter %}"

    def _generate_exprstmt(self, node: j.ExprStmt) -> str:
        node_str = self.generate(node.node, parent=node)
        return "{% do " + node_str + " %}"

    def _generate_condexpr(
        self, node: j.CondExpr, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        test_sql = self.generate(node.test, parent=node)
        expr1_sql = self.generate(node.expr1, parent=node)

        if node.expr2 is None:
            raise ValueError("CondExpr lacked an 'else', not sure how to handle this")

        expr2_sql = self.generate(node.expr2, parent=node)
        return self._wrap_in_expression_if_necessary(
            f"{expr1_sql} if {test_sql} else {expr2_sql}", prev, parent
        )

    def __generate_binexp(self, node: j.BinExpr, parent: t.Optional[j.Node]) -> str:
        left_str = self.generate(node.left, parent=node)
        right_str = self.generate(node.right, parent=node)

        wrap_left = isinstance(node.left, j.BinExpr)
        wrap_right = isinstance(node.right, j.BinExpr)

        acc = f"({left_str})" if wrap_left else left_str
        acc += f" {node.operator} "
        acc += f"({right_str})" if wrap_right else right_str

        return acc

    def __generate_unaryexp(self, node: j.UnaryExpr) -> str:
        body_str = self.generate(node.node, parent=node)
        return f"{node.operator} {body_str}"

    def _generate_nsref(self, node: j.NSRef) -> str:
        return f"{node.name}.{node.attr}"

    def _generate_callblock(self, node: j.CallBlock) -> str:
        call = self.generate(node.call, parent=node)
        body = "".join(self.generate(e, parent=node) for e in node.body)
        args = ", ".join(self.generate(arg, parent=node) for arg in node.args)

        open_tag = "{% call"

        if args:
            open_tag += "(" + args + ")"

        if len(node.defaults) > 0:
            raise NotImplementedError("Not sure how to handle CallBlock.defaults")

        return open_tag + " " + call + " %}" + body + "{% endcall %}"

    def _wrap_in_expression_if_necessary(
        self, string: str, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> str:
        wrap = False
        if isinstance(prev, j.TemplateData):
            wrap = True
        elif prev is None and isinstance(parent, j.Output):
            wrap = True
        elif parent:
            # if the node is nested inside eg an {% if %} block, dont wrap it in {{ }}
            wrap = not any(isinstance(parent, t) for t in (j.Operand, j.Stmt, j.Expr, j.Helper))

        return "{{ " + string + " }}" if wrap else string


def _contains_jinja(query: str) -> bool:
    if "{{" in query:
        return True
    if "{%" in query:
        return True
    return False


def transform(base: j.Node, handler: JinjaTransform) -> j.Node:
    sig = signature(handler)

    def _build_handler_kwargs(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Dict[str, t.Any]:
        kwargs: t.Dict[str, t.Optional[j.Node]] = {"node": node}
        if "prev" in sig.parameters:
            kwargs["prev"] = prev
        if "parent" in sig.parameters:
            kwargs["parent"] = parent
        return kwargs

    def _transform(
        node: j.Node, prev: t.Optional[j.Node], parent: t.Optional[j.Node]
    ) -> t.Optional[j.Node]:
        transformed_node: t.Optional[j.Node] = handler(**_build_handler_kwargs(node, prev, parent))  # type: ignore

        if not transformed_node:
            return None

        node = transformed_node

        new_children: t.Dict[j.Node, t.Optional[j.Node]] = {}
        prev = None
        for child in list(node.iter_child_nodes()):
            transformed_child = _transform(node=child, prev=prev, parent=node)
            if transformed_child != child:
                new_children[child] = transformed_child
            prev = child

        if new_children:
            replacement_fields: t.Dict[str, t.Union[j.Node, t.List[j.Node]]] = {}
            for name, value in node.iter_fields():
                assert isinstance(name, str)

                if isinstance(value, list):
                    replacement_value_list = [new_children.get(i, i) for i in value]
                    replacement_fields[name] = [r for r in replacement_value_list if r is not None]
                elif isinstance(value, j.Node):
                    replacement_value = new_children.get(value) or value
                    replacement_fields[name] = replacement_value
            for name, value in replacement_fields.items():
                setattr(node, name, value)

        return node

    transformed = _transform(node=base, prev=None, parent=None)
    if transformed is None:
        raise ValueError(
            f"Transform '{handler.__name__}' consumed the entire AST; this indicates a bug"
        )
    return transformed


def convert_jinja_query(
    context: Context,
    node: Node,
    query: d.Jinja,
    package: t.Optional[str] = None,
    exclude: t.Optional[t.List[t.Callable]] = None,
) -> t.Union[d.JinjaQuery, d.JinjaStatement, exp.Query, exp.DDL]:
    jinja_env = node.jinja_macros.build_environment()

    ast: j.Node = jinja_env.parse(query.text("this"))  # type: ignore

    transforms = [
        # transform {{ ref("foo") }} -> schema.foo (NOT "fully_qualified"."schema"."foo")
        jt.resolve_dbt_ref_to_model_name(context.models, jinja_env, node.dialect),
        # Rewrite ref() calls that cant be converted to strings (maybe theyre macro aguments) to __migrated_ref() calls
        jt.rewrite_dbt_ref_to_migrated_ref(context.models, jinja_env, node.dialect),
        # transform {{ source("upstream"."foo") }} -> upstream.foo (NOT "fully_qualified"."upstream"."foo")
        jt.resolve_dbt_source_to_model_name(context.models, jinja_env, node.dialect),
        # Rewrite source() calls that cant be converted to strings (maybe theyre macro aguments) to __migrated_source() calls
        jt.rewrite_dbt_source_to_migrated_source(context.models, jinja_env, node.dialect),
        # transform {{ this }} -> model.name
        jt.resolve_dbt_this_to_model_name(node.name),
        # deuplicate where both {% if sqlmesh_incremental %} and {% if is_incremental() %} are used
        jt.deduplicate_incremental_checks(),
        # unpack {% if is_incremental() %} blocks because they arent necessary when running a native project
        jt.unpack_incremental_checks(),
    ]

    if package:
        transforms.append(jt.append_dbt_package_kwarg_to_var_calls(package))

    transforms = [
        t for t in transforms if not any(e.__name__ in t.__name__ for e in (exclude or []))
    ]

    for handler in transforms:
        ast = transform(ast, handler)

    generator = JinjaGenerator()
    pre_post_processing = generator.generate(ast)
    if isinstance(node, SqlModel) and isinstance(query, d.JinjaQuery) and not node.depends_on_self:
        # is it self-referencing now is_incremental() has been removed?
        # if so, and columns_to_types are not all known, then we can't remove is_incremental() or we will get a load error

        # try to load the converted model with the native loader
        model_definition = node.copy(update=dict(audits=[])).render_definition()[0].sql()

        # we need the Jinja builtins that inclide the compatibility shims because the transforms may have created eg __migrated_ref() calls
        jinja_macros = node.jinja_macros.copy(
            update=dict(create_builtins_module=SQLMESH_DBT_COMPATIBILITY_PACKAGE)
        )

        converted_node = load_sql_based_model(
            expressions=[d.parse_one(model_definition), d.JinjaQuery(this=pre_post_processing)],
            jinja_macros=jinja_macros,
            defaults=context.config.model_defaults.dict(),
            default_catalog=node.default_catalog,
        )
        original_model = context.models[node.fqn]

        if converted_node.depends_on_self:
            try:
                # we need to upsert the model into the context to trigger columns_to_types inference
                # note that this can sometimes bust the optimized query cache which can lead to long pauses converting some models in large projects
                context.upsert_model(converted_node)
            except ConfigError as e:
                if "Self-referencing models require inferrable column types" in str(e):
                    # we have a self-referencing model where the columns_to_types cannot be inferred
                    # run the conversion again without the unpack_incremental_checks transform
                    return convert_jinja_query(
                        context, node, query, exclude=[jt.unpack_incremental_checks]
                    )
                raise
            except Exception:
                # todo: perhaps swallow this so that we just continue on with the original logic
                raise
            finally:
                context.upsert_model(original_model)  # put the original model definition back

    ast = transform(ast, jt.rewrite_sqlmesh_predefined_variables_to_sqlmesh_macro_syntax())
    post_processed = generator.generate(ast)

    # post processing - have we removed all the jinja so this can effectively be a normal SQL query?
    if not _contains_jinja(post_processed):
        parsed = d.parse_one(post_processed, dialect=node.dialect)

        # converting DBT '{{ start_ds }}' to a SQLMesh macro results in single quoted '@start_ds' but we really need unquoted @start_ds
        transformed = parsed.transform(jt.unwrap_macros_in_string_literals())
        if isinstance(transformed, (exp.Query, exp.DDL)):
            return transformed

        raise ValueError(
            f"Transformation resulted in a {type(transformed)} node instead of Query / DDL statement"
        )

    if isinstance(query, d.JinjaQuery):
        return d.JinjaQuery(this=pre_post_processing)
    if isinstance(query, d.JinjaStatement):
        return d.JinjaStatement(this=pre_post_processing)

    raise ValueError(f"Not sure how to handle: {type(query)}")


def convert_jinja_macro(context: Context, src: str, package: t.Optional[str] = None) -> str:
    jinja_macros = DbtContext().jinja_macros  # ensures the correct create_builtins_module is set
    jinja_macros = jinja_macros.merge(context._jinja_macros)

    jinja_env = jinja_macros.build_environment()

    dialect = context.default_dialect
    if not dialect:
        raise ValueError("No project dialect configured?")

    transforms = [
        # transform {{ ref("foo") }} -> schema.foo (NOT "fully_qualified"."schema"."foo")
        jt.resolve_dbt_ref_to_model_name(context.models, jinja_env, dialect),
        # Rewrite ref() calls that cant be converted to strings (maybe theyre macro aguments) to __migrated_ref() calls
        jt.rewrite_dbt_ref_to_migrated_ref(context.models, jinja_env, dialect),
        # transform {{ source("foo", "bar") }} -> `qualified`.`foo`.`bar`
        jt.resolve_dbt_source_to_model_name(context.models, jinja_env, dialect),
        # transform {{ var('foo') }} -> {{ var('foo', __dbt_package='<package>') }}
        jt.append_dbt_package_kwarg_to_var_calls(package),
        # deduplicate where both {% if sqlmesh_incremental %} and {% if is_incremental() %} are used
        jt.deduplicate_incremental_checks(),
        # unpack {% if sqlmesh_incremental %} blocks because they arent necessary when running a native project
        jt.unpack_incremental_checks(),
    ]

    ast: j.Node = jinja_env.parse(src)

    for handler in transforms:
        ast = transform(ast, handler)

    generator = JinjaGenerator()

    return generator.generate(ast)
