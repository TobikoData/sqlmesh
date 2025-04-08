"""
Warn if there is a metadata macro / signal that references another global.

The metadata status for macros and signals is now transitive, i.e. every dependency of a
metadata macro or signal is also metadata, unless it is referenced by a non-metadata object.

This means that global references of metadata objects may now be excluded from the
data hash calculation because of their new metadata status, which would lead to a
diff. This script detects the possibility for such a diff and warns users ahead of time.
"""

import ast
import dis
import json
import textwrap
import types
import typing as t
import zlib

from dataclasses import dataclass
from enum import Enum
from jinja2 import Environment, nodes
from sqlglot import exp
from sqlglot.optimizer.simplify import gen

import sqlmesh.core.dialect as d
from sqlmesh.core.console import get_console
from sqlmesh.utils import unique
from sqlmesh.utils.pydantic import PydanticModel

GATEWAY = "gateway"
VAR = "var"

PROPERTIES = {"physical_properties", "session_properties", "virtual_properties"}


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    for (
        name,
        identifier,
        version,
        snapshot,
        kind_name,
        updated_ts,
        unpaused_ts,
        ttl_ms,
        unrestorable,
    ) in engine_adapter.fetchall(
        exp.select(
            "name",
            "identifier",
            "version",
            "snapshot",
            "kind_name",
            "updated_ts",
            "unpaused_ts",
            "ttl_ms",
            "unrestorable",
        ).from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]
        python_env = node.get("python_env")

        # This try-catch is a conservative measure; the following code doesn't affect the state
        try:
            if python_env and isinstance(python_env, dict):
                dialect = node.get("dialect")

                statements = []
                if pre_statements := node.get("pre_statements"):
                    statements.extend(parse_expression(pre_statements, dialect))
                if query := node.get("query"):
                    statements.append(parse_expression(query, dialect))
                if post_statements := node.get("post_statements"):
                    statements.extend(parse_expression(post_statements, dialect))
                if on_virtual_update := node.get("on_virtual_update"):
                    statements.extend(parse_expression(on_virtual_update, dialect))

                for property_name in PROPERTIES:
                    if property_value := node.get(property_name):
                        properties = parse_properties(property_value, dialect)
                        if isinstance(properties, exp.Tuple):
                            statements.extend(properties.expressions)

                jinja_macro_references, _ = extract_macro_references_and_variables(
                    *(gen(e) for e in statements)
                )

                if audit_definitions := node.get("audit_definitions"):
                    audit_queries = [
                        parse_expression(audit["query"], audit["dialect"])
                        for audit in audit_definitions.values()
                    ]
                    statements.extend(audit_queries)

                for _, audit_args in func_call_validator(node.get("audits") or []):
                    statements.extend(audit_args.values())

                metadata_func_candidates = set()

                for signal_name, signal_args in func_call_validator(
                    node.get("signals") or [], is_signal=True
                ):
                    metadata_func_candidates.add(signal_name)
                    statements.extend(signal_args.values())

                metadata_func_candidates |= extract_used_macros(statements, jinja_macro_references)
                if metadata_func_candidates and any(
                    c in python_env for c in metadata_func_candidates
                ):
                    serialized_python_env = {k: Executable(**v) for k, v in python_env.items()}
                    hydrated_python_env = prepare_env(serialized_python_env)

                    for func_name in metadata_func_candidates:
                        func = hydrated_python_env.get(func_name)
                        if not func:
                            continue

                        executable = serialized_python_env[func_name]
                        if executable.is_metadata and any(
                            global_ref in hydrated_python_env
                            for global_ref in func_globals(func, executable.payload)
                        ):
                            get_console().log_warning(
                                f"Node '{node['name']}' references a metadata-only function (macro or signal), "
                                "which in turn relies on non-metadata objects. This means that the next "
                                "plan command may detect unexpected changes and prompt about backfilling "
                                "this model. If this is a concern, consider running a forward-only plan "
                                "instead: https://sqlmesh.readthedocs.io/en/stable/concepts/plans/#forward-only-plans.\n"
                            )
                            break
        except Exception:
            pass


def environment(**kwargs):
    extensions = kwargs.pop("extensions", [])
    extensions.append("jinja2.ext.do")
    extensions.append("jinja2.ext.loopcontrols")
    return Environment(extensions=extensions, **kwargs)


ENVIRONMENT = environment()


def extract_used_macros(expressions, jinja_macro_references):
    used_macros = set()
    for expression in expressions:
        if isinstance(expression, d.Jinja):
            continue
        for macro_func in expression.find_all(d.MacroFunc):
            if macro_func.__class__ is d.MacroFunc:
                used_macros.add(macro_func.this.name.lower())

    for macro_ref in jinja_macro_references or set():
        if macro_ref.package is None:
            used_macros.add(macro_ref.name)

    return used_macros


def extract_macro_references_and_variables(*jinja_strs):
    macro_references = set()
    variables = set()
    for jinja_str in jinja_strs:
        for call_name, node in extract_call_names(jinja_str):
            if call_name[0] == VAR:
                assert isinstance(node, nodes.Call)
                args = [jinja_call_arg_name(arg) for arg in node.args]
                if args and args[0]:
                    variables.add(args[0].lower())
            elif call_name[0] == GATEWAY:
                variables.add(GATEWAY)
            elif len(call_name) == 1:
                macro_references.add(MacroReference(name=call_name[0]))
            elif len(call_name) == 2:
                macro_references.add(MacroReference(package=call_name[0], name=call_name[1]))
    return macro_references, variables


def extract_call_names(jinja_str, cache=None):
    def parse():
        return list(find_call_names(ENVIRONMENT.parse(jinja_str), set()))

    if cache is not None:
        key = str(zlib.crc32(jinja_str.encode("utf-8")))
        if key in cache:
            names = cache[key][0]
        else:
            names = parse()
        cache[key] = (names, True)
        return names
    return parse()


def find_call_names(node, vars_in_scope):
    vars_in_scope = vars_in_scope.copy()
    for child_node in node.iter_child_nodes():
        if "target" in child_node.fields:
            target = getattr(child_node, "target")
            if isinstance(target, nodes.Name):
                vars_in_scope.add(target.name)
            elif isinstance(target, nodes.Tuple):
                for item in target.items:
                    if isinstance(item, nodes.Name):
                        vars_in_scope.add(item.name)
        elif isinstance(child_node, nodes.Macro):
            for arg in child_node.args:
                vars_in_scope.add(arg.name)
        elif isinstance(child_node, nodes.Call) or (
            isinstance(child_node, nodes.Getattr) and not isinstance(child_node.node, nodes.Getattr)
        ):
            name = call_name(child_node)
            if name[0][0] != "'" and name[0] not in vars_in_scope:
                yield (name, child_node)
        yield from find_call_names(child_node, vars_in_scope)


def call_name(node):
    if isinstance(node, nodes.Name):
        return (node.name,)
    if isinstance(node, nodes.Const):
        return (f"'{node.value}'",)
    if isinstance(node, nodes.Getattr):
        return call_name(node.node) + (node.attr,)
    if isinstance(node, (nodes.Getitem, nodes.Call)):
        return call_name(node.node)
    return ()


def jinja_call_arg_name(node):
    if isinstance(node, nodes.Const):
        return node.value
    return ""


class MacroReference(PydanticModel, frozen=True):
    package: t.Optional[str] = None
    name: str

    @property
    def reference(self) -> str:
        if self.package is None:
            return self.name
        return ".".join((self.package, self.name))

    def __str__(self) -> str:
        return self.reference


@dataclass
class SqlValue:
    sql: str


class ExecutableKind(str, Enum):
    IMPORT = "import"
    VALUE = "value"
    DEFINITION = "definition"

    def __lt__(self, other):
        if not isinstance(other, ExecutableKind):
            return NotImplemented
        values = list(ExecutableKind.__dict__.values())
        return values.index(self) < values.index(other)

    def __str__(self):
        return self.value


class Executable(PydanticModel):
    payload: str
    kind: ExecutableKind = ExecutableKind.DEFINITION
    name: t.Optional[str] = None
    path: t.Optional[str] = None
    alias: t.Optional[str] = None
    is_metadata: t.Optional[bool] = None

    @property
    def is_definition(self):
        return self.kind == ExecutableKind.DEFINITION

    @property
    def is_import(self):
        return self.kind == ExecutableKind.IMPORT

    @property
    def is_value(self):
        return self.kind == ExecutableKind.VALUE

    @classmethod
    def value(cls, v, is_metadata):
        return Executable(payload=repr(v), kind=ExecutableKind.VALUE, is_metadata=is_metadata)


def prepare_env(python_env, env=None):
    env = {} if env is None else env

    for name, executable in sorted(
        python_env.items(), key=lambda item: 0 if item[1].is_import else 1
    ):
        if executable.is_value:
            env[name] = eval(executable.payload)
        else:
            exec(executable.payload, env)
            if executable.alias and executable.name:
                env[executable.alias] = env[executable.name]

    return env


def code_globals(code):
    variables = {
        instruction.argval: None
        for instruction in dis.get_instructions(code)
        if instruction.opname == "LOAD_GLOBAL"
    }

    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            variables.update(code_globals(const))

    return variables


def func_globals(func, func_payload):
    root_node = ast.parse(textwrap.dedent(func_payload))
    if not root_node:
        return set()

    func_args = next(node for node in ast.walk(root_node) if isinstance(node, ast.arguments))
    arg_defaults = (d for d in func_args.defaults + func_args.kw_defaults if d is not None)

    arg_globals = [
        n.id for default in arg_defaults for n in ast.walk(default) if isinstance(n, ast.Name)
    ]

    variables = set()
    code = func.__code__
    for var in arg_globals + list(code_globals(code)) + decorator_vars(func, root_node=root_node):
        if var in func.__globals__:
            variables.add(var)

    if func.__closure__:
        for var, value in zip(code.co_freevars, func.__closure__):
            variables.add(var)

    return variables


def decorator_vars(func, root_node):
    root_node = root_node
    finder = DecoratorDependencyFinder()
    finder.visit(root_node)
    return unique(finder.dependencies)


IGNORE_DECORATORS = {"macro", "model", "signal"}


class DecoratorDependencyFinder(ast.NodeVisitor):
    def __init__(self):
        self.dependencies = []

    def extract_dependencies(self, node):
        for decorator in node.decorator_list:
            dependencies = []
            for n in ast.walk(decorator):
                if isinstance(n, ast.Attribute):
                    dep = n.attr
                elif isinstance(n, ast.Name):
                    dep = n.id
                else:
                    continue

                if dep in IGNORE_DECORATORS:
                    dependencies = []
                    break

                dependencies.append(dep)

            self.dependencies.extend(dependencies)

    def visit_FunctionDef(self, node):
        self.extract_dependencies(node)

    def visit_ClassDef(self, node):
        self.extract_dependencies(node)

    visit_AsyncFunctionDef = visit_FunctionDef


def func_call_validator(v, is_signal=False):
    assert isinstance(v, list)

    audits = []
    for entry in v:
        if isinstance(entry, dict):
            args = entry
            name = "" if is_signal else entry.pop("name")
        else:
            assert isinstance(entry, (tuple, list))
            name, args = entry

        parsed_audit = {
            key: d.parse_one(value) if isinstance(value, str) else value
            for key, value in args.items()
        }
        audits.append((name.lower(), parsed_audit))

    return audits


def parse_expression(v, dialect):
    if v is None:
        return None

    if isinstance(v, list):
        return [d.parse_one(e, dialect=dialect) for e in v]

    assert isinstance(v, str)
    return d.parse_one(v, dialect=dialect)


def parse_properties(v, dialect):
    if v is None:
        return v

    if isinstance(v, str):
        v = d.parse_one(v, dialect=dialect)

    if isinstance(v, (exp.Array, exp.Paren, exp.Tuple)):
        eq_expressions = [v.unnest()] if isinstance(v, exp.Paren) else v.expressions
        properties = (
            exp.Tuple(expressions=eq_expressions) if isinstance(v, (exp.Paren, exp.Array)) else v
        )
    else:
        assert isinstance(v, dict)
        expressions = [exp.Literal.string(key).eq(value) for key, value in v.items()]
        properties = exp.Tuple(expressions=expressions)

    properties.meta["dialect"] = dialect
    return properties
