import ast
import dis
import inspect
import os
import re
import sys
import textwrap
import traceback
import types
import typing as t
from enum import Enum
from pathlib import Path

from sqlmesh.utils import trim_path
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel


def _code_globals(code: types.CodeType) -> t.Dict[str, None]:
    variables = {
        instruction.argval: None
        for instruction in dis.get_instructions(code)
        if instruction.opname == "LOAD_GLOBAL"
    }

    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            variables.update(_code_globals(const))

    return variables


def func_globals(func: t.Callable) -> t.Dict[str, t.Any]:
    """Finds all global references in a function and nested functions.

    Args:
        func: The function to introspect

    Returns:
        A dictionary of all global references.
    """
    variables = {}

    if hasattr(func, "__code__"):
        for var in _code_globals(func.__code__):
            if var in func.__globals__:
                ref = func.__globals__[var]
                variables[var] = ref

    return variables


def normalize_source(obj: t.Any) -> str:
    """Rewrites an object's source with formatting and doc strings removed by using Python ast.

    Args:
        obj: The object to fetch source from and convert to a string.

    Returns:
        A string representation of the normalized function.
    """
    root_node = ast.parse(textwrap.dedent(inspect.getsource(obj)))

    for node in ast.walk(root_node):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            # remove decorators for regular functions
            node.decorator_list = [
                d
                for d in node.decorator_list
                if isinstance(d, ast.Name)
                and d.id in ("property", "staticmethod", "classmethod")
            ]

            # remove docstrings
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Str)
            ):
                node.body = body[1:]

            # remove function return type annotation
            if isinstance(node, ast.FunctionDef):
                node.returns = None
        elif isinstance(node, ast.arg):
            node.annotation = None

    if sys.version_info < (3, 9):
        import astor

        return astor.to_source(root_node).strip()
    return ast.unparse(root_node)


def build_env(obj: t.Any, *, env: t.Dict[str, t.Any], name: str, module: str) -> None:
    """Fills in env dictionary with all globals needed to execute the object.

    Recursively traverse classes and functions.

    Args:
        obj: Any python object.
        env: Dictionary to store the env.
        name: Name of the object in the env.
        module: The module to filter on. Other modules will not be walked and treated as imports.
    """

    obj_module = obj.__module__ if hasattr(obj, "__module__") else ""

    if obj_module == "builtins":
        return

    def walk(obj: t.Any) -> None:
        if inspect.isclass(obj):
            for base in obj.__bases__:
                build_env(base, env=env, name=base.__qualname__, module=module)

            for k, v in obj.__dict__.items():
                if k.startswith("__"):
                    continue
                # traverse methods in a class to find global references
                if isinstance(v, (classmethod, staticmethod)):
                    v = v.__func__
                if callable(v):
                    # if the method is a part of the object, walk it
                    # else it is a global function and we just store it
                    if v.__qualname__.startswith(obj.__qualname__):
                        walk(v)
                    else:
                        build_env(v, env=env, name=v.__name__, module=module)
        elif callable(obj):
            for k, v in func_globals(obj).items():
                build_env(v, env=env, name=k, module=module)

    if name not in env:
        env[name] = obj
        if obj_module.startswith(module):
            walk(obj)
    elif env[name] != obj:
        raise SQLMeshError(
            f"Cannot store {obj} in environment, duplicate definitions found for '{name}'"
        )


class ExecutableKind(str, Enum):
    """The kind of of executable."""

    DEFINITION = "definition"
    IMPORT = "import"
    VALUE = "value"


class Executable(PydanticModel):
    payload: t.Any
    kind: ExecutableKind = ExecutableKind.DEFINITION
    name: t.Optional[str] = None
    path: t.Optional[str] = None

    @property
    def is_definition(self):
        return self.kind == ExecutableKind.DEFINITION

    @property
    def is_import(self):
        return self.kind == ExecutableKind.IMPORT

    @property
    def is_value(self):
        return self.kind == ExecutableKind.VALUE


def serialize_env(env: t.Dict[str, t.Any], module: str) -> t.Dict[str, Executable]:
    """Serializes a python function into a self contained dictionary.

    Recursively walks a function's globals to store all other references inside of env.

    Args:
        env: Dictionary to store the env.
        module: The module to filter on. Other modules will not be walked and treated as imports.
    """
    serialized = {}

    for k, v in env.items():
        if callable(v):
            name = v.__name__
            name = k if name == "<lambda>" else name

            if v.__module__.startswith(module):
                serialized[k] = Executable(
                    name=name if name != k else None,
                    payload=normalize_source(v),
                    kind=ExecutableKind.DEFINITION,
                    path=trim_path(Path(inspect.getfile(v)), module).name,
                )
            else:
                serialized[k] = Executable(
                    payload=f"from {v.__module__} import {name}",
                    kind=ExecutableKind.IMPORT,
                )
        elif inspect.ismodule(v):
            name = v.__name__
            postfix = "" if name == k else f" as {k}"
            serialized[k] = Executable(
                payload=f"import {name}{postfix}",
                kind=ExecutableKind.IMPORT,
            )
        else:
            serialized[k] = Executable(payload=v, kind=ExecutableKind.VALUE)

    return serialized


def prepare_env(
    env: t.Dict[str, t.Any],
    python_env: t.Dict[str, Executable],
) -> None:
    """Prepare a python env by hydrating and executing functions.

    The Python ENV is stored in a json serializable format.
    Functions and imports are stored as a special data class.

    Args:
        env: The dictionary to execute code in.
        python_env: The dictionary containing the serialized python environment.
    """
    for name, executable in python_env.items():
        if executable.is_value:
            env[name] = executable.payload
        else:
            exec(executable.payload, env)


def print_exception(
    exception: Exception,
    python_env: t.Dict[str, Executable],
    out=sys.stderr,
) -> None:
    """Formats exceptions that occur from evaled code.

    Stack traces generated by evaled code lose code context and are difficult to debug.
    This intercepts the default stack trace and tries to make it debuggable.

    Args:
        exception: The exception to print the stack trace for.
        python_env: The environment containing stringified python code.
    """
    tb: t.List[str] = []

    if sys.version_info < (3, 10):
        formatted_exception = traceback.format_exception(
            type(exception), exception, exception.__traceback__
        )  # type: ignore
    else:
        formatted_exception = traceback.format_exception(exception)  # type: ignore

    for error_line in formatted_exception:
        match = re.search(f'File "<string>", line (.*), in (.*)', error_line)

        if not match:
            tb.append(error_line)
            continue

        line_num = int(match.group(1))
        func = match.group(2)

        if func not in python_env:
            tb.append(error_line)
            continue

        executable = python_env[func]
        indent = error_line[: match.start()]

        error_line = f"{indent}File '{executable.path}' (or imported file), line {line_num}, in {func}"

        code = executable.payload
        formatted = []

        for i, code_line in enumerate(code.splitlines()):
            if i < line_num:
                pad = len(code_line) - len(code_line.lstrip())
                if i + 1 == line_num:
                    formatted.append(f"{code_line[:pad]}{code_line[pad:]}")
                else:
                    formatted.append(code_line)

        tb.extend(
            (
                error_line,
                textwrap.indent(
                    os.linesep.join(formatted),
                    indent + "  ",
                ),
                os.linesep,
            )
        )

    out.write(os.linesep.join(tb))
