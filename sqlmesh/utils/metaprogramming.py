from __future__ import annotations

import ast
import dis
import importlib
import inspect
import linecache
import os
import re
import sys
import textwrap
import types
import typing as t
from enum import Enum
from pathlib import Path

from astor import to_source

from sqlmesh.utils import format_exception, unique
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

IGNORE_DECORATORS = {"macro", "model"}


def _is_relative_to(path: t.Optional[Path | str], other: t.Optional[Path | str]) -> bool:
    """path.is_relative_to compatibility, was only supported >= 3.9"""
    if path is None or other is None:
        return False

    if isinstance(path, str):
        path = Path(path)
    if isinstance(other, str):
        other = Path(other)

    if "site-packages" in str(path):
        return False

    try:
        path.absolute().relative_to(other.absolute())
        return True
    except ValueError:
        return False


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
    """Finds all global references and closures in a function and nested functions.

    This function treats closures as global variables, which could cause problems in the future.

    Args:
        func: The function to introspect

    Returns:
        A dictionary of all global references.
    """
    variables = {}

    if hasattr(func, "__code__"):
        code = func.__code__

        for var in list(_code_globals(code)) + decorators(func):
            if var in func.__globals__:
                ref = func.__globals__[var]
                variables[var] = ref

        if func.__closure__:
            for var, value in zip(code.co_freevars, func.__closure__):
                variables[var] = value.cell_contents

    return variables


class ClassFoundException(Exception):
    pass


class _ClassFinder(ast.NodeVisitor):
    def __init__(self, qualname: str) -> None:
        self.stack: t.List[str] = []
        self.qualname = qualname

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node.name)
        self.stack.append("<locals>")
        self.generic_visit(node)
        self.stack.pop()
        self.stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.stack.append(node.name)
        if self.qualname == ".".join(self.stack):
            # Return the decorator for the class if present
            if node.decorator_list:
                line_number = node.decorator_list[0].lineno
            else:
                line_number = node.lineno

            # decrement by one since lines starts with indexing by zero
            line_number -= 1
            raise ClassFoundException(line_number)
        self.generic_visit(node)
        self.stack.pop()


def getsource(obj: t.Any) -> str:
    """Get the source of a function or class.

    inspect.getsource doesn't find decorators in python < 3.9
    https://github.com/python/cpython/commit/696136b993e11b37c4f34d729a0375e5ad544ade
    """
    path = inspect.getsourcefile(obj)
    if path:
        module = inspect.getmodule(obj, path)

        if module:
            lines = linecache.getlines(path, module.__dict__)
        else:
            lines = linecache.getlines(path)

        def join_source(lnum: int) -> str:
            return "".join(inspect.getblock(lines[lnum:]))

        if inspect.isclass(obj):
            qualname = obj.__qualname__
            source = "".join(lines)
            tree = ast.parse(source)
            class_finder = _ClassFinder(qualname)
            try:
                class_finder.visit(tree)
            except ClassFoundException as e:
                return join_source(e.args[0])
        elif inspect.isfunction(obj):
            obj = obj.__code__
            if hasattr(obj, "co_firstlineno"):
                lnum = obj.co_firstlineno - 1
                pat = re.compile(r"^(\s*def\s)|(\s*async\s+def\s)|(.*(?<!\w)lambda(:|\s))|^(\s*@)")
                while lnum > 0:
                    try:
                        line = lines[lnum]
                    except IndexError:
                        raise OSError("lineno is out of bounds")
                    if pat.match(line):
                        break
                    lnum = lnum - 1
                return join_source(lnum)
    raise SQLMeshError(f"Cannot find source for {obj}")


def parse_source(func: t.Callable) -> ast.Module:
    """Parse a function and returns an ast node."""
    return ast.parse(textwrap.dedent(getsource(func)))


def _decorator_name(decorator: ast.expr) -> str:
    if isinstance(decorator, ast.Call):
        return decorator.func.id  # type: ignore
    if isinstance(decorator, ast.Name):
        return decorator.id
    return ""


def decorators(func: t.Callable) -> t.List[str]:
    """Finds a list of all the decorators of a callable."""
    root_node = parse_source(func)
    decorators = []

    for node in ast.walk(root_node):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            for decorator in node.decorator_list:
                name = _decorator_name(decorator)
                if name not in IGNORE_DECORATORS:
                    decorators.append(name)
    return unique(decorators)


def normalize_source(obj: t.Any) -> str:
    """Rewrites an object's source with formatting and doc strings removed by using Python ast.

    Args:
        obj: The object to fetch source from and convert to a string.

    Returns:
        A string representation of the normalized function.
    """
    root_node = parse_source(obj)

    for node in ast.walk(root_node):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            for decorator in node.decorator_list:
                if _decorator_name(decorator) in IGNORE_DECORATORS:
                    node.decorator_list.remove(decorator)

            # remove docstrings
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Str):
                node.body = body[1:]

            # remove function return type annotation
            if isinstance(node, ast.FunctionDef):
                node.returns = None

    return to_source(root_node).strip()


def build_env(
    obj: t.Any,
    *,
    env: t.Dict[str, t.Any],
    name: str,
    path: Path,
) -> None:
    """Fills in env dictionary with all globals needed to execute the object.

    Recursively traverse classes and functions.

    Args:
        obj: Any python object.
        env: Dictionary to store the env.
        name: Name of the object in the env.
        path: The module path to serialize. Other modules will not be walked and treated as imports.
    """

    obj_module = inspect.getmodule(obj)

    if obj_module and obj_module.__name__ == "builtins":
        return

    def walk(obj: t.Any) -> None:
        if inspect.isclass(obj):
            for decorator in decorators(obj):
                if obj_module and decorator in obj_module.__dict__:
                    build_env(
                        obj_module.__dict__[decorator],
                        env=env,
                        name=decorator,
                        path=path,
                    )

            for base in obj.__bases__:
                build_env(base, env=env, name=base.__qualname__, path=path)

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
                        build_env(v, env=env, name=v.__name__, path=path)
        elif callable(obj):
            for k, v in func_globals(obj).items():
                build_env(v, env=env, name=k, path=path)

    if name not in env:
        # We only need to add the undecorated code of @macro() functions in env, which
        # is accessible through the `__wrapped__` attribute added by functools.wraps
        # We account for the case where the function is wrapped multiple times too
        env[name] = inspect.unwrap(obj) if hasattr(obj, "__sqlmesh_macro__") else obj

        if obj_module and _is_relative_to(obj_module.__file__, path):
            walk(env[name])
    elif env[name] != obj:
        raise SQLMeshError(
            f"Cannot store {obj} in environment, duplicate definitions found for '{name}'"
        )


class ExecutableKind(str, Enum):
    """The kind of of executable. The order of the members is used when serializing the python model to text."""

    IMPORT = "import"
    VALUE = "value"
    DEFINITION = "definition"
    STATEMENT = "statement"

    def __lt__(self, other: t.Any) -> bool:
        if not isinstance(other, ExecutableKind):
            return NotImplemented
        values = list(ExecutableKind.__dict__.values())
        return values.index(self) < values.index(other)

    def __str__(self) -> str:
        return self.value


class Executable(PydanticModel):
    payload: str
    kind: ExecutableKind = ExecutableKind.DEFINITION
    name: t.Optional[str] = None
    path: t.Optional[str] = None
    alias: t.Optional[str] = None

    @property
    def is_definition(self) -> bool:
        return self.kind == ExecutableKind.DEFINITION

    @property
    def is_import(self) -> bool:
        return self.kind == ExecutableKind.IMPORT

    @property
    def is_statement(self) -> bool:
        return self.kind == ExecutableKind.STATEMENT

    @property
    def is_value(self) -> bool:
        return self.kind == ExecutableKind.VALUE

    @classmethod
    def value(cls, v: t.Any) -> Executable:
        return Executable(payload=repr(v), kind=ExecutableKind.VALUE)


def serialize_env(env: t.Dict[str, t.Any], path: Path) -> t.Dict[str, Executable]:
    """Serializes a python function into a self contained dictionary.

    Recursively walks a function's globals to store all other references inside of env.

    Args:
        env: Dictionary to store the env.
        path: The root path to seralize. Other modules will not be walked and treated as imports.
    """
    serialized = {}

    for k, v in env.items():
        if callable(v):
            name = v.__name__
            name = k if name == "<lambda>" else name
            file_path = Path(inspect.getfile(v))

            if _is_relative_to(file_path, path):
                serialized[k] = Executable(
                    name=name,
                    payload=normalize_source(v),
                    kind=ExecutableKind.DEFINITION,
                    # Do `as_posix` to serialize windows path back to POSIX
                    path=file_path.relative_to(path.absolute()).as_posix(),
                    alias=k if name != k else None,
                )
            else:
                serialized[k] = Executable(
                    payload=f"from {v.__module__} import {name}",
                    kind=ExecutableKind.IMPORT,
                )
        elif inspect.ismodule(v):
            name = v.__name__
            if _is_relative_to(v.__file__, path):
                raise SQLMeshError(
                    f"Cannot serialize 'import {name}'. Use 'from {name} import ...' instead."
                )
            postfix = "" if name == k else f" as {k}"
            serialized[k] = Executable(
                payload=f"import {name}{postfix}",
                kind=ExecutableKind.IMPORT,
            )
        else:
            serialized[k] = Executable.value(v)

    return serialized


def prepare_env(
    python_env: t.Dict[str, Executable],
    env: t.Optional[t.Dict[str, t.Any]] = None,
) -> t.Dict[str, t.Any]:
    """Prepare a python env by hydrating and executing functions.

    The Python ENV is stored in a json serializable format.
    Functions and imports are stored as a special data class.

    Args:
        python_env: The dictionary containing the serialized python environment.
        env: The dictionary to execute code in.

    Returns:
        The prepared environment with hydrated functions.
    """
    env = {} if env is None else env

    for name, executable in sorted(
        python_env.items(), key=lambda item: 0 if item[1].is_import else 1
    ):
        if executable.is_value:
            env[name] = ast.literal_eval(executable.payload)
        else:
            exec(executable.payload, env)
            if executable.alias and executable.name:
                env[executable.alias] = env[executable.name]
    return env


def print_exception(
    exception: Exception,
    python_env: t.Dict[str, Executable],
    out: t.TextIO = sys.stderr,
) -> None:
    """Formats exceptions that occur from evaled code.

    Stack traces generated by evaled code lose code context and are difficult to debug.
    This intercepts the default stack trace and tries to make it debuggable.

    Args:
        exception: The exception to print the stack trace for.
        python_env: The environment containing stringified python code.
        out: The output stream to write to.
    """
    tb: t.List[str] = []

    for error_line in format_exception(exception):
        match = re.search('File "<string>", line (.*), in (.*)', error_line)

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

        error_line = (
            f"{indent}File '{executable.path}' (or imported file), line {line_num}, in {func}"
        )

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


def import_python_file(path: Path, relative_base: Path = Path()) -> types.ModuleType:
    relative_path = path.absolute().relative_to(relative_base.absolute())
    module_name = str(relative_path.with_suffix("")).replace(os.path.sep, ".")

    # remove the entire module hierarchy in case they were already loaded
    parts = module_name.split(".")
    for i in range(len(parts)):
        sys.modules.pop(".".join(parts[0 : i + 1]), None)

    return importlib.import_module(module_name)
