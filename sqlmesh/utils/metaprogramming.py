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
from dataclasses import dataclass
from enum import Enum
from numbers import Number
from pathlib import Path

from astor import to_source

from sqlmesh.core import constants as c
from sqlmesh.utils import format_exception, unique
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

IGNORE_DECORATORS = {"macro", "model", "signal"}
SERIALIZABLE_CALLABLES = (type, types.FunctionType)
LITERALS = (Number, str, bytes, tuple, list, dict, set, bool)


def _is_relative_to(path: t.Optional[Path | str], other: t.Optional[Path | str]) -> bool:
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
        root_node = parse_source(func)

        func_args = next(node for node in ast.walk(root_node) if isinstance(node, ast.arguments))
        arg_defaults = (d for d in func_args.defaults + func_args.kw_defaults if d is not None)

        # ast.Name corresponds to variable references, such as foo or x.foo. The former is
        # represented as Name(id=foo), and the latter as Attribute(value=Name(id=x) attr=foo)
        arg_globals = [
            n.id for default in arg_defaults for n in ast.walk(default) if isinstance(n, ast.Name)
        ]

        code = func.__code__
        for var in (
            arg_globals + list(_code_globals(code)) + decorator_vars(func, root_node=root_node)
        ):
            if var in func.__globals__:
                variables[var] = func.__globals__[var]

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


class _DecoratorDependencyFinder(ast.NodeVisitor):
    def __init__(self) -> None:
        self.dependencies: t.List[str] = []

    def _extract_dependencies(self, node: ast.ClassDef | ast.FunctionDef) -> None:
        for decorator in node.decorator_list:
            dependencies: t.List[str] = []
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

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._extract_dependencies(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._extract_dependencies(node)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore


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
    node = decorator
    if isinstance(decorator, ast.Call):
        node = decorator.func
    return node.id if isinstance(node, ast.Name) else ""


def decorator_vars(func: t.Callable, root_node: t.Optional[ast.Module] = None) -> t.List[str]:
    """
    Returns a list of all the decorators of a callable, as well as names of objects that
    are referenced in their argument list. These objects may be transitive dependencies
    that we need to include in the serialized python environments.
    """
    root_node = root_node or parse_source(func)
    finder = _DecoratorDependencyFinder()
    finder.visit(root_node)
    return unique(finder.dependencies)


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
    # We don't rely on `env` to keep track of visited objects, because it's populated in post-order
    visited: t.Set[str] = set()

    def walk(obj: t.Any, name: str) -> None:
        obj_module = inspect.getmodule(obj)
        if name in visited or (obj_module and obj_module.__name__ == "builtins"):
            return

        visited.add(name)
        if name not in env:
            if hasattr(obj, c.SQLMESH_MACRO):
                # We only need to add the undecorated code of @macro() functions in env, which
                # is accessible through the `__wrapped__` attribute added by functools.wraps
                obj = obj.__wrapped__
            elif callable(obj) and not isinstance(obj, SERIALIZABLE_CALLABLES):
                obj = getattr(obj, "__wrapped__", None)
                name = getattr(obj, "__name__", "")

                # Callable class instances shouldn't be serialized (e.g. tenacity.Retrying).
                # We still want to walk the callables they decorate, though
                if not isinstance(obj, SERIALIZABLE_CALLABLES) or name in env:
                    return

            if (
                not obj_module
                or not hasattr(obj_module, "__file__")
                or not _is_relative_to(obj_module.__file__, path)
            ):
                env[name] = obj
                return
        elif env[name] != obj:
            raise SQLMeshError(
                f"Cannot store {obj} in environment, duplicate definitions found for '{name}'"
            )

        if inspect.isclass(obj):
            for var in decorator_vars(obj):
                if obj_module and var in obj_module.__dict__:
                    walk(obj_module.__dict__[var], var)

            for base in obj.__bases__:
                walk(base, base.__qualname__)

            for k, v in obj.__dict__.items():
                if k.startswith("__"):
                    continue

                # Traverse methods in a class to find global references
                if isinstance(v, (classmethod, staticmethod)):
                    v = v.__func__

                if callable(v):
                    # Walk the method if it's part of the object, else it's a global function and we just store it
                    if v.__qualname__.startswith(obj.__qualname__):
                        for k, v in func_globals(v).items():
                            walk(v, k)
                    else:
                        walk(v, v.__name__)
        elif callable(obj):
            for k, v in func_globals(obj).items():
                walk(v, k)

        # We store the object in the environment after its dependencies, because otherwise we
        # could crash at environment hydration time, since dicts are ordered and the top-level
        # objects would be loaded before their dependencies.
        env[name] = obj

    walk(obj, name)


@dataclass
class SqlValue:
    """A SQL string representing a generated SQLGlot AST."""

    sql: str


class ExecutableKind(str, Enum):
    """The kind of of executable. The order of the members is used when serializing the python model to text."""

    IMPORT = "import"
    VALUE = "value"
    DEFINITION = "definition"

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
    is_metadata: t.Optional[bool] = None

    @property
    def is_definition(self) -> bool:
        return self.kind == ExecutableKind.DEFINITION

    @property
    def is_import(self) -> bool:
        return self.kind == ExecutableKind.IMPORT

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
        if isinstance(v, LITERALS) or v is None:
            serialized[k] = Executable.value(v)
        elif inspect.ismodule(v):
            name = v.__name__
            if hasattr(v, "__file__") and _is_relative_to(v.__file__, path):
                raise SQLMeshError(
                    f"Cannot serialize 'import {name}'. Use 'from {name} import ...' instead."
                )
            postfix = "" if name == k else f" as {k}"
            serialized[k] = Executable(
                payload=f"import {name}{postfix}",
                kind=ExecutableKind.IMPORT,
            )
        elif callable(v):
            name = v.__name__
            name = k if name == "<lambda>" else name

            # getfile raises a `TypeError` for built-in modules, classes, or functions
            # https://docs.python.org/3/library/inspect.html#inspect.getfile
            try:
                file_path = Path(inspect.getfile(v))
                relative_obj_file_path = _is_relative_to(file_path, path)

                # A callable can be a "wrapper" that is defined in a third-party library [1], in which case the file
                # containing its definition won't be relative to the project's path. This can lead to serializing
                # it as a "relative import", such as `from models.some_python_model import foo`, because the `wraps`
                # decorator preserves the wrapped function's module [2]. Payloads like this are invalid, as they
                # can result in `ModuleNotFoundError`s when hydrating python environments, e.g. if a project's files
                # are not available during a scheduled cadence run.
                #
                # [1]: https://github.com/jd/tenacity/blob/0d40e76f7d06d631fb127e1ec58c8bd776e70d49/tenacity/__init__.py#L322-L346
                # [2]: https://github.com/python/cpython/blob/f502c8f6a6db4be27c97a0e5466383d117859b7f/Lib/functools.py#L33-L57
                if not relative_obj_file_path and (wrapped := getattr(v, "__wrapped__", None)):
                    v = wrapped
                    file_path = Path(inspect.getfile(wrapped))
                    relative_obj_file_path = _is_relative_to(file_path, path)
            except TypeError:
                file_path = None
                relative_obj_file_path = False

            if relative_obj_file_path:
                serialized[k] = Executable(
                    name=name,
                    payload=normalize_source(v),
                    kind=ExecutableKind.DEFINITION,
                    # Do `as_posix` to serialize windows path back to POSIX
                    path=t.cast(Path, file_path).relative_to(path.absolute()).as_posix(),
                    alias=k if name != k else None,
                    is_metadata=getattr(v, c.SQLMESH_METADATA, None),
                )
            else:
                serialized[k] = Executable(
                    payload=f"from {v.__module__} import {name}",
                    kind=ExecutableKind.IMPORT,
                )
        else:
            raise SQLMeshError(
                f"Object '{v}' cannot be serialized. If it's defined in a library, import the corresponding "
                "module and reference the object using its fully-qualified name. For example, the datetime "
                "module's 'UTC' object should be accessed as 'datetime.UTC'."
            )

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
            env[name] = eval(executable.payload)
        else:
            exec(executable.payload, env)
            if executable.alias and executable.name:
                env[executable.alias] = env[executable.name]

    return env


def format_evaluated_code_exception(
    exception: Exception,
    python_env: t.Dict[str, Executable],
) -> str:
    """Formats exceptions that occur from evaled code.

    Stack traces generated by evaled code lose code context and are difficult to debug.
    This intercepts the default stack trace and tries to make it debuggable.

    Args:
        exception: The exception to print the stack trace for.
        python_env: The environment containing stringified python code.
    """
    tb: t.List[str] = []
    indent = ""

    for error_line in format_exception(exception):
        traceback_match = error_line.startswith("Traceback (most recent call last):")
        model_def_match = re.search('File ".*?core/model/definition.py', error_line)
        if traceback_match or model_def_match:
            continue

        error_match = re.search("^.*?Error: ", error_line)
        if error_match:
            tb.append(f"{indent*2}  {error_line}")
            continue

        eval_code_match = re.search('File "<string>", line (.*), in (.*)', error_line)
        if not eval_code_match:
            tb.append(f"{indent}{error_line}")
            continue

        line_num = int(eval_code_match.group(1))
        func = eval_code_match.group(2)

        if func not in python_env:
            tb.append(error_line)
            continue

        executable = python_env[func]
        indent = error_line[: eval_code_match.start()]

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
            )
        )

    return os.linesep.join(tb)


def print_exception(
    exception: Exception,
    python_env: t.Dict[str, Executable],
    out: t.TextIO = sys.stderr,
) -> None:
    """Prints exceptions that occur from evaled code.

    Stack traces generated by evaled code lose code context and are difficult to debug.
    This intercepts the default stack trace and tries to make it debuggable.

    Args:
        exception: The exception to print the stack trace for.
        python_env: The environment containing stringified python code.
        out: The output stream to write to.
    """
    tb = format_evaluated_code_exception(exception, python_env)
    out.write(tb)


def import_python_file(path: Path, relative_base: Path = Path()) -> types.ModuleType:
    relative_path = path.absolute().relative_to(relative_base.absolute())
    module_name = str(relative_path.with_suffix("")).replace(os.path.sep, ".")

    # remove the entire module hierarchy in case they were already loaded
    parts = module_name.split(".")
    for i in range(len(parts)):
        sys.modules.pop(".".join(parts[0 : i + 1]), None)

    return importlib.import_module(module_name)
