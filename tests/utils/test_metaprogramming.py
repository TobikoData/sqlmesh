import typing as t
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest
import sqlglot
from pytest_mock.plugin import MockerFixture
from sqlglot import exp
from sqlglot import exp as expressions
from sqlglot.expressions import to_table

import tests.utils.test_date as test_date
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core import constants as c
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.metaprogramming import (
    Executable,
    ExecutableKind,
    build_env,
    func_globals,
    normalize_source,
    prepare_env,
    print_exception,
    serialize_env,
)


def test_print_exception(mocker: MockerFixture):
    out_mock = mocker.Mock()

    test_env = {
        "test_fun": Executable(
            name="test_func",
            payload="""def test_fun():
    raise RuntimeError("error")""",
            path="/test/path.py",
        ),
    }
    env = prepare_env(test_env)
    try:
        eval("test_fun()", env)
    except Exception as ex:
        print_exception(ex, test_env, out_mock)

    expected_message = f"""Traceback (most recent call last):

  File "{__file__}", line 43, in test_print_exception
    eval("test_fun()", env)

  File "<string>", line 1, in <module>

  File '/test/path.py' (or imported file), line 2, in test_fun
    def test_fun():
        raise RuntimeError("error")


RuntimeError: error
"""
    out_mock.write.assert_called_once_with(expected_message)


X = 1
Y = 2
Z = 3

my_lambda = lambda: print("z")  # noqa: E731

KLASS_X = 1
KLASS_Y = 2
KLASS_Z = 3


@dataclass
class DataClass:
    x: int


class MyClass:
    @staticmethod
    def foo():
        return KLASS_X

    @classmethod
    def bar(cls):
        return KLASS_Y

    def baz(self):
        return KLASS_Z


def other_func(a: int) -> int:
    import sqlglot

    sqlglot.parse_one("1")
    pd.DataFrame([{"x": 1}])
    to_table("y")
    my_lambda()  # type: ignore
    return X + a


def noop_metadata() -> None:
    return None


setattr(noop_metadata, c.SQLMESH_METADATA, True)


@contextmanager
def test_context_manager():
    yield


def main_func(y: int, foo=exp.true(), *, bar=expressions.Literal.number(1) + 2) -> int:
    """DOC STRING"""
    sqlglot.parse_one("1")
    MyClass()
    DataClass(x=y)
    noop_metadata()
    normalize_model_name("test")

    def closure(z: int) -> int:
        return z + Z

    with test_context_manager():
        pass

    return closure(y) + other_func(Y)


def test_func_globals() -> None:
    assert func_globals(main_func) == {
        "Y": 2,
        "Z": 3,
        "DataClass": DataClass,
        "MyClass": MyClass,
        "noop_metadata": noop_metadata,
        "normalize_model_name": normalize_model_name,
        "other_func": other_func,
        "sqlglot": sqlglot,
        "exp": exp,
        "expressions": exp,
        "test_context_manager": test_context_manager,
    }
    assert func_globals(other_func) == {
        "X": 1,
        "my_lambda": my_lambda,
        "pd": pd,
        "to_table": to_table,
    }

    def closure_test() -> t.Callable:
        y = 1

        def closure() -> int:
            return main_func(y)

        return closure

    assert func_globals(closure_test()) == {
        "main_func": main_func,
        "y": 1,
    }


def test_normalize_source() -> None:
    assert (
        normalize_source(main_func)
        == """def main_func(y: int, foo=exp.true(), *, bar=expressions.Literal.number(1) + 2
    ):
    sqlglot.parse_one('1')
    MyClass()
    DataClass(x=y)
    noop_metadata()
    normalize_model_name('test')

    def closure(z: int):
        return z + Z
    with test_context_manager():
        pass
    return closure(y) + other_func(Y)"""
    )

    assert (
        normalize_source(other_func)
        == """def other_func(a: int):
    import sqlglot
    sqlglot.parse_one('1')
    pd.DataFrame([{'x': 1}])
    to_table('y')
    my_lambda()
    return X + a"""
    )


def test_serialize_env_error() -> None:
    with pytest.raises(SQLMeshError):
        # pretend to be the module pandas
        serialize_env({"test_date": test_date}, path=Path("tests/utils"))


def test_serialize_env() -> None:
    env: t.Dict[str, t.Any] = {}
    path = Path("tests/utils")
    build_env(main_func, env=env, name="MAIN", path=path)
    env = serialize_env(env, path=path)  # type: ignore

    assert prepare_env(env)
    assert env == {
        "MAIN": Executable(
            name="main_func",
            alias="MAIN",
            path="test_metaprogramming.py",
            payload="""def main_func(y: int, foo=exp.true(), *, bar=expressions.Literal.number(1) + 2
    ):
    sqlglot.parse_one('1')
    MyClass()
    DataClass(x=y)
    noop_metadata()
    normalize_model_name('test')

    def closure(z: int):
        return z + Z
    with test_context_manager():
        pass
    return closure(y) + other_func(Y)""",
        ),
        "X": Executable(payload="1", kind=ExecutableKind.VALUE),
        "Y": Executable(payload="2", kind=ExecutableKind.VALUE),
        "Z": Executable(payload="3", kind=ExecutableKind.VALUE),
        "_GeneratorContextManager": Executable(
            payload="from contextlib import _GeneratorContextManager", kind=ExecutableKind.IMPORT
        ),
        "contextmanager": Executable(
            payload="from contextlib import contextmanager", kind=ExecutableKind.IMPORT
        ),
        "KLASS_X": Executable(payload="1", kind=ExecutableKind.VALUE),
        "KLASS_Y": Executable(payload="2", kind=ExecutableKind.VALUE),
        "KLASS_Z": Executable(payload="3", kind=ExecutableKind.VALUE),
        "to_table": Executable(
            kind=ExecutableKind.IMPORT,
            payload="from sqlglot.expressions import to_table",
        ),
        "DataClass": Executable(
            kind=ExecutableKind.DEFINITION,
            name="DataClass",
            path="test_metaprogramming.py",
            payload="""@dataclass
class DataClass:
    x: int""",
        ),
        "MyClass": Executable(
            kind=ExecutableKind.DEFINITION,
            name="MyClass",
            path="test_metaprogramming.py",
            payload="""class MyClass:

    @staticmethod
    def foo():
        return KLASS_X

    @classmethod
    def bar(cls):
        return KLASS_Y

    def baz(self):
        return KLASS_Z""",
        ),
        "dataclass": Executable(
            payload="from dataclasses import dataclass", kind=ExecutableKind.IMPORT
        ),
        "pd": Executable(payload="import pandas as pd", kind=ExecutableKind.IMPORT),
        "sqlglot": Executable(kind=ExecutableKind.IMPORT, payload="import sqlglot"),
        "exp": Executable(kind=ExecutableKind.IMPORT, payload="import sqlglot.expressions as exp"),
        "expressions": Executable(
            kind=ExecutableKind.IMPORT, payload="import sqlglot.expressions as expressions"
        ),
        "func": Executable(
            payload="""@contextmanager
def test_context_manager():
    yield""",
            name="test_context_manager",
            path="test_metaprogramming.py",
            alias="func",
        ),
        "my_lambda": Executable(
            name="my_lambda",
            path="test_metaprogramming.py",
            payload="my_lambda = lambda : print('z')",
        ),
        "noop_metadata": Executable(
            name="noop_metadata",
            path="test_metaprogramming.py",
            payload="""def noop_metadata():
    return None""",
            is_metadata=True,
        ),
        "normalize_model_name": Executable(
            payload="from sqlmesh.core.dialect import normalize_model_name",
            kind=ExecutableKind.IMPORT,
        ),
        "other_func": Executable(
            name="other_func",
            path="test_metaprogramming.py",
            payload="""def other_func(a: int):
    import sqlglot
    sqlglot.parse_one('1')
    pd.DataFrame([{'x': 1}])
    to_table('y')
    my_lambda()
    return X + a""",
        ),
        "test_context_manager": Executable(
            payload="from tests.utils.test_metaprogramming import test_context_manager",
            kind=ExecutableKind.IMPORT,
        ),
        "wraps": Executable(payload="from functools import wraps", kind=ExecutableKind.IMPORT),
    }
