import typing as t
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest
import sqlglot
from pytest_mock.plugin import MockerFixture
from sqlglot.expressions import to_table

import tests.utils.test_date as test_date
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

  File "{__file__}", line 38, in test_print_exception
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

my_lambda = lambda: print("z")

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


def main_func(y: int) -> int:
    """DOC STRING"""
    sqlglot.parse_one("1")
    MyClass()
    DataClass(x=y)

    def closure(z: int) -> int:
        return z + Z

    return closure(y) + other_func(Y)


def test_func_globals() -> None:
    assert func_globals(main_func) == {
        "Y": 2,
        "Z": 3,
        "DataClass": DataClass,
        "MyClass": MyClass,
        "other_func": other_func,
        "sqlglot": sqlglot,
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
        == """def main_func(y: int):
    sqlglot.parse_one('1')
    MyClass()
    DataClass(x=y)

    def closure(z: int):
        return z + Z
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
    with pytest.raises(SQLMeshError) as e:
        # pretend to be the module pandas
        serialize_env({"test_date": test_date}, path=Path("tests/utils"))


def test_serialize_env() -> None:
    env: t.Dict[str, t.Any] = {}
    path = Path("tests/utils")
    build_env(main_func, env=env, name="MAIN", path=path)
    env = serialize_env(env, path=path)  # type: ignore

    assert env == {
        "MAIN": Executable(
            name="main_func",
            alias="MAIN",
            path="test_metaprogramming.py",
            payload="""def main_func(y: int):
    sqlglot.parse_one('1')
    MyClass()
    DataClass(x=y)

    def closure(z: int):
        return z + Z
    return closure(y) + other_func(Y)""",
        ),
        "X": Executable(payload="1", kind=ExecutableKind.VALUE),
        "Y": Executable(payload="2", kind=ExecutableKind.VALUE),
        "Z": Executable(payload="3", kind=ExecutableKind.VALUE),
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
        "my_lambda": Executable(
            name="my_lambda",
            path="test_metaprogramming.py",
            payload=f"my_lambda = lambda : print('z')",
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
    }
