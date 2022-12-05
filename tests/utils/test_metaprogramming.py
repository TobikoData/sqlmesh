import typing as t

import pandas as pd
import sqlglot
from pytest_mock.plugin import MockerFixture
from sqlglot.expressions import to_table

from sqlmesh.core.model import EXEC_PREFIX, prepare_env
from sqlmesh.utils.metaprogramming import (
    build_env,
    func_globals,
    normalize_source,
    print_exception,
    serialize_env,
)

X = 1
Y = 2
Z = 3

my_lambda = lambda: print("z")

KLASS_X = 1
KLASS_Y = 2
KLASS_Z = 3


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

    def closure(z: int) -> int:
        return z + Z

    return closure(y) + other_func(Y)


def test_func_globals() -> None:
    assert func_globals(main_func) == {
        "Y": 2,
        "Z": 3,
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


def test_normalize_source() -> None:
    assert (
        normalize_source(main_func)
        == """def main_func(y):
    sqlglot.parse_one('1')
    MyClass()

    def closure(z):
        return z + Z
    return closure(y) + other_func(Y)"""
    )

    assert (
        normalize_source(other_func)
        == """def other_func(a):
    import sqlglot
    sqlglot.parse_one('1')
    pd.DataFrame([{'x': 1}])
    to_table('y')
    my_lambda()
    return X + a"""
    )


def test_serialize_env() -> None:
    env: t.Dict[str, t.Any] = {}
    build_env(main_func, env=env, name="MAIN", module="tests")
    env = serialize_env(env, module="tests", prefix="PREFIX ")  # type: ignore

    assert env == {
        "MAIN": """PREFIX def main_func(y):
    sqlglot.parse_one('1')
    MyClass()

    def closure(z):
        return z + Z
    return closure(y) + other_func(Y)""",
        "X": 1,
        "Y": 2,
        "Z": 3,
        "KLASS_X": 1,
        "KLASS_Y": 2,
        "KLASS_Z": 3,
        "to_table": "PREFIX from sqlglot.expressions import to_table",
        "MyClass": """PREFIX class MyClass:

    @staticmethod
    def foo():
        return KLASS_X

    @classmethod
    def bar(cls):
        return KLASS_Y

    def baz(self):
        return KLASS_Z""",
        "pd": "PREFIX import pandas as pd",
        "sqlglot": "PREFIX import sqlglot",
        "my_lambda": "PREFIX my_lambda = lambda : print('z')",
        "other_func": """PREFIX def other_func(a):
    import sqlglot
    sqlglot.parse_one('1')
    pd.DataFrame([{'x': 1}])
    to_table('y')
    my_lambda()
    return X + a""",
    }


def test_print_exception(mocker: MockerFixture):
    out_mock = mocker.Mock()

    test_code = """

def test_fun():
    raise RuntimeError("error")

"""

    test_path = "/test/path.py"
    test_env = {"test_fun": f"{EXEC_PREFIX}{test_code}"}
    env: t.Dict[str, t.Any] = {}
    prepare_env(env, test_env)
    try:
        eval("test_fun()", env)
    except Exception as ex:
        print_exception(ex, test_env, test_path, out_mock)

    expected_message = f"""Traceback (most recent call last):

  File "{__file__}", line 162, in test_print_exception
    eval("test_fun()", env)

  File "<string>", line 1, in <module>

  File '/test/path.py' (or imported file), line 4, in test_fun


    def test_fun():
        raise RuntimeError("error")


RuntimeError: error
"""
    out_mock.write.assert_called_once_with(expected_message)
