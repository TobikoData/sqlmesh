from functools import cached_property

from sqlmesh.utils.date import TimeLike, to_date, to_datetime
from sqlmesh.utils.pydantic import PydanticModel


def test_datetime_date_serialization() -> None:
    class Test(PydanticModel):
        ds: TimeLike

    target_ds = "2022-01-01"

    deserialized_date = Test.parse_raw(Test(ds=to_date(target_ds)).json())
    deserialized_datetime = Test.parse_raw(Test(ds=to_datetime(target_ds)).json())
    assert deserialized_date.ds == to_date(target_ds)
    assert deserialized_datetime.ds == to_datetime("2022-01-01T00:00:00+00:00")


def test_pydantic_2_equality() -> None:
    class TestModel(PydanticModel):
        name: str

        @cached_property
        def private(self) -> str:
            return "should be ignored"

    model_a = TestModel(name="a")
    model_a_duplicate = TestModel(name="a")
    assert model_a == model_a_duplicate
    model_b = TestModel(name="b")
    assert model_a != model_b


def test_pydantic_2_hash() -> None:
    class TestModel(PydanticModel):
        name: str

        @cached_property
        def private(self) -> str:
            return "should be ignored"

    class TestModel2(PydanticModel):
        name: str
        field2: str = "test"

        @cached_property
        def private(self) -> str:
            return "should be ignored"

    model_a = TestModel(name="a")
    model_a_duplicate = TestModel(name="a")
    assert hash(model_a) == hash(model_a_duplicate)

    model_2_a = TestModel2(name="a")
    model_2_b = TestModel2(name="a")
    assert hash(model_2_a) == hash(model_2_b)
    assert hash(model_a) != hash(model_2_a)


def test_pydantic_dict_default_args_override() -> None:
    class TestModel(PydanticModel):
        name: str

    assert TestModel(name="foo").dict(by_alias=True)
