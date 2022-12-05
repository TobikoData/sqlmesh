from sqlmesh.utils.date import TimeLike, to_date, to_datetime
from sqlmesh.utils.pydantic import PydanticModel


def test_datetime_date_serialization() -> None:
    class Test(PydanticModel):
        ds: TimeLike

    target_ds = "2022-01-01"

    deserialized_date = Test.parse_raw(Test(ds=to_date(target_ds)).json())
    assert deserialized_date.ds == "2022-01-01"

    deserialized_datetime = Test.parse_raw(Test(ds=to_datetime(target_ds)).json())
    assert deserialized_datetime.ds == "2022-01-01T00:00:00+00:00"
