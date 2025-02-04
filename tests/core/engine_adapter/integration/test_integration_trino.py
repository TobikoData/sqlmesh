import typing as t
import pytest
from pathlib import Path
from sqlmesh.core.engine_adapter import TrinoEngineAdapter
from tests.core.engine_adapter.integration import TestContext
from sqlglot import parse_one, exp

pytestmark = [pytest.mark.docker, pytest.mark.engine, pytest.mark.trino]


@pytest.fixture(
    params=[
        pytest.param(
            "trino",
            marks=[
                pytest.mark.docker,
                pytest.mark.engine,
                pytest.mark.trino,
            ],
        ),
        pytest.param(
            "trino_iceberg",
            marks=[
                pytest.mark.docker,
                pytest.mark.engine,
                pytest.mark.trino_iceberg,
            ],
        ),
        pytest.param(
            "trino_delta",
            marks=[
                pytest.mark.docker,
                pytest.mark.engine,
                pytest.mark.trino_delta,
            ],
        ),
        pytest.param(
            "trino_nessie",
            marks=[
                pytest.mark.docker,
                pytest.mark.engine,
                pytest.mark.trino_nessie,
            ],
        ),
    ]
)
def mark_gateway(request) -> t.Tuple[str, str]:
    return request.param, f"inttest_{request.param}"


@pytest.fixture
def test_type() -> str:
    return "query"


def test_macros_in_physical_properties(
    tmp_path: Path, ctx: TestContext, engine_adapter: TrinoEngineAdapter
):
    if "iceberg" not in ctx.gateway:
        pytest.skip("This test only needs to be run once")

    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True)

    schema = ctx.schema()

    with open(models_dir / "test_model.sql", "w") as f:
        f.write(
            """
        MODEL (
            name SCHEMA.test,
            kind FULL,
            physical_properties (
                location = @resolve_template('s3://trino/@{catalog_name}/@{schema_name}/@{table_name}'),
                sorted_by = @if(@gateway = 'inttest_trino_iceberg', ARRAY['col_a'], ARRAY['col_b'])
            )
        );

        select 1 as col_a, 2 as col_b;
        """.replace("SCHEMA", schema)
        )

    context = ctx.create_context(path=tmp_path)
    assert len(context.models) == 1

    plan_result = context.plan(auto_apply=True, no_prompts=True)

    assert len(plan_result.new_snapshots) == 1

    snapshot = plan_result.new_snapshots[0]

    physical_table_str = snapshot.table_name()
    physical_table = exp.to_table(physical_table_str)
    create_sql = list(engine_adapter.fetchone(f"show create table {physical_table}") or [])[0]

    parsed_create_sql = parse_one(create_sql, dialect="trino")

    location_property = parsed_create_sql.find(exp.LocationProperty)
    assert location_property

    assert "@{table_name}" not in location_property.sql(dialect="trino")
    assert (
        location_property.text("this")
        == f"s3://trino/{physical_table.catalog}/{physical_table.db}/{physical_table.name}"
    )

    sorted_by_property = next(
        p for p in parsed_create_sql.find_all(exp.Property) if "sorted_by" in p.sql(dialect="trino")
    )
    assert sorted_by_property.sql(dialect="trino") == "sorted_by=ARRAY['col_a ASC NULLS FIRST']"
