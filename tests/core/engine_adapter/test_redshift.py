# type: ignore
import typing as t

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import RedshiftEngineAdapter
from sqlmesh.core.engine_adapter.redshift import parse_plan
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.redshift]


@pytest.fixture
def adapter(make_mocked_engine_adapter):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    adapter.cursor.fetchall.return_value = []
    return adapter


def test_columns(adapter: t.Callable):
    adapter.cursor.fetchall.return_value = [("col", "INT")]
    resp = adapter.columns("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT "column_name", "data_type" FROM "svv_columns" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp == {"col": exp.DataType.build("INT")}


def test_create_table_from_query_exists_no_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )

    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.parse_plan",
        return_value={
            "targetlist": [
                {
                    "name": "TARGETENTRY",
                    "resdom": {
                        "resname": "a",
                        "restype": "1043",
                        "restypmod": "- 1",
                    },
                },
                {
                    "name": "TARGETENTRY",
                    "resdom": {
                        "resname": "b",
                        "restype": "1043",
                        "restypmod": "64",
                    },
                },
                {
                    "name": "TARGETENTRY",
                    "resdom": {
                        "resname": "c",
                        "restype": "1043",
                        "restypmod": "- 1",
                    },
                },
                {
                    "name": "TARGETENTRY",
                    "resdom": {
                        "resname": "d",
                        "restype": "1043",
                        "restypmod": "- 1",
                    },
                },
                {
                    "name": "TARGETENTRY",
                    "resdom": {
                        "resname": "e",
                        "restype": "1114",
                        "restypmod": "- 1",
                    },
                },
                {
                    "name": "TARGETENTRY",
                    "resdom": {
                        "resname": "f",
                        "restype": "0000",  # Unknown type
                        "restypmod": "- 1",
                    },
                },
            ]
        },
    )

    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one(
            "SELECT a, b, x + 1 AS c, d AS d, e, f FROM (SELECT * FROM table WHERE FALSE LIMIT 0) WHERE d > 0 AND FALSE LIMIT 0"
        ),
        exists=False,
    )

    assert to_sql_calls(adapter) == [
        'EXPLAIN VERBOSE CREATE TABLE "test_table" AS SELECT "a", "b", "x" + 1 AS "c", "d" AS "d", "e", "f" FROM (SELECT * FROM "table")',
        'CREATE TABLE "test_table" AS SELECT CAST(NULL AS VARCHAR(MAX)) AS "a", CAST(NULL AS VARCHAR(60)) AS "b", CAST(NULL '
        'AS VARCHAR(MAX)) AS "c", CAST(NULL AS VARCHAR(MAX)) AS "d", CAST(NULL AS TIMESTAMP) AS "e", "f" FROM (SELECT "a", "b", "x" + 1 '
        'AS "c", "d" AS "d", "e", "f" FROM (SELECT * FROM "table" WHERE FALSE LIMIT 0) WHERE "d" > 0 AND FALSE LIMIT 0) AS "_subquery"',
    ]


def test_create_table_from_query_exists_and_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    adapter.cursor.execute.assert_not_called()


def test_create_table_from_query_not_exists_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    adapter.cursor.execute.assert_called_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_create_table_from_query_not_exists_no_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=False,
    )

    adapter.cursor.execute.assert_called_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_values_to_sql(adapter: t.Callable, mocker: MockerFixture):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    result = adapter._values_to_sql(
        values=list(df.itertuples(index=False, name=None)),
        columns_to_types={"a": "int", "b": "int"},
        batch_start=0,
        batch_end=2,
    )
    # 3,6 is missing since the batch range excluded it
    assert (
        result.sql(dialect="redshift")
        == "SELECT CAST(a AS INTEGER) AS a, CAST(b AS INTEGER) AS b FROM (SELECT 1 AS a, 4 AS b UNION ALL SELECT 2, 5) AS t"
    )


def test_replace_query_with_query(adapter: t.Callable, mocker: MockerFixture):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.columns",
        return_value={"cola": exp.DataType(this=exp.DataType.Type.INT)},
    )

    adapter.replace_query(table_name="test_table", query_or_df=parse_one("SELECT cola FROM table"))

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"',
    ]


def test_replace_query_with_df_table_exists(adapter: t.Callable, mocker: MockerFixture):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )
    call_counter = 0

    def mock_table(*args, **kwargs):
        nonlocal call_counter
        call_counter += 1
        return f"temp_table_{call_counter}"

    mock_temp_table = mocker.MagicMock(side_effect=mock_table)
    mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table", mock_temp_table)

    adapter.replace_query(
        table_name="test_table",
        query_or_df=df,
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    adapter.cursor.begin.assert_called_once()
    adapter.cursor.commit.assert_called_once()

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "temp_table_1" ("a" INTEGER, "b" INTEGER)',
        'INSERT INTO "temp_table_1" ("a", "b") SELECT CAST("a" AS INTEGER) AS "a", CAST("b" AS INTEGER) AS "b" FROM (SELECT 1 AS "a", 4 AS "b" UNION ALL SELECT 2, 5 UNION ALL SELECT 3, 6) AS "t"',
        'ALTER TABLE "test_table" RENAME TO "temp_table_2"',
        'ALTER TABLE "temp_table_1" RENAME TO "test_table"',
        'DROP TABLE IF EXISTS "temp_table_2"',
    ]


def test_replace_query_with_df_table_not_exists(adapter: t.Callable, mocker: MockerFixture):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.replace_query(
        table_name="test_table",
        query_or_df=df,
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "test_table" AS SELECT CAST("a" AS INTEGER) AS "a", CAST("b" AS INTEGER) AS "b" FROM (SELECT 1 AS "a", 4 AS "b" UNION ALL SELECT 2, 5 UNION ALL SELECT 3, 6) AS "t"',
    ]


def test_table_exists_db_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    connection_mock.cursor.return_value.fetchone.return_value = (1,)

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    assert adapter.table_exists(table_name=exp.to_table("some_db.some_table"))

    cursor_mock.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'some_table' AND "table_schema" = 'some_db'"""
    )


def test_table_exists_table_only(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    connection_mock.cursor.return_value.fetchone.return_value = None

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    assert not adapter.table_exists(table_name=exp.to_table("some_table"))

    cursor_mock.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'some_table'"""
    )


def test_create_view(adapter: t.Callable):
    adapter.create_view(
        view_name="test_view",
        query_or_df=parse_one("SELECT cola FROM table"),
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    assert to_sql_calls(adapter) == [
        'DROP VIEW IF EXISTS "test_view" CASCADE',
        'CREATE VIEW "test_view" ("a", "b") AS SELECT "cola" FROM "table" WITH NO SCHEMA BINDING',
    ]


def test_parse_plan():
    plan = parse_plan(
        """
{ RESULT
  :startup_cost 0.00
  :total_cost 0.07
  :plan_rows 2
  :node_id 1
  :parent_id 0
  :plan_width 32
  :best_pathkeys <>
  :dist_info.dist_strategy DS_DIST_NONE
  :dist_info.dist_keys <>
  :dist_info.seqscan_is_distributed false
  :dist_info.topology 1966505242
  :phys_properties <>
  :reqd_phys_properties <>
  :targetlist (
    { TARGETENTRY
    :resdom
      { RESDOM
      :resno 1
      :restype 25
      :restypmod 36
      :resname cola
      :ressortgroupref 0
      :resorigtbl 0
      :resorigcol 0
      :resjunk false
      }
    :expr
      { FUNCEXPR
      :funcid 2311
      :funcresulttype 25
      :funcretset false
      :funcformat 0
      :args (
        { RELABELTYPE
        :arg
          { VAR
          :varno 65001
          :varattno 1
          :vartype 1043
          :vartypmod -1
          :varlevelsup 0
          :varnoold 2
          :varoattno 1
          }
        :resulttype 25
        :resulttypmod -1
        :relabelformat 0
        }
      )
      }
    }
  )
  :non_null_target (b)
  :qual <>
  :lefttree
    { SUBQUERYSCAN
    :startup_cost 0.00
    :total_cost 0.06
    :plan_rows 2
    :node_id 2
    :parent_id 1
    :plan_width 32
    :best_pathkeys <>
    :dist_info.dist_strategy DS_DIST_ERR
    :dist_info.dist_keys <>
    :dist_info.seqscan_is_distributed false
    :dist_info.topology 1966505242
    :phys_properties <>
    :reqd_phys_properties <>
    :targetlist (
      { TARGETENTRY
      :resdom
        { RESDOM
        :resno 1
        :restype 1043
        :restypmod -1
        :resname <>
        :ressortgroupref 0
        :resorigtbl 0
        :resorigcol 0
        :resjunk false
        }
      :expr
        { VAR
        :varno 2
        :varattno 1
        :vartype 1043
        :vartypmod -1
        :varlevelsup 0
        :varnoold 2
        :varoattno 1
        }
      }
    )
    :non_null_target (b)
    :qual <>
    :lefttree <>
    :righttree <>
    :initPlan <>
    :extParam (b)
    :allParam (b)
    :nParamExec 0
    :scanrelid 2
    :tuples 0
    :subplan
      { APPEND
      :startup_cost 0.00
      :total_cost 0.04
      :plan_rows 2
      :node_id 3
      :parent_id 2
      :plan_width 0
      :best_pathkeys <>
      :dist_info.dist_strategy DS_DIST_ERR
      :dist_info.dist_keys <>
      :dist_info.seqscan_is_distributed false
      :dist_info.topology 1966505242
      :phys_properties <>
      :reqd_phys_properties <>
      :targetlist (
        { TARGETENTRY
        :resdom
          { RESDOM
          :resno 1
          :restype 1043
          :restypmod 5
          :resname cola
          :ressortgroupref 0
          :resorigtbl 0
          :resorigcol 0
          :resjunk false
          }
        :expr
          { VAR
          :varno 0
          :varattno 1
          :vartype 1043
          :vartypmod 5
          :varlevelsup 0
          :varnoold 0
          :varoattno 1
          }
        }
      )
      :non_null_target (b)
      :qual <>
      :lefttree <>
      :righttree <>
      :initPlan <>
      :extParam (b)
      :allParam (b)
      :nParamExec 0
      :appendplans (
        { NETWORK
        :startup_cost 0.00
        :total_cost 0.02
        :plan_rows 1
        :node_id 4
        :parent_id 3
        :plan_width 0
        :best_pathkeys <>
        :dist_info.dist_strategy DS_DIST_ERR
        :dist_info.dist_keys <>
        :dist_info.seqscan_is_distributed false
        :dist_info.topology 1966505242
        :phys_properties <>
        :reqd_phys_properties <>
        :targetlist (
          { TARGETENTRY
          :resdom
            { RESDOM
            :resno 1
            :restype 1043
            :restypmod 5
            :resname cola
            :ressortgroupref 0
            :resorigtbl 0
            :resorigcol 0
            :resjunk false
            }
          :expr
            { CONST
            :consttype 1043
            :constlen -1
            :constbyval false
            :constisnull false
            :constvalue 5 [ 5 0 0 0 49 ]
            }
          }
        )
        :non_null_target (b)
        :qual <>
        :lefttree
          { SUBQUERYSCAN
          :startup_cost 0.00
          :total_cost 0.02
          :plan_rows 1
          :node_id 5
          :parent_id 4
          :plan_width 0
          :best_pathkeys <>
          :dist_info.dist_strategy DS_DIST_ERR
          :dist_info.dist_keys <>
          :dist_info.seqscan_is_distributed false
          :dist_info.topology 1966505242
          :phys_properties <>
          :reqd_phys_properties <>
          :targetlist (
            { TARGETENTRY
            :resdom
              { RESDOM
              :resno 1
              :restype 1043
              :restypmod 5
              :resname cola
              :ressortgroupref 0
              :resorigtbl 0
              :resorigcol 0
              :resjunk false
              }
            :expr
              { FUNCEXPR
              :funcid 669
              :funcresulttype 1043
              :funcretset false
              :funcformat 2
              :args (
                { CONST
                :consttype 1043
                :constlen -1
                :constbyval false
                :constisnull false
                :constvalue 5 [ 5 0 0 0 49 ]
                }
                { CONST
                :consttype 23
                :constlen 4
                :constbyval true
                :constisnull false
                :constvalue 4 [ 5 0 0 0 0 0 0 0 ]
                }
                { CONST
                :consttype 16
                :constlen 1
                :constbyval true
                :constisnull false
                :constvalue 1 [ 0 0 0 0 0 0 0 0 ]
                }
              )
              }
            }
          )
          :non_null_target (b)
          :qual <>
          :lefttree <>
          :righttree <>
          :initPlan <>
          :extParam (b)
          :allParam (b)
          :nParamExec 0
          :scanrelid 1
          :tuples 0
          :subplan
            { RESULT
            :startup_cost 0.00
            :total_cost 0.01
            :plan_rows 1
            :node_id 6
            :parent_id 5
            :plan_width 0
            :best_pathkeys <>
            :dist_info.dist_strategy DS_DIST_NONE
            :dist_info.dist_keys <>
            :dist_info.seqscan_is_distributed false
            :dist_info.topology 1966505242
            :phys_properties <>
            :reqd_phys_properties <>
            :targetlist (
              { TARGETENTRY
              :resdom
                { RESDOM
                :resno 1
                :restype 1043
                :restypmod 204
                :resname cola
                :ressortgroupref 0
                :resorigtbl 0
                :resorigcol 0
                :resjunk false
                }
              :expr
                { CONST
                :consttype 1043
                :constlen -1
                :constbyval false
                :constisnull false
                :constvalue 5 [ 5 0 0 0 49 ]
                }
              }
            )
            :non_null_target (b)
            :qual <>
            :lefttree <>
            :righttree <>
            :initPlan <>
            :extParam (b)
            :allParam (b)
            :nParamExec 0
            :resconstantqual <>
            :scanrelid 0
            :values_lists <>
            }
          }
        :righttree <>
        :initPlan <>
        :extParam (b)
        :allParam (b)
        :nParamExec 0
        :dataMovement Distribute
        }
        { NETWORK
        :startup_cost 0.00
        :total_cost 0.02
        :plan_rows 1
        :node_id 7
        :parent_id 3
        :plan_width 0
        :best_pathkeys <>
        :dist_info.dist_strategy DS_DIST_ERR
        :dist_info.dist_keys <>
        :dist_info.seqscan_is_distributed false
        :dist_info.topology 1966505242
        :phys_properties <>
        :reqd_phys_properties <>
        :targetlist (
          { TARGETENTRY
          :resdom
            { RESDOM
            :resno 1
            :restype 1043
            :restypmod 5
            :resname cola
            :ressortgroupref 0
            :resorigtbl 0
            :resorigcol 0
            :resjunk false
            }
          :expr
            { CONST
            :consttype 1043
            :constlen -1
            :constbyval false
            :constisnull false
            :constvalue 5 [ 5 0 0 0 51 ]
            }
          }
        )
        :non_null_target (b)
        :qual <>
        :lefttree
          { SUBQUERYSCAN
          :startup_cost 0.00
          :total_cost 0.02
          :plan_rows 1
          :node_id 8
          :parent_id 7
          :plan_width 0
          :best_pathkeys <>
          :dist_info.dist_strategy DS_DIST_ERR
          :dist_info.dist_keys <>
          :dist_info.seqscan_is_distributed false
          :dist_info.topology 1966505242
          :phys_properties <>
          :reqd_phys_properties <>
          :targetlist (
            { TARGETENTRY
            :resdom
              { RESDOM
              :resno 1
              :restype 1043
              :restypmod 5
              :resname cola
              :ressortgroupref 0
              :resorigtbl 0
              :resorigcol 0
              :resjunk false
              }
            :expr
              { FUNCEXPR
              :funcid 669
              :funcresulttype 1043
              :funcretset false
              :funcformat 2
              :args (
                { CONST
                :consttype 1043
                :constlen -1
                :constbyval false
                :constisnull false
                :constvalue 5 [ 5 0 0 0 51 ]
                }
                { CONST
                :consttype 23
                :constlen 4
                :constbyval true
                :constisnull false
                :constvalue 4 [ 5 0 0 0 0 0 0 0 ]
                }
                { CONST
                :consttype 16
                :constlen 1
                :constbyval true
                :constisnull false
                :constvalue 1 [ 0 0 0 0 0 0 0 0 ]
                }
              )
              }
            }
          )
          :non_null_target (b)
          :qual <>
          :lefttree <>
          :righttree <>
          :initPlan <>
          :extParam (b)
          :allParam (b)
          :nParamExec 0
          :scanrelid 2
          :tuples 0
          :subplan
            { RESULT
            :startup_cost 0.00
            :total_cost 0.01
            :plan_rows 1
            :node_id 9
            :parent_id 8
            :plan_width 0
            :best_pathkeys <>
            :dist_info.dist_strategy DS_DIST_NONE
            :dist_info.dist_keys <>
            :dist_info.seqscan_is_distributed false
            :dist_info.topology 1966505242
            :phys_properties <>
            :reqd_phys_properties <>
            :targetlist (
              { TARGETENTRY
              :resdom
                { RESDOM
                :resno 1
                :restype 1043
                :restypmod 204
                :resname varchar
                :ressortgroupref 0
                :resorigtbl 0
                :resorigcol 0
                :resjunk false
                }
              :expr
                { CONST
                :consttype 1043
                :constlen -1
                :constbyval false
                :constisnull false
                :constvalue 5 [ 5 0 0 0 51 ]
                }
              }
            )
            :non_null_target (b)
            :qual <>
            :lefttree <>
            :righttree <>
            :initPlan <>
            :extParam (b)
            :allParam (b)
            :nParamExec 0
            :resconstantqual <>
            :scanrelid 0
            :values_lists <>
            }
          }
        :righttree <>
        :initPlan <>
        :extParam (b)
        :allParam (b)
        :nParamExec 0
        :dataMovement Distribute
        }
      )
      :isTarget false
      :streamable false
      }
    }
  :righttree <>
  :initPlan <>
  :extParam (b)
  :allParam (b)
  :nParamExec 0
  :resconstantqual (
    { CONST
    :consttype 16
    :constlen 1
    :constbyval true
    :constisnull false
    :constvalue 1 [ 0 0 0 0 0 0 0 0 ]
    }
    { CONST
    :consttype 16
    :constlen 1
    :constbyval true
    :constisnull false
    :constvalue 1 [ 0 0 0 0 0 0 0 0 ]
    }
  )
  :scanrelid 0
  :values_lists <>
  }
    """
    )
    assert plan["targetlist"] == [
        {
            "expr": {
                "args": [
                    {
                        "arg": {
                            "name": "VAR",
                            "varattno": "1",
                            "varlevelsup": "0",
                            "varno": "65001",
                            "varnoold": "2",
                            "varoattno": "1",
                            "vartype": "1043",
                            "vartypmod": "- 1",
                        },
                        "name": "RELABELTYPE",
                        "relabelformat": "0",
                        "resulttype": "25",
                        "resulttypmod": "- 1",
                    }
                ],
                "funcformat": "0",
                "funcid": "2311",
                "funcresulttype": "25",
                "funcretset": "false",
                "name": "FUNCEXPR",
            },
            "name": "TARGETENTRY",
            "resdom": {
                "name": "RESDOM",
                "resjunk": "false",
                "resname": "cola",
                "resno": "1",
                "resorigcol": "0",
                "resorigtbl": "0",
                "ressortgroupref": "0",
                "restype": "25",
                "restypmod": "36",
            },
        },
    ]


def test_parse_plan_limit():
    plan = parse_plan(
        """
{ LIMIT
  :startup_cost 0.00
  :total_cost 0.00
  :plan_rows 1
  :node_id 1
  :parent_id 0
  :plan_width 0
  :best_pathkeys <>
  :phys_properties <>
  :reqd_phys_properties <>
  :targetlist (
    { TARGETENTRY
    :resdom
      { RESDOM
      :resno 1
      :restype 20
      :restypmod -1
      :resname ?column?
      :ressortgroupref 0
      :resorigtbl 0
      :resorigcol 0
      :resjunk false
      }
    :expr
      { CONST
      :consttype 20
      :constlen 8
      :constbyval true
      :constisnull true
      :constvalue <>
      }
    }
  )
  }

XN Limit  (cost=0.00..0.00 rows=1 width=0)
  ->  XN Result  (cost=0.00..0.01 rows=1 width=0)
        One-Time Filter: false

  { RESULT
  :targetlist (
    { TARGETENTRY
    :resdom
      { RESDOM
      :resno 1
      :restype 1043
      :restypmod 36
      :resname surrogate_key
      :ressortgroupref 0
      :resorigtbl 0
      :resorigcol 0
      :resjunk false
      }
    :expr
      { VAR
      :varno 65001
      :varattno 1
      :vartype 1043
      :vartypmod 36
      :varlevelsup 0
      :varnoold 1
      :varoattno 1
      }
    }
  )
  :scanrelid 0
  :values_lists <>
  }
        """
    )

    assert plan == {
        "name": "RESULT",
        "scanrelid": "0",
        "targetlist": [
            {
                "expr": {
                    "name": "VAR",
                    "varattno": "1",
                    "varlevelsup": "0",
                    "varno": "65001",
                    "varnoold": "1",
                    "varoattno": "1",
                    "vartype": "1043",
                    "vartypmod": "36",
                },
                "name": "TARGETENTRY",
                "resdom": {
                    "name": "RESDOM",
                    "resjunk": "false",
                    "resname": "surrogate_key",
                    "resno": "1",
                    "resorigcol": "0",
                    "resorigtbl": "0",
                    "ressortgroupref": "0",
                    "restype": "1043",
                    "restypmod": "36",
                },
            }
        ],
        "values_lists": "<>",
    }
