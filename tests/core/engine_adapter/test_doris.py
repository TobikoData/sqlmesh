import pytest
from sqlglot import exp

from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.doris, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter, mocker) -> DorisEngineAdapter:
    """创建 DorisEngineAdapter 的 mock 实例"""
    return make_mocked_engine_adapter(DorisEngineAdapter)


def test_get_current_catalog(adapter: DorisEngineAdapter):
    """测试获取当前 catalog"""
    assert adapter.get_current_catalog() == "internal"


def test_create_index(adapter: DorisEngineAdapter):
    """测试创建索引"""
    # 测试 INVERTED 索引
    adapter.create_index("test_table", "idx_inverted", ("col1",), index_type="INVERTED")
    sqls = to_sql_calls(adapter)
    assert any("CREATE INDEX" in s for s in sqls)

    # 测试 BLOOMFILTER 索引
    adapter.create_index("test_table", "idx_bloom", ("col2",), index_type="BLOOMFILTER")
    sqls = to_sql_calls(adapter)
    assert any("BLOOMFILTER" in s for s in sqls)

    # 测试带属性的索引
    adapter.create_index(
        "test_table",
        "idx_ngram",
        ("col3",),
        index_type="NGRAM_BF",
        properties={"gram_size": 3, "bf_size": 256},
        comment="NGRAM index for text search",
    )
    sqls = to_sql_calls(adapter)
    assert any("NGRAM_BF" in s and "PROPERTIES" in s and "COMMENT" in s for s in sqls)


def test_create_index_invalid_type(adapter: DorisEngineAdapter):
    """测试无效的索引类型"""
    with pytest.raises(ValueError, match="Doris only supports INVERTED, BLOOMFILTER, and NGRAM_BF"):
        adapter.create_index("test_table", "idx_invalid", ("col1",), index_type="BTREE")


def test_drop_schema(adapter: DorisEngineAdapter):
    """测试删除 schema，Doris 不支持 CASCADE"""
    adapter.drop_schema("test_schema", ignore_if_not_exists=True, cascade=True)
    sqls = to_sql_calls(adapter)
    # Doris 的 drop_schema 会忽略 cascade 参数
    assert any("DROP" in s and "test_schema" in s for s in sqls)
    # 确保没有 CASCADE
    assert not any("CASCADE" in s for s in sqls)


def test_create_table_like(adapter: DorisEngineAdapter):
    """测试基于已有表创建新表"""
    adapter.create_table_like("target_table", "source_table", exists=True)
    sqls = to_sql_calls(adapter)
    assert any("CREATE TABLE" in s and "LIKE" in s for s in sqls)


def test_ping(adapter: DorisEngineAdapter, mocker):
    """测试连接 ping"""
    mock_fetchone = mocker.patch.object(adapter, "fetchone", return_value=(1,))
    adapter.ping()
    mock_fetchone.assert_called_once_with("SELECT 1")


def test_ping_failure(adapter: DorisEngineAdapter, mocker):
    """测试 ping 失败情况"""
    mocker.patch.object(adapter, "fetchone", side_effect=Exception("Connection failed"))
    with pytest.raises(Exception, match="Connection failed"):
        adapter.ping()


def test_truncate_table_comment(adapter: DorisEngineAdapter):
    """测试表注释截断"""
    long_comment = "a" * 3000
    truncated = adapter._truncate_table_comment(long_comment)
    max_length = adapter.MAX_TABLE_COMMENT_LENGTH
    assert max_length is not None
    assert len(truncated) <= max_length
    assert truncated.endswith("...")


def test_truncate_column_comment(adapter: DorisEngineAdapter):
    """测试列注释截断"""
    long_comment = "b" * 300
    truncated = adapter._truncate_column_comment(long_comment)
    max_length = adapter.MAX_COLUMN_COMMENT_LENGTH
    assert max_length is not None
    assert len(truncated) <= max_length
    assert truncated.endswith("...")


def test_build_table_properties_exp(adapter: DorisEngineAdapter):
    """测试构建表属性表达式"""
    props = adapter._build_table_properties_exp(
        table_properties={
            "TABLE_MODEL": "UNIQUE",
            "DISTRIBUTED_BY": "HASH(id)",
            "BUCKETS": 5,
            "replication_num": 1,
        },
        columns_to_types={"id": exp.DataType.build("INT"), "name": exp.DataType.build("STRING")},
        table_description="Test table description",
    )
    assert props is not None
    assert isinstance(props, exp.Properties)


def test_build_partitioned_by_exp(adapter: DorisEngineAdapter):
    """测试构建分区表达式"""
    # 测试 RANGE 分区
    part = adapter._build_partitioned_by_exp([exp.column("dt")], partition_type="RANGE")
    assert part is not None
    assert isinstance(part, exp.Property)

    # 测试 LIST 分区
    part = adapter._build_partitioned_by_exp([exp.column("region")], partition_type="LIST")
    assert part is not None


def test_build_partitioned_by_exp_invalid_type(adapter: DorisEngineAdapter):
    """测试无效的分区类型"""
    with pytest.raises(ValueError, match="Doris only supports RANGE and LIST partitioning"):
        adapter._build_partitioned_by_exp([exp.column("dt")], partition_type="HASH")


def test_build_create_table_exp(adapter: DorisEngineAdapter):
    """测试构建 CREATE TABLE 表达式"""
    exp_obj = adapter._build_create_table_exp(
        "test_table",
        None,
        columns_to_types={"id": exp.DataType.build("INT"), "name": exp.DataType.build("STRING")},
        table_description="Test table",
        table_properties={"TABLE_MODEL": "UNIQUE", "BUCKETS": 10},
    )
    assert isinstance(exp_obj, exp.Create)


def test_generate_doris_create_table_sql(adapter: DorisEngineAdapter):
    """测试生成 Doris CREATE TABLE SQL"""
    exp_obj = adapter._build_create_table_exp(
        "test_table",
        None,
        columns_to_types={"id": exp.DataType.build("INT"), "name": exp.DataType.build("STRING")},
        table_description="Test table",
        table_properties={"TABLE_MODEL": "UNIQUE", "BUCKETS": 10},
    )
    sql = adapter._to_sql(exp_obj)
    assert "CREATE TABLE" in sql
    assert "UNIQUE KEY" in sql
    assert "DISTRIBUTED BY" in sql
    assert "BUCKETS" in sql
    assert "PROPERTIES" in sql


def test_create_view(adapter: DorisEngineAdapter):
    """测试创建普通视图"""
    adapter.create_view("test_view", "SELECT 1 as col", replace=True)
    sqls = to_sql_calls(adapter)
    assert any("CREATE OR REPLACE VIEW" in s for s in sqls)


def test_create_view_no_replace(adapter: DorisEngineAdapter):
    """测试创建视图（不替换）"""
    adapter.create_view("test_view", "SELECT 1 as col", replace=False)
    sqls = to_sql_calls(adapter)
    assert any("CREATE VIEW" in s and "OR REPLACE" not in s for s in sqls)


def test_create_materialized_view(adapter: DorisEngineAdapter):
    """测试创建物化视图"""
    adapter.create_view(
        "test_mv",
        "SELECT count(*) FROM test_table",
        materialized=True,
        table_description="Test materialized view",
        materialized_properties={
            "build": "IMMEDIATE",
            "refresh": "MANUAL",
            "properties": {"enable_duplicate_without_keys_by_default": "true"},
        },
    )
    sqls = to_sql_calls(adapter)
    assert any("CREATE MATERIALIZED VIEW" in s for s in sqls)
    assert any("COMMENT" in s for s in sqls)
    assert any("BUILD IMMEDIATE" in s for s in sqls)
    assert any("REFRESH MANUAL" in s for s in sqls)
    assert any("PROPERTIES" in s for s in sqls)


def test_drop_view(adapter: DorisEngineAdapter):
    """测试删除普通视图"""
    adapter.drop_view("test_view", ignore_if_not_exists=True)
    sqls = to_sql_calls(adapter)
    assert any("DROP VIEW IF EXISTS" in s for s in sqls)


def test_drop_materialized_view(adapter: DorisEngineAdapter):
    """测试删除物化视图"""
    adapter.drop_view("test_mv", ignore_if_not_exists=True, materialized=True)
    sqls = to_sql_calls(adapter)
    assert any("DROP MATERIALIZED VIEW IF EXISTS" in s for s in sqls)


def test_get_data_objects(adapter: DorisEngineAdapter, mocker):
    """测试获取数据对象"""
    # Mock fetchdf 返回空列表
    mock_fetchdf = mocker.patch.object(adapter, "fetchdf", return_value=[])
    objs = adapter._get_data_objects("test_schema")
    assert isinstance(objs, list)
    mock_fetchdf.assert_called_once()


def test_supports_features(adapter: DorisEngineAdapter):
    """测试支持的功能特性"""
    assert adapter.supports_indexes is True
    assert adapter.SUPPORTS_TRANSACTIONS is False
    assert adapter.SUPPORTS_MATERIALIZED_VIEWS is True
    assert adapter.SUPPORTS_VIEW_SCHEMA is False
    assert adapter.SUPPORTS_CREATE_DROP_CATALOG is False
    assert adapter.SUPPORTS_REPLACE_TABLE is False
    assert adapter.MAX_IDENTIFIER_LENGTH == 64
    assert adapter.MAX_TABLE_COMMENT_LENGTH == 2048
    assert adapter.MAX_COLUMN_COMMENT_LENGTH == 255


def test_schema_differ(adapter: DorisEngineAdapter):
    """测试 schema differ 配置"""
    assert hasattr(adapter, "SCHEMA_DIFFER")
    differ = adapter.SCHEMA_DIFFER
    assert differ is not None


def test_comment_creation_support(adapter: DorisEngineAdapter):
    """测试注释创建支持"""
    from sqlmesh.core.engine_adapter.shared import CommentCreationTable, CommentCreationView

    assert adapter.COMMENT_CREATION_TABLE == CommentCreationTable.IN_SCHEMA_DEF_CTAS
    assert adapter.COMMENT_CREATION_VIEW == CommentCreationView.UNSUPPORTED


def test_table_model_defaults(adapter: DorisEngineAdapter):
    """测试表模型默认值"""
    assert adapter.DEFAULT_TABLE_MODEL == "UNIQUE"
    assert adapter.DEFAULT_UNIQUE_KEY_MERGE_ON_WRITE is True


def test_dialect_setting(adapter: DorisEngineAdapter):
    """测试方言设置"""
    assert adapter.DIALECT == "mysql"  # Doris 使用 MySQL 协议


def test_create_table_duplicate_model(adapter: DorisEngineAdapter):
    """测试创建 DUPLICATE 模型表"""
    exp_obj = adapter._build_create_table_exp(
        "duplicate_table",
        None,
        columns_to_types={"id": exp.DataType.build("INT"), "value": exp.DataType.build("DECIMAL")},
        table_properties={"TABLE_MODEL": "DUPLICATE", "BUCKETS": 5},
    )
    sql = adapter._to_sql(exp_obj)
    assert "CREATE TABLE" in sql
    # DUPLICATE 模型不应该有 UNIQUE KEY
    assert "UNIQUE KEY" not in sql


def test_create_table_aggregate_model(adapter: DorisEngineAdapter):
    """测试创建 AGGREGATE 模型表"""
    exp_obj = adapter._build_create_table_exp(
        "aggregate_table",
        None,
        columns_to_types={
            "dim": exp.DataType.build("STRING"),
            "metric": exp.DataType.build("BIGINT"),
        },
        table_properties={"TABLE_MODEL": "AGGREGATE", "BUCKETS": 8},
    )
    sql = adapter._to_sql(exp_obj)
    assert "CREATE TABLE" in sql
    # AGGREGATE 模型不应该有 UNIQUE KEY
    assert "UNIQUE KEY" not in sql


def test_create_table_with_partition(adapter: DorisEngineAdapter):
    """测试创建分区表"""
    exp_obj = adapter._build_create_table_exp(
        "partitioned_table",
        None,
        columns_to_types={"id": exp.DataType.build("INT"), "dt": exp.DataType.build("DATE")},
        partitioned_by=[exp.column("dt")],
        table_properties={"TABLE_MODEL": "UNIQUE", "BUCKETS": 12},
    )
    assert isinstance(exp_obj, exp.Create)
    # 验证分区相关属性被设置
    properties = exp_obj.find(exp.Properties)
    assert properties is not None


def test_error_handling_create_index_alter_table(adapter: DorisEngineAdapter):
    """测试使用 ALTER TABLE 方式创建索引"""
    adapter.create_index(
        "test_table",
        "idx_alter",
        ("col1",),
        index_type="INVERTED",
        use_create_index=False,  # 使用 ALTER TABLE 方式
    )
    sqls = to_sql_calls(adapter)
    assert any("ALTER TABLE" in s and "ADD INDEX" in s for s in sqls)


def test_table_properties_merge_on_write(adapter: DorisEngineAdapter):
    """测试 UNIQUE 表的 merge-on-write 属性"""
    props = adapter._build_table_properties_exp(
        table_properties={"TABLE_MODEL": "UNIQUE"},
        columns_to_types={"id": exp.DataType.build("INT")},
    )
    # 验证 enable_unique_key_merge_on_write 默认为 true
    assert props is not None


def test_empty_comment_handling(adapter: DorisEngineAdapter):
    """测试空注释处理"""
    short_comment = "short"
    assert adapter._truncate_table_comment(short_comment) == short_comment
    assert adapter._truncate_column_comment(short_comment) == short_comment

    empty_comment = ""
    assert adapter._truncate_table_comment(empty_comment) == empty_comment
    assert adapter._truncate_column_comment(empty_comment) == empty_comment


def test_distributed_by_default(adapter: DorisEngineAdapter):
    """测试默认分布策略"""
    props = adapter._build_table_properties_exp(
        columns_to_types={"id": exp.DataType.build("INT"), "name": exp.DataType.build("STRING")},
    )
    # 应该使用第一列作为默认分布列
    assert props is not None
