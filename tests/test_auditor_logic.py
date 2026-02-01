import pytest
from bq_partition_audit import SQLGlotExtractor, PartitionInfo

def test_basic_equality():
    extractor = SQLGlotExtractor()
    sql = "SELECT * FROM tbl WHERE _PARTITIONDATE = '2023-01-01'"
    results = extractor.extract(sql, "_PARTITIONDATE")
    assert any(p.normalized_id == "20230101" for p in results)

def test_join_transitive_pruning():
    extractor = SQLGlotExtractor()
    sql = """
    SELECT * 
    FROM fact f
    JOIN dim d ON f.key = d.key
    WHERE d.date_col = '2023-05-05'
    """
    results = extractor.extract(sql, "_PARTITIONDATE")
    # Should find 20230505 from dim
    assert any(p.normalized_id == "20230505" for p in results)

def test_query_95_complexity():
    from .extreme_tpcds_test import QUERY_95
    extractor = SQLGlotExtractor()
    results = extractor.extract(QUERY_95, "_PARTITIONDATE")
    
    # Expected from QUERY_95:
    # 20010501 (from ws1._PARTITIONDATE, ws2._PARTITIONDATE, web_sales_p)
    # 20010401 (from d.d_date BETWEEN range start)
    # 20010430 (from d.d_date BETWEEN range end)
    
    normalized_ids = {p.normalized_id for p in results}
    assert "20010501" in normalized_ids
    assert "20010401" in normalized_ids
    assert "20010430" in normalized_ids

def test_integer_range_partition():
    extractor = SQLGlotExtractor()
    sql = "SELECT * FROM tbl WHERE range_col BETWEEN 100 AND 200"
    results = extractor.extract(sql, "range_col")
    normalized_ids = {p.normalized_id for p in results}
    assert "100" in normalized_ids
    assert "200" in normalized_ids

def test_dimension_filter_detection():
    extractor = SQLGlotExtractor()
    sql = """
    SELECT * 
    FROM fact f
    JOIN dim d ON f.key = d.key
    WHERE d.month = '2023-10'
    """
    _, dim_filters = extractor.extract_with_context(sql, "_PARTITIONDATE")
    
    # Should detect the filter on d.month
    dim_map = {f.table_alias: f for f in dim_filters}
    assert "d" in dim_map
    assert dim_map["d"].column == "month"
    assert dim_map["d"].value == "2023-10"

def test_extract_in_expressions():
    extractor = SQLGlotExtractor()
    sql = "SELECT * FROM tbl WHERE c IN ('2023-01-01', '2023-01-02')"
    results = extractor.extract(sql, "c")
    ids = {p.normalized_id for p in results}
    assert "20230101" in ids
    assert "20230102" in ids

def test_partition_info_eq_and_hash():
    p1 = PartitionInfo(value="v", context="c", normalized_id="n")
    p2 = PartitionInfo(value="v", context="other", normalized_id="n")
    p3 = PartitionInfo(value="x", context="c", normalized_id="x")
    
    assert p1 == p2
    assert p1 != p3
    assert p1 != "not a partition info"
    assert hash(p1) == hash(p2)

def test_add_lit_filtering():
    extractor = SQLGlotExtractor()
    # Test short values or non-alphanumeric that should be filtered out
    results = extractor.extract("SELECT * FROM t WHERE c = 'a'", "c")
    assert len(results) == 0
    
    # Test longer valid-looking strings
    results = extractor.extract("SELECT * FROM t WHERE c = 'partition123'", "c")
    assert len(results) > 0
