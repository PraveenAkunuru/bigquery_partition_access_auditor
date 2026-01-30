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
    from extreme_tpcds_test import QUERY_95
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
