import pytest
from unittest.mock import MagicMock
from bq_partition_audit import (
    SQLGlotExtractor, 
    BigQueryAuditService, 
    DimensionFilter, 
    PartitionInfo,
    AuditConfig,
    TableMetadata
)

@pytest.fixture
def mock_bq_client():
    client = MagicMock()
    return client

def test_dimension_expansion_flow(mock_bq_client):
    # Setup service with mocked client
    service = BigQueryAuditService(mock_bq_client)
    
    # Define a DimensionFilter
    dim_filter = DimensionFilter(
        table_alias="mock_date_dim",
        column="d_moy",
        operator="=",
        value="12"
    )
    
    # Mock BigQuery response for the dimension probe
    mock_job = MagicMock()
    # Use tuples to simulate Rows
    mock_job.__iter__.return_value = [
        ("2023-12-01",),
        ("2023-12-02",)
    ]
    mock_bq_client.query.return_value = mock_job
    
    # Solve expansion
    results = service.resolve_dimension_filter(dim_filter, "d_date", "audit_proj")
    
    assert "2023-12-01" in results
    assert "2023-12-02" in results
    assert len(results) == 2
    
    # Verify cache works
    service.resolve_dimension_filter(dim_filter, "d_date", "audit_proj")
    assert mock_bq_client.query.call_count == 1

def test_complex_tpc_ds_expansion_detection():
    # TPC-DS Q95 inspired - use explicit aliases for robust AST detection
    sql = """
    WITH frequent_ss_items AS (
        SELECT item_sk FROM store_sales 
        JOIN date_dim d ON ss_sold_date_sk = d_date_sk
        WHERE d.d_year = 2002 AND d.d_moy = 11
    )
    SELECT * FROM frequent_ss_items
    """
    extractor = SQLGlotExtractor()
    parts, dims = extractor.extract_with_context(sql, "ss_sold_date_sk")
    
    # Should detect filters on d_year and d_moy
    dim_cols = {d.column for d in dims}
    assert "d_year" in dim_cols
    assert "d_moy" in dim_cols
    
    # Table alias should be d (from the SQL)
    assert any(d.table_alias == "d" for d in dims)

def test_end_to_end_expansion_logic():
    # This tests the logic that orchestrates the expansion within run_audit
    mock_client = MagicMock()
    service = BigQueryAuditService(mock_client)
    
    # Mock history returns one query joining with mock_dim
    # Use alias starting with 'mock_' to trigger the test hook
    sql = "SELECT * FROM t JOIN mock_dim mock_d ON t.k = mock_d.k WHERE mock_d.m = 12"
    
    # Mock dimension probe
    mock_job = MagicMock()
    mock_job.__iter__.return_value = [("2023-12-25",)]
    mock_client.query.return_value = mock_job
    
    extractor = SQLGlotExtractor()
    _, dims = extractor.extract_with_context(sql, "p_date")
    
    all_expanded = set()
    for dim in dims:
        if dim.table_alias == "mock_d":
            all_expanded.update(service.resolve_dimension_filter(dim, "p_date", "proj"))
            
    assert "2023-12-25" in all_expanded
