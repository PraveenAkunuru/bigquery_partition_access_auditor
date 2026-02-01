from unittest.mock import MagicMock
from bq_partition_audit import BigQueryAuditService, TableMetadata

def test_fetch_table_metadata():
    client = MagicMock()
    mock_table = MagicMock()
    
    # Simulate time partitioning
    mock_table.time_partitioning.field = "p_date"
    mock_table.time_partitioning.type_ = "DAY"
    mock_table.range_partitioning = None
    client.get_table.return_value = mock_table
    
    service = BigQueryAuditService(client)
    meta = service.fetch_table_metadata("p", "d", "t")
    
    assert meta.project == "p"
    assert meta.partition_column == "p_date"
    assert meta.partition_type == "DAY"
    client.get_table.assert_called_once_with("p.d.t")

def test_fetch_range_partition_metadata():
    client = MagicMock()
    mock_table = MagicMock()
    
    # Simulate range partitioning
    mock_table.time_partitioning = None
    mock_table.range_partitioning.field = "r_col"
    client.get_table.return_value = mock_table
    
    service = BigQueryAuditService(client)
    meta = service.fetch_table_metadata("p", "d", "t")
    
    assert meta.partition_column == "r_col"
    assert meta.partition_type == "INTEGER_RANGE"

def test_stream_job_history():
    client = MagicMock()
    mock_job = MagicMock()
    mock_row = MagicMock()
    mock_row.job_id = "job123"
    mock_row.query = "SELECT 1"
    
    mock_job.job_id = "fetch_history_job"
    mock_job.result.return_value = [mock_row]
    client.query.return_value = mock_job
    
    service = BigQueryAuditService(client)
    target = TableMetadata(project="p", dataset="d", table="t", partition_column="c", partition_type="type")
    
    history = list(service.stream_job_history("audit_p", target, 7))
    
    assert len(history) == 1
    assert history[0] == ("job123", "SELECT 1")
    client.query.assert_called_once()
    assert "INFORMATION_SCHEMA.JOBS" in client.query.call_args[0][0]
