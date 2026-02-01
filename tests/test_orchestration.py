from unittest.mock import MagicMock, patch
from bq_partition_audit import AuditConfig, run_audit, TableMetadata, PartitionInfo, parse_job_batch

def test_run_audit_no_partitions():
    client = MagicMock()
    mock_table = MagicMock()
    mock_table.time_partitioning = None
    mock_table.range_partitioning = None
    client.get_table.return_value = mock_table
    
    config = AuditConfig(audit_project="p_audit", target_table_ref="p.d.t")
    
    with patch("logging.warning") as mock_log:
        run_audit(config, client)
        mock_log.assert_any_call("Table p.d.t is not partitioned. Aborting.")

def test_run_audit_full_flow():
    client = MagicMock()
    
    # 1. Mock Metadata
    mock_table = MagicMock()
    mock_table.time_partitioning.field = "c"
    mock_table.time_partitioning.type_ = "DAY"
    mock_table.range_partitioning = None
    client.get_table.return_value = mock_table
    
    # 2. Mock History Job
    mock_job = MagicMock()
    mock_job.job_id = "hist_query_job"
    row = MagicMock(job_id="q1", query="SELECT * FROM fact WHERE c = '2023-01-01'")
    mock_job.result.return_value = [row]
    client.query.return_value = mock_job
    
    # 3. Mock ProcessPoolExecutor to return results
    mock_future = MagicMock()
    p_info = PartitionInfo(value="2023-01-01", context="from tests", normalized_id="20230101")
    mock_future.result.return_value = ({p_info}, [])
    
    with patch("concurrent.futures.ProcessPoolExecutor") as mock_executor:
        mock_pool = mock_executor.return_value.__enter__.return_value
        mock_pool.submit.return_value = mock_future
        
        # We need as_completed to yield our mock_future
        with patch("concurrent.futures.as_completed") as mock_as_completed:
            mock_as_completed.return_value = [mock_future]
            
            config = AuditConfig(audit_project="p_audit", target_table_ref="p.d.t", parallelism=1)
            run_audit(config, client)
            
    # Verify metadata was fetched
    client.get_table.assert_called_once()

def test_run_audit_empty_history():
    client = MagicMock()
    mock_table = MagicMock()
    mock_table.time_partitioning.field = "c"
    mock_table.time_partitioning.type_ = "DAY"
    mock_table.range_partitioning = None
    client.get_table.return_value = mock_table
    
    mock_job = MagicMock()
    mock_job.job_id = "empty_hist"
    mock_job.result.return_value = []
    client.query.return_value = mock_job
    
    config = AuditConfig(audit_project="p_audit", target_table_ref="p.d.t")
    run_audit(config, client)

def test_parse_job_batch_direct_call():
    # Direct test of the parse_job_batch helper to hit its specific lines
    batch = [("job1", "SELECT * FROM fact WHERE c = '2023-10-10'")]
    p_info, d_info = parse_job_batch(batch, "c")
    assert len(p_info) == 1
    assert any(p.normalized_id == "20231010" for p in p_info)
