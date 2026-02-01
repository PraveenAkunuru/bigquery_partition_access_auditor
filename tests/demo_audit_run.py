import logging
import sys
import os
from unittest.mock import MagicMock

# Add project root to path
sys.path.append(os.getcwd())

from bq_partition_audit import (
    AuditConfig, 
    run_audit, 
    setup_logging,
    TableMetadata
)

def create_mock_client():
    client = MagicMock()
    
    # 1. Mock fetch_table_metadata
    mock_table = MagicMock()
    mock_table.time_partitioning.field = "p_date"
    mock_table.time_partitioning.type_ = "DAY"
    mock_table.range_partitioning = None
    client.get_table.return_value = mock_table
    
    # 2. Mock stream_job_history
    # We return a mock job for the history fetch
    mock_history_job = MagicMock()
    mock_history_job.job_id = "history_fetch_job_001"
    
    # Define row objects for the history result
    # Query 1: Direct partition filter
    # Query 2: Dimension filter (for expansion)
    mock_row1 = MagicMock(job_id="user_query_alpha", query="SELECT * FROM fact WHERE p_date = '2023-10-01'")
    mock_row2 = MagicMock(job_id="user_query_beta", query="SELECT * FROM fact JOIN mock_dim d ON fact.k = d.k WHERE d.m = 'DEC-2023'")
    
    mock_history_job.result.return_value = [mock_row1, mock_row2]
    
    # The client.query() call needs to return different jobs based on the SQL
    def side_effect(sql):
        if "INFORMATION_SCHEMA.JOBS" in sql:
            return mock_history_job
        if "mock_dim" in sql:
            mock_probe_job = MagicMock()
            mock_probe_job.job_id = "dim_probe_job_999"
            mock_probe_job.__iter__.return_value = [("2023-12-25",)]
            return mock_probe_job
        return MagicMock()

    client.query.side_effect = side_effect
    return client

def run_demo(name, expand=False, verbose=False):
    print(f"\n{'='*20} DEMO: {name} {'='*20}")
    setup_logging("DEBUG" if verbose else "INFO")
    
    config = AuditConfig(
        audit_project="demo-project",
        target_table_ref="demo-project.my_dataset.my_table",
        expand_dimensions=expand,
        parallelism=1 # Low parallelism for clean log sequence
    )
    
    client = create_mock_client()
    run_audit(config, client)

if __name__ == "__main__":
    # 1. Default Mode
    run_demo("DEFAULT (INFO)", expand=False, verbose=False)
    
    # 2. Verbose Mode
    run_demo("VERBOSE (DEBUG)", expand=False, verbose=True)
    
    # 3. Expansion Mode (INFO)
    run_demo("EXPANSION (INFO)", expand=True, verbose=False)
    
    # 4. Full Mode (DEBUG + EXPANSION)
    run_demo("FULL (DEBUG + EXPANSION)", expand=True, verbose=True)
