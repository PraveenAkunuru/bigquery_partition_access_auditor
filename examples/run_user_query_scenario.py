
import logging
import os
import sys
from google.cloud import bigquery
from bq_partition_audit import BigQueryAuditService, TableMetadata, DimensionFilter, SQLGlotExtractor

# Setup Logging
from bq_partition_audit import setup_logging
setup_logging(level="INFO")

def run_verify():
    project = "pakunuru-1119-20250930202256"
    client = bigquery.Client(project=project)
    
    # 1. Check if tables exist
    try:
        client.get_table("bigquerybench.tpcds_100G.store_sales")
        client.get_table("bigquerybench.tpcds_100G.date_dim")
        logging.info("Validation: TPC-DS tables exist.")
    except Exception as e:
        logging.error(f"Tables missing: {e}")
        return

    # 2. Extract Logic Test
    user_sql = """
    WITH frequent_ss_items AS (
        SELECT item_sk FROM `bigquerybench.tpcds_100G.store_sales`
        JOIN `bigquerybench.tpcds_100G.date_dim` d ON ss_sold_date_sk = d_date_sk
        WHERE d.d_year = 2002 AND d.d_moy = 11
    )
    SELECT * FROM frequent_ss_items
    """
    
    extractor = SQLGlotExtractor()
    parts, dims = extractor.extract_with_context(user_sql, "ss_sold_date_sk") # Fact join key
    
    logging.info(f"Extraction Results: {len(dims)} dimension filters.")
    for dim in dims:
        logging.info(f"Found Filter: {dim}")
        
    if not dims:
        logging.error("Failed to extract dimension filters from user query!")
        return

    # 3. Resolve Test
    service = BigQueryAuditService(client=client)
    # We need to simulate the audit process
    for dim in dims:
        logging.info(f"Attempting to resolve {dim.table_alias}.{dim.column}...")
        # Note: 'ss_sold_date_sk' is not the join key relative to date_dim (which is d_date_sk),
        # but for the PROBE we want to transform valid d_date_sk values back to ss_sold_date_sk? 
        # Actually, in the audit logic: "fact_join_key" is the column in the FACT table (ss_sold_date_sk).
        # But we probe the DIMENSION table.
        # So we want distinct 'd_date_sk' from date_dim where d_year=2002.
        # Wait, the code in resolve_dimension_filter uses `SELECT DISTINCT {fact_join_key} FROM ...`
        # IF fact_join_key (e.g. ss_sold_date_sk) doesn't exist in date_dim, the probe fails.
        # Implementation Detail:
        # In the real code, we pass "fact_join_key".
        # BUT usually the join condition is fact.key = dim.key.
        # If the columns have DIFFERENT names, `resolve_dimension_filter` doing `SELECT DISTINCT fact_key` on dim table will FAIL if dim table doesn't have that column.
        # In TPC-DS: store_sales.ss_sold_date_sk = date_dim.d_date_sk.
        # So we need to select `d_date_sk` from date_dim.
        # The current `extract_with_context` logic doesn't seemingly capture the "associated join key" for the dimension.
        # It relies on `fact_join_key` passed from the caller.
        # If the caller passes `ss_sold_date_sk`, and we query `SELECT DISTINCT ss_sold_date_sk FROM date_dim`, it fails.
        
        # Checking this behavior is CRITICAL for the user's "Probing failed" question.
        # If `extract_with_context` doesn't return the right key to query on the dimension, it explains the failure.
        
        # For this test, let's try to resolve using 'd_date_sk' which we know is correct, 
        # to prove the aliases work.
        results = service.resolve_dimension_filter(dim, "d_date_sk", project)
        logging.info(f"Resolved {len(results)} values: {list(results)[:5]}...")

if __name__ == "__main__":
    run_verify()
