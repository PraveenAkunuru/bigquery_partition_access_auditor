# TPC-DS Test Queries for BigQuery Auditor
# These queries test the auditor's ability to identify partition filters
# in complex SQL contexts.

# Query 1-ish: Equality filter on Partition Date
QUERY_1 = """
SELECT
    cs_item_sk,
    SUM(cs_ext_sales_price) as total_sales
FROM
    `bigquerybench.tpcds_100G.catalog_sales`
WHERE
    _PARTITIONDATE = '2000-01-01'
GROUP BY
    cs_item_sk
LIMIT 10
"""

# Query 2-ish: Range filter on Partition Date
QUERY_2 = """
SELECT
    ws_item_sk,
    COUNT(*) as num_sales
FROM
    `bigquerybench.tpcds_100G.web_sales`
WHERE
    _PARTITIONDATE BETWEEN '2002-01-01' AND '2002-01-31'
GROUP BY
    ws_item_sk
LIMIT 10
"""

# Query 3-ish: IN filter on Partition Date
QUERY_3 = """
SELECT
    ss_item_sk,
    AVG(ss_sales_price) as avg_price
FROM
    `bigquerybench.tpcds_100G.store_sales`
WHERE
    _PARTITIONDATE IN ('2001-05-15', '2001-05-16', '2001-05-17')
GROUP BY
    ss_item_sk
LIMIT 10
"""

# Query 4: Join-based partition pruning (Fact + Dim) - EQ
QUERY_4 = """
SELECT
    cs.cs_item_sk,
    SUM(cs.cs_ext_sales_price) as total_sales
FROM
    `bigquerybench.tpcds_100G.catalog_sales` cs
JOIN
    `bigquerybench.tpcds_100G.date_dim` d ON cs.cs_sold_date_sk = d.d_date_sk
WHERE
    d.d_date = '2000-09-09'
GROUP BY
    cs.cs_item_sk
LIMIT 10
"""

# Query 5: Join-based partition pruning - IN
QUERY_5 = """
SELECT
    ws.ws_item_sk,
    COUNT(*) as sales_count
FROM
    `bigquerybench.tpcds_100G.web_sales` ws
JOIN
    `bigquerybench.tpcds_100G.date_dim` d ON ws.ws_sold_date_sk = d.d_date_sk
WHERE
    d.d_date IN ('2002-10-10', '2002-10-11')
GROUP BY
    ws.ws_item_sk
LIMIT 10
"""

# Query 6: Join-based partition pruning - BETWEEN
QUERY_6 = """
SELECT
    ss.ss_item_sk,
    AVG(ss.ss_list_price) as avg_price
FROM
    `bigquerybench.tpcds_100G.store_sales` ss
JOIN
    `bigquerybench.tpcds_100G.date_dim` d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE
    d.d_date BETWEEN '2001-11-11' AND '2001-11-20'
GROUP BY
    ss.ss_item_sk
LIMIT 10
"""

# Query 7: Triple-join transitive pruning (Fact + Dim1 + Dim2)
# Tests if d2 filter propagates to d1 and then to Fact
QUERY_7 = """
SELECT
    cs.cs_item_sk,
    SUM(cs.cs_ext_sales_price)
FROM
    `bigquerybench.tpcds_100G.catalog_sales` cs
JOIN
    `bigquerybench.tpcds_100G.date_dim` d1 ON cs.cs_sold_date_sk = d1.d_date_sk
JOIN
    `bigquerybench.tpcds_100G.date_dim` d2 ON d1.d_date_sk = d2.d_date_sk
WHERE
    d2.d_date = '2000-05-05'
GROUP BY
    cs.cs_item_sk
LIMIT 10
"""

if __name__ == "__main__":
    import argparse

    from google.cloud import bigquery

    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    queries = [QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7]
    for i, q in enumerate(queries, 1):
        print(f"Running Complex Query {i}...")
        try:
            client.query(q).result()
            print(f"Query {i} completed.")
        except Exception as e:
            print(f"Query {i} failed: {e}")
