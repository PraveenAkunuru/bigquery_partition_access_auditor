# TPC-DS Query 95: Extreme Complexity Stress Test
# This query involves multiple CTEs, joins, and subqueries.
# We include a filter on _PARTITIONDATE to test pruning detection.

QUERY_95 = """
WITH ws_wh AS (
    SELECT
        ws1.ws_order_number,
        ws1.ws_warehouse_sk wh1,
        ws2.ws_warehouse_sk wh2
    FROM
        `bigquerybench.tpcds_100G.web_sales` ws1,
        `bigquerybench.tpcds_100G.web_sales` ws2
    WHERE
        ws1.ws_order_number = ws2.ws_order_number
        AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
        AND ws1._PARTITIONDATE = '2001-05-01'
        AND ws2._PARTITIONDATE = '2001-05-01'
),
web_returns_p AS (
    SELECT * FROM `bigquerybench.tpcds_100G.web_returns`
),
web_sales_p AS (
    SELECT * FROM `bigquerybench.tpcds_100G.web_sales`
    WHERE _PARTITIONDATE = '2001-05-01'
)
SELECT
    count(distinct ws_order_number) as order_count,
    sum(ws_ext_ship_cost) as total_ship_cost,
    sum(ws_net_profit) as total_net_profit
FROM
    web_sales_p ws1
JOIN
    `bigquerybench.tpcds_100G.date_dim` d ON ws1.ws_ship_date_sk = d.d_date_sk
JOIN
    `bigquerybench.tpcds_100G.web_site` s ON ws1.ws_web_site_sk = s.web_site_sk
WHERE
    d.d_date BETWEEN '2001-04-01' AND '2001-04-30'
    AND s.web_company_name = 'pri'
    AND EXISTS (
        SELECT *
        FROM ws_wh
        WHERE ws1.ws_order_number = ws_wh.ws_order_number
    )
    AND EXISTS (
        SELECT *
        FROM web_returns_p wr1
        WHERE ws1.ws_order_number = wr1.wr_order_number
    )
LIMIT 100
"""

if __name__ == "__main__":
    import argparse

    from google.cloud import bigquery

    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    print("Running Extreme TPC-DS Query 95...")
    try:
        client.query(QUERY_95).result()
        print("Query 95 completed.")
    except Exception as e:
        print(f"Query 95 execution note: {e}")
