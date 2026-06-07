from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="build_all_marts",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False
) as dag:

    refresh_daily_sales = PostgresOperator(
        task_id="refresh_daily_sales",
        postgres_conn_id="postgres_analytics",
        sql="""
        DROP TABLE IF EXISTS mart_daily_sales;

        CREATE TABLE mart_daily_sales AS
        SELECT
            DATE(event_time) AS event_date,
            COUNT(*) FILTER (
                WHERE event_type = 'purchase'
            ) AS purchases,
            ROUND(
                COALESCE(
                    SUM(price) FILTER (
                        WHERE event_type = 'purchase'
                    ),
                    0
                ),
                2
            ) AS revenue
        FROM raw_events
        GROUP BY DATE(event_time);
        """
    )

    refresh_top_products = PostgresOperator(
        task_id="refresh_top_products",
        postgres_conn_id="postgres_analytics",
        sql="""
        DROP TABLE IF EXISTS mart_top_products;

        CREATE TABLE mart_top_products AS
        SELECT
            product_id,
            COUNT(*) AS purchases,
            ROUND(SUM(price), 2) AS revenue
        FROM raw_events
        WHERE event_type = 'purchase'
        GROUP BY product_id
        ORDER BY revenue DESC;
        """
    )

    refresh_daily_sales >> refresh_top_products