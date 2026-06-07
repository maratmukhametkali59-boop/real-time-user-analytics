from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="build_daily_sales",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False
) as dag:

    refresh_sales_mart = PostgresOperator(
        task_id="refresh_sales_mart",
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