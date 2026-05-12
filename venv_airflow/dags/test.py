from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator # Use this import
from datetime import datetime


with DAG(
    dag_id="test_connections",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["testing"]
) as dag:

    # Task 2: Test Snowflake
    test_snowflake = SQLExecuteQueryOperator(
        task_id="test_snowflake_connection",
        conn_id="snowflake_default",
        sql="SELECT CURRENT_VERSION();"
    )