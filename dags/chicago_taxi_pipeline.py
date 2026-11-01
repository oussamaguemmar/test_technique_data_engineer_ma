from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="chicago_taxi_pipeline",
    description="Chicago Taxi Trips Medallion data pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["chicago-taxi", "pyspark", "medallion"],
    doc_md="""
    ## Chicago Taxi Trips Pipeline

    Bronze CSV in RustFS → Silver cleaned Parquet → Gold BI aggregates.

    Spark runs locally inside the Airflow environment.
    """,
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "python /opt/airflow/src/spark/jobs/bronze_to_silver.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "python /opt/airflow/src/spark/jobs/silver_to_gold.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    bronze_to_silver >> silver_to_gold
