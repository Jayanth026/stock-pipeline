from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

# Make sure /opt/airflow/scripts is on sys.path (mounted in docker-compose)
SCRIPTS_PATH = "/opt/airflow/scripts"
if SCRIPTS_PATH not in sys.path:
    sys.path.append(SCRIPTS_PATH)

from fetch_and_store import main as fetch_and_store_main  # type: ignore

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="stock_market_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch stock prices from Alpha Vantage and store in Postgres",
    schedule_interval="@daily",  # change to "0 * * * *" for hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "alpha_vantage"],
) as dag:

    def run_stock_pipeline(**context):
        """
        Wrapper to call the main() from fetch_and_store.py.
        Extra try/except here for better logging inside Airflow.
        """
        try:
            fetch_and_store_main()
        except Exception as e:
            # Logging here triggers Airflow error visualization
            from airflow.utils.log.logging_mixin import LoggingMixin

            logger = LoggingMixin().log
            logger.error("Stock pipeline failed inside DAG: %s", e)
            raise

    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_stock_data",
        python_callable=run_stock_pipeline,
        provide_context=True,
    )

    fetch_and_store_task
