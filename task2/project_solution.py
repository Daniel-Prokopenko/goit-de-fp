from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

default_args = {"start_date": datetime(2025, 5, 1), "catchup": False}

base_path = os.path.dirname(os.path.abspath(__file__))

with DAG(
    "athlete_etl_pipeline",
    schedule_interval=None,
    default_args=default_args,
    description="ETL pipeline for athlete data",
    tags=["goit", "etl", "spark"],
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(base_path, "task2", "landing_to_bronze.py"),
        task_id="landing_to_bronze",
        conn_id="spark-default",
        verbose=1,
    )

    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(base_path, "task2", "bronze_to_silver.py"),
        task_id="bronze_to_silver",
        conn_id="spark-default",
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        application=os.path.join(base_path, "task2", "silver_to_gold.py"),
        task_id="silver_to_gold",
        conn_id="spark-default",
        verbose=1,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
