import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from botocore.client import Config


def notify_failure(context):
    ti = context["task_instance"]
    print(f"ETL FAILED ALERT: task={ti.task_id} dag={ti.dag_id} date={context['ds']}")


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

SPARK_SUBMIT = "spark-submit --master local[2] --driver-memory 1g"


def verify_silver_data_in_s3(ds, **_):
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://s3:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    prefix = f"orders/order_date={ds}/"
    contents = s3.list_objects_v2(Bucket="silver", Prefix=prefix).get("Contents", [])
    parquet_files = [o for o in contents if o["Key"].endswith(".parquet")]
    if not parquet_files:
        raise ValueError(f"Quality check failed: no Parquet files in s3://silver/{prefix}")
    print(f"Quality check passed: {len(parquet_files)} Parquet file(s) for {ds}")


with DAG(
    dag_id="sneakers_lakehouse_pipeline",
    default_args=default_args,
    description="Batch ETL: Bronze/Silver/Gold on MinIO -> Postgres datamart",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["sneakers", "lakehouse", "spark", "minio"],
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw_events",
        bash_command="python3 /opt/airflow/scripts/generate_raw_data.py {{ ds }}",
    )

    bronze_to_silver = BashOperator(
        task_id="pyspark_bronze_to_silver",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/scripts/bronze_to_silver.py {{{{ ds }}}}",
    )

    quality_check = PythonOperator(
        task_id="quality_check_silver",
        python_callable=verify_silver_data_in_s3,
    )

    silver_to_gold = BashOperator(
        task_id="pyspark_silver_to_gold_and_postgres",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/scripts/silver_to_gold.py {{{{ ds }}}}",
    )

    ingest_raw >> bronze_to_silver >> quality_check >> silver_to_gold