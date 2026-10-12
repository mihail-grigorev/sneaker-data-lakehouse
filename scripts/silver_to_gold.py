"""Step 3/4: Silver -> aggregated metrics -> Gold (Parquet) + PostgreSQL datamart."""
import os
import sys
from datetime import datetime

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://s3:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "datamart")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SneakersSilverToGold")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def delete_existing_rows(target_date: str):
    """Make the Postgres load idempotent: remove the day's rows before re-inserting."""
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM public.daily_sneaker_brand_metrics WHERE order_date = %s", (target_date,))
    finally:
        conn.close()


def run(target_date: str):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(f"s3a://silver/orders/order_date={target_date}")

    purchases = F.when(F.col("event_type") == "PURCHASE", F.col("final_price")).otherwise(0)
    refunds = F.when(F.col("event_type") == "RETURN", F.col("final_price")).otherwise(0)
    purchase_counter = F.when(F.col("event_type") == "PURCHASE", 1).otherwise(0)
    return_counter = F.when(F.col("event_type") == "RETURN", 1).otherwise(0)

    df_metrics = (
        df.groupBy("brand", "channel")
        .agg(
            F.sum(purchases).cast(DecimalType(14, 2)).alias("gross_revenue"),
            F.sum(refunds).cast(DecimalType(14, 2)).alias("refund_amount"),
            F.sum(purchase_counter).cast("int").alias("purchase_count"),
            F.sum(return_counter).cast("int").alias("return_count"),
        )
        .withColumn("net_revenue", (F.col("gross_revenue") - F.col("refund_amount")).cast(DecimalType(14, 2)))
        .withColumn(
            "return_rate_pct",
            F.when(F.col("purchase_count") > 0,
                   F.round((F.col("return_count") / F.col("purchase_count")) * 100.0, 2))
             .otherwise(0.0).cast(DecimalType(5, 2)),
        )
        .withColumn(
            "avg_ticket",
            F.when(F.col("purchase_count") > 0,
                   F.round(F.col("gross_revenue") / F.col("purchase_count"), 2))
             .otherwise(0.0).cast(DecimalType(10, 2)),
        )
        .withColumn("order_date", F.to_date(F.lit(target_date)))
        .withColumn("calculated_at", F.current_timestamp())
        .cache()
    )

    # 4a. Gold layer (Parquet in MinIO)
    df_metrics.write.mode("overwrite").parquet(f"s3a://gold/daily_sneaker_brand_metrics/order_date={target_date}")

    # 4b. PostgreSQL datamart
    delete_existing_rows(target_date)
    df_metrics.write.mode("append").jdbc(
        url=f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}",
        table="public.daily_sneaker_brand_metrics",
        properties={"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"},
    )

    print(f"Finished metrics calculation for date={target_date}")
    spark.stop()


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    run(date_arg)