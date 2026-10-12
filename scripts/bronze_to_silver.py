"""Step 2: Bronze (JSON) -> Silver (Parquet, partitioned by order_date)."""
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://s3:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SneakersBronzeToSilver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run(target_date: str):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("brand", StringType(), True),
        StructField("model", StringType(), True),
        StructField("size_eu", DoubleType(), True),
        StructField("channel", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("final_price", DoubleType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
    ])

    raw_path = f"s3a://bronze/raw/date={target_date}/*.json"
    df = spark.read.schema(schema).json(raw_path)

    df_clean = (
        df
        .filter(F.col("order_id").isNotNull())
        .filter((F.col("final_price") > 0) & (F.col("size_eu") >= 35.0) & (F.col("size_eu") <= 49.0))
        .dropDuplicates(["order_id", "event_type"])
        .withColumn("order_timestamp", F.to_timestamp(F.col("event_time")))
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("brand", F.trim(F.initcap(F.col("brand"))))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("final_price", F.col("final_price").cast(DecimalType(10, 2)))
        .withColumn("price", F.col("price").cast(DecimalType(10, 2)))
        .withColumn("discount_amount", F.col("discount_amount").cast(DecimalType(10, 2)))
        .withColumn("processed_at", F.current_timestamp())
        .drop("event_time")
    )

    # Dynamic partition overwrite: re-running a date replaces only its partition (idempotent)
    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet("s3a://silver/orders")
    )

    print(f"Successfully processed Bronze -> Silver for date={target_date}")
    spark.stop()


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    run(date_arg)