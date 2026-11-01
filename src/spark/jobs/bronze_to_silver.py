import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
)


BRONZE_PATH = (
    "s3a://bronze/chicago_taxi_kaggle/"
    "source=2023/Taxi_Trips_2023.csv"
)

SILVER_PATH = "s3a://silver/chicago_taxi/year=2023/quarter=Q1"

S3_ENDPOINT = os.environ.get(
    "RUSTFS_ENDPOINT_URL", "http://rustfs:9000"
)


def normalize_column_name(name):
    """Normalize CSV headers to predictable snake_case names."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def main():
    spark = (
        SparkSession.builder
        .appName("ChicagoTaxi-BronzeToSilver")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ["RUSTFS_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ["RUSTFS_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading Bronze CSV...")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(BRONZE_PATH)
        )

        # Normalize headers, including the double space in one source column.
        for old_name in df.columns:
            df = df.withColumnRenamed(
                old_name, normalize_column_name(old_name)
            )

        print(f"Source columns: {len(df.columns)}")

        # Parse dates and cast numeric fields.
        df = (
            df
            .withColumn(
                "trip_start_timestamp",
                F.to_timestamp(
                    "trip_start_timestamp", "M/d/yyyy h:mm:ss a"
                ),
            )
            .withColumn(
                "trip_end_timestamp",
                F.to_timestamp(
                    "trip_end_timestamp", "M/d/yyyy h:mm:ss a"
                ),
            )
            .withColumn("trip_seconds", F.col("trip_seconds").cast(LongType()))
            .withColumn("trip_miles", F.col("trip_miles").cast(DoubleType()))
            .withColumn(
                "pickup_census_tract",
                F.col("pickup_census_tract").cast("string"),
            )
            .withColumn(
                "dropoff_census_tract",
                F.col("dropoff_census_tract").cast("string"),
            )
            .withColumn(
                "pickup_community_area",
                F.col("pickup_community_area").cast(IntegerType()),
            )
            .withColumn(
                "dropoff_community_area",
                F.col("dropoff_community_area").cast(IntegerType()),
            )
        )

        for column in ("fare", "tips", "tolls", "extras", "trip_total"):
            df = df.withColumn(
                column, F.col(column).cast(DecimalType(12, 2))
            )

        for column in (
            "pickup_centroid_latitude",
            "pickup_centroid_longitude",
            "dropoff_centroid_latitude",
            "dropoff_centroid_longitude",
        ):
            df = df.withColumn(
                column, F.col(column).cast(DoubleType())
            )

        # Keep Q1 2023 only.
        df = df.filter(
            (F.col("trip_start_timestamp") >= F.lit("2023-01-01"))
            & (F.col("trip_start_timestamp") < F.lit("2023-04-01"))
        )

        rows_in_period = df.count()
        print(f"Rows in Q1 2023: {rows_in_period:,}")

        # Basic data quality rules.
        valid_condition = (
            F.col("trip_id").isNotNull()
            & (F.trim(F.col("trip_id")) != "")
            & F.col("trip_start_timestamp").isNotNull()
            & F.col("trip_end_timestamp").isNotNull()
            & (F.col("trip_end_timestamp") >= F.col("trip_start_timestamp"))
            & (F.col("trip_seconds").isNull() | (F.col("trip_seconds") >= 0))
            & (F.col("trip_miles").isNull() | (F.col("trip_miles") >= 0))
        )

        valid_df = df.filter(valid_condition)
        rejected_count = rows_in_period - valid_df.count()

        print(f"Rejected invalid rows: {rejected_count:,}")

        # One record per Trip ID.
        silver_df = (
            valid_df
            .dropDuplicates(["trip_id"])
            .withColumn("year", F.year("trip_start_timestamp"))
            .withColumn("quarter", F.quarter("trip_start_timestamp"))
            .withColumn("processed_at", F.current_timestamp())
        )

        final_count = silver_df.count()
        print(f"Final Silver rows: {final_count:,}")

        (
            silver_df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(SILVER_PATH)
        )

        print(f"Silver data written to: {SILVER_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
