
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "s3a://silver/chicago_taxi/year=2023/quarter=Q1"
GOLD_DAILY_PATH = "s3a://gold/chicago_taxi/daily_metrics"
GOLD_AREA_PATH = "s3a://gold/chicago_taxi/pickup_area_metrics"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ChicagoTaxi-PipelineTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.environ["RUSTFS_ENDPOINT_URL"],
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.environ["RUSTFS_ACCESS_KEY"],
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.environ["RUSTFS_SECRET_KEY"],
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def check(name, condition):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}")
    return condition


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    results = []

    try:
        print("\n=== Loading pipeline datasets ===")

        silver = spark.read.parquet(SILVER_PATH)
        daily = spark.read.parquet(GOLD_DAILY_PATH)
        areas = spark.read.parquet(GOLD_AREA_PATH)

        silver_count = silver.count()
        daily_count = daily.count()
        area_count = areas.count()

        print("\n=== Silver quality tests ===")

        results.append(check(
            "Silver contains records",
            silver_count > 0,
        ))

        results.append(check(
            "No null trip IDs",
            silver.filter(F.col("trip_id").isNull()).count() == 0,
        ))

        results.append(check(
            "No empty trip IDs",
            silver.filter(F.trim("trip_id") == "").count() == 0,
        ))

        results.append(check(
            "No duplicate trip IDs",
            silver.groupBy("trip_id").count()
            .filter(F.col("count") > 1).count() == 0,
        ))

        results.append(check(
            "Valid timestamps",
            silver.filter(
                F.col("trip_start_timestamp").isNull()
                | F.col("trip_end_timestamp").isNull()
                | (
                    F.col("trip_end_timestamp")
                    < F.col("trip_start_timestamp")
                )
            ).count() == 0,
        ))

        results.append(check(
            "No negative trip durations",
            silver.filter(F.col("trip_seconds") < 0).count() == 0,
        ))

        results.append(check(
            "No negative trip distances",
            silver.filter(F.col("trip_miles") < 0).count() == 0,
        ))

        results.append(check(
            "All records belong to Q1 2023",
            silver.filter(
                (F.col("trip_start_timestamp") < "2023-01-01")
                | (F.col("trip_start_timestamp") >= "2023-04-01")
            ).count() == 0,
        ))

        print("\n=== Gold aggregation tests ===")

        daily_trips = daily.agg(
            F.sum("total_trips")
        ).first()[0]

        area_trips = areas.agg(
            F.sum("total_trips")
        ).first()[0]

        results.append(check(
            "Gold daily aggregation reconciles with Silver",
            daily_trips == silver_count,
        ))

        results.append(check(
            "Gold pickup-area aggregation reconciles with Silver",
            area_trips == silver_count,
        ))

        results.append(check(
            "Gold contains 90 daily records",
            daily_count == 90,
        ))

        results.append(check(
            "Gold contains 78 pickup-area groups",
            area_count == 78,
        ))

        print("\n=== Test summary ===")

        passed = sum(results)
        failed = len(results) - passed

        print(f"Total tests: {len(results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")

        if failed:
            sys.exit(1)

        print("\nAll pipeline quality tests passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
