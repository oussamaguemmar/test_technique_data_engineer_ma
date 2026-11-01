import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "s3a://silver/chicago_taxi/year=2023/quarter=Q1"

GOLD_DAILY_PATH = "s3a://gold/chicago_taxi/daily_metrics"
GOLD_AREA_PATH = "s3a://gold/chicago_taxi/pickup_area_metrics"


def main():
    spark = (
        SparkSession.builder
        .appName("ChicagoTaxi-SilverToGold")
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

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading Silver data...")

        df = spark.read.parquet(SILVER_PATH)

        # Daily business metrics.
        daily_metrics = (
            df
            .withColumn("trip_date", F.to_date("trip_start_timestamp"))
            .groupBy("trip_date")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum("trip_total").alias("total_revenue"),
                F.avg("fare").alias("average_fare"),
                F.avg("trip_miles").alias("average_trip_miles"),
                F.avg("trip_seconds").alias("average_trip_seconds"),
                F.sum("tips").alias("total_tips"),
            )
            .orderBy("trip_date")
        )

        print("Writing daily metrics...")

        (
            daily_metrics.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(GOLD_DAILY_PATH)
        )

        # Pickup area business metrics.
        pickup_area_metrics = (
            df
            .groupBy("pickup_community_area")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum("trip_total").alias("total_revenue"),
                F.avg("trip_total").alias("average_trip_total"),
                F.avg("trip_miles").alias("average_trip_miles"),
            )
            .orderBy(F.desc("total_trips"))
        )

        print("Writing pickup area metrics...")

        (
            pickup_area_metrics.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(GOLD_AREA_PATH)
        )

        print("Gold aggregation completed.")
        print(f"Daily metric rows: {daily_metrics.count()}")
        print(f"Pickup area rows: {pickup_area_metrics.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
