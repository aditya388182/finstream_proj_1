import os
from pyspark.sql.functions import col, to_timestamp
from src.spark_jobs.utils.spark_session import build_spark_session


def run_silver_cleansing():
    spark = build_spark_session("FinStream_Silver_Cleansing")
    spark.sparkContext.setLogLevel("WARN")

    print("Silver Cleansing Engine Online. Reading from Bronze Lakehouse...")

    # 1. Read directly from the Bronze Delta table as a stream
    # ignoreChanges=true ensures that if we ever optimize/compact the Bronze folder,
    # the streaming job won't crash thinking the underlying files were corrupted.
    df_bronze = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load("data/bronze/transactions")
    )

    # 2. Data Type Enforcement & Parsing
    # We elevate raw strings/longs into explicit Spark data types.
    # (Assuming your Avro schema contains these standard fields)
    df_cleaned = (
        df_bronze.withColumn("event_time", to_timestamp(col("timestamp"))).withColumn(
            "amount", col("amount").cast("double")
        )
        # Removed is_international since it doesn't exist in your Bronze schema yet
    )
    # 3. The Senior Engineer Trick: Watermarking & Deduplication
    # Fraud streams often receive the exact same credit card swipe twice due to network glitches.
    # We tell Spark to keep state for 10 minutes (the watermark) and drop any row
    # that has the exact same transaction_id within that time window.
    df_silver = df_cleaned.withWatermark("event_time", "10 minutes").dropDuplicates(
        ["transaction_id", "event_time"]
    )

    # 4. Stream to the Silver Delta Table
    checkpoint_path = "data/silver/_checkpoints/transactions"
    silver_path = "data/silver/transactions"

    print(f"Streaming cleansed data to: {silver_path}")
    query = (
        df_silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(silver_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_silver_cleansing()
