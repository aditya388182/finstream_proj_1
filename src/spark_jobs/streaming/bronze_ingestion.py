import os
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.avro.functions import from_avro
from src.spark_jobs.utils.spark_session import build_spark_session

def process_micro_batch(batch_df, batch_id):
    """
    The Enterprise DLQ Router.
    This function executes on every micro-batch of data that arrives from Kafka.
    """
    # 1. Cache the batch so Spark doesn't re-read from Kafka for the splits
    batch_df.persist()
    
    # 2. The Clean Path (Bronze Layer)
    # We define a "clean" record as one where the Avro parser successfully extracted a transaction_id
    clean_df = (
        batch_df.filter(col("parsed_data.transaction_id").isNotNull())
        .select("parsed_data.*")
        .withColumn("_ingested_at", current_timestamp()) # Add audit metadata
    )
    
    if clean_df.count() > 0:
        clean_df.write \
            .format("delta") \
            .mode("append") \
            .save("data/bronze/transactions")
            
    # 3. The Dead Letter Queue (DLQ) Path
    # If the parser failed or the payload was malformed, it lands here
    corrupt_df = (
        batch_df.filter(col("parsed_data.transaction_id").isNull())
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_kafka_value"),
            current_timestamp().alias("_failed_at")
        )
    )
                         
    if corrupt_df.count() > 0:
        corrupt_df.write \
            .format("delta") \
            .mode("append") \
            .save("data/dlq/transactions")
            
    # 4. Release the memory
    batch_df.unpersist()


def run_bronze_ingestion():
    spark = build_spark_session("FinStream_Bronze_Ingestion")
    spark.sparkContext.setLogLevel("WARN")
    
    print("Bronze Ingestion Engine Online. Connecting to Kafka...")

    # 1. Read Raw Kafka Stream
    # We connect to the EXTERNAL walkie-talkie (localhost:9092)
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions-raw")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false") # Prevents crashes if Kafka deletes old logs
        .load()
    )

    # 2. Load the strict Avro Contract
    schema_path = "src/producer/schemas/transaction_value.avsc"
    with open(schema_path, "r") as f:
        avro_schema = f.read()

    # 3. Strip the 5-byte Confluent header and attempt to parse
    # We do NOT drop the data here. We keep it nested in a 'parsed_data' column.
    df_parsed = (
        df_raw
        .withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
        .withColumn("parsed_data", from_avro(col("fixed_value"), avro_schema))
    )
        
    # 4. Route the stream through our DLQ logic
    print("Starting micro-batch processing with DLQ routing...")
    query = (
        df_parsed.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", "data/bronze/_checkpoints/transactions")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    run_bronze_ingestion()