from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    """
    a sparksession configured for delta lake, kafka, avro
    """
    """We define the exact Java packages (JARs) Spark needs to download on boot."""
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0",
        "io.delta:delta-spark_2.12:3.1.0"
    ]
    
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # 2. Inject the Kafka & Avro Dependencies
        .config("spark.jars.packages", ",".join(packages))
        
        # 3. Local Mac Optimization
        # Default shuffle partitions is 200, which will freeze a local laptop. We drop it to 4.
        .config("spark.sql.shuffle.partitions", "4") 
        
        .getOrCreate()
    )
    