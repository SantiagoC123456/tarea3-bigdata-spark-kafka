from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, from_json, from_unixtime, to_timestamp, window
)
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StructField, StructType
)

spark = (
    SparkSession.builder
    .appName("SensorDataAnalysis")
    .master("local[1]")
    .config("spark.driver.memory", "512m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType())
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
)

stats_df = (
    parsed_df
    .groupBy(window(col("event_time"), "1 minute"), col("sensor_id"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )
)

query = (
    stats_df.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
