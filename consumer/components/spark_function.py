from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
import logging
import os

def configure_logger(topic):
    logger = logging.getLogger(topic)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(f"%(asctime)s - {topic} - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def read_stream(spark: SparkSession, kafka_server: str, kafka_topic: str):

    return spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("keyDeserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .option("valueDeserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .load()
    
def parse_dataframe(dataframe):
    
    dataframe.printSchema()

    return (
        dataframe \
        .selectExpr("CAST(value AS STRING)") \
        .withColumn("parts", split(col("value"), ",")) \
        .select(
            col("parts")[0].alias("timestamp"),
            col("parts")[1].alias("ip"),
            col("parts")[2].alias("agent")
        )
    )

def show_dataframe(dataframe):
    return (
        dataframe
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 10) 
        .start()
    )

def write_parse_dataframe_to_json(
    dataframe,
    kafka_topic: str
):
    (
        dataframe
        .writeStream 
        .format("json")
        .option("checkpointLocation", f"./checkpoints/{kafka_topic}")
        .option("path", f"/app/data_lake/{kafka_topic}")
        .outputMode("append")
        .start()
        .awaitTermination()
    )