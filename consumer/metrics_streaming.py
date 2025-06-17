from components.spark_function import (
    configure_logger,
    read_stream, 
    parse_dataframe,
    show_dataframe
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, to_timestamp, date_format, col
import os

def get_connexion_info_streaming(parsed_dataframe, col_to_group, output_path_name):
    return (
        parsed_dataframe
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("day", date_format(to_timestamp("timestamp"), "yyyy-MM-dd"))
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window("timestamp", "10 minutes"),  # Fenêtre glissante
            col(col_to_group)
        )
        .count()
        .writeStream
        .outputMode("append")
        .format("json")
        .option("checkpointLocation", f"./checkpoints/{output_path_name}")
        .option("path", f"/app/data_lake/{output_path_name}")
        .start()
    )


if __name__ == '__main__':

    kafka_server = "broker:29092"
    kafka_topic = os.getenv('KAFKA_TOPIC', 'logs')  # default value if unset

    logger = configure_logger(kafka_topic)
    logger.info("Lancement de l'application Spark Streaming...")

    spark = SparkSession.builder \
        .appName(f"KafkaConsumer_{kafka_topic}") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    stream_result = read_stream(spark, kafka_server, kafka_topic)
    parse_stream_result = parse_dataframe(stream_result)

    # Métriques temps réel
    logger.info("Métriques en cours d'écriture ...")
    ip_query = get_connexion_info_streaming(parse_stream_result, 'ip', 'metrics/streaming/ip')
    agent_query = get_connexion_info_streaming(parse_stream_result, 'agent', 'metrics/streaming/agent')
    daily_query = get_connexion_info_streaming(parse_stream_result, 'day', 'metrics/streaming/daily')

    logger.info("Métriques en live lancées...")

    ip_query.awaitTermination()
    agent_query.awaitTermination()
    daily_query.awaitTermination()
    
    spark.stop()
