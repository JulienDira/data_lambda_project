from components.spark_function import(
    configure_logger,
    read_stream, 
    parse_dataframe,
    show_dataframe,
    write_parse_dataframe_to_json
)
from pyspark.sql import SparkSession
import os
    
if __name__ == '__main__':

    kafka_server = "broker:29092"
    kafka_topic = os.getenv('KAFKA_TOPIC')
    
    logger = configure_logger(kafka_topic)

    logger.info("Lancement de l'application Spark Streaming...")

    spark = SparkSession.builder \
        .appName(f"KafkaConsumer_{kafka_topic}") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Session Spark créée.")

    logger.info(f"Tentative de connexion à Kafka sur {kafka_server} et abonnement au topic {kafka_topic}.")

    stream_result = read_stream(
        spark=spark,
        kafka_server=kafka_server,
        kafka_topic=kafka_topic
    )
    
    logger.info("Données chargées avec succès.")
    logger.info("Données en cours de transformation...")
    
    parse_stream_result = parse_dataframe(stream_result)    
    logger.info("Données parsée avec succès !!!")

    logger.info("Démarrage de l'écriture en console pour le debug.")
    show_dataframe(parse_stream_result)

    logger.info("Initialisation de l'écriture vers le file system...")
    write_parse_dataframe_to_json(
        dataframe=parse_stream_result,
        kafka_topic=kafka_topic
    )
    logger.info("L'écriture vers postgresql a démarrée. En attente des messages...")
    
    spark.stop()



