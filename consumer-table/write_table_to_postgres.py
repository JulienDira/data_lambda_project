from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date,  expr, hex, regexp_replace
from pyspark.sql.types import StructType, StringType
from schema import get_primary_key_and_schema
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

def show_dataframe_parsed_key_value(dataframe):
    (
        dataframe
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    )

def show_dataframe_hex_code(dataframe):
    (
        dataframe
        .select(
            hex(col("key")).alias("key_hex"),
            hex(col("value")).alias("value_hex")
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
    )

def parse_dataframe_with_schema(dataframe, primary_key: str, value_schema: StructType):
    
    dataframe.printSchema()

    # return dataframe \
    #     .select(
    #         expr("regexp_extract(CAST(key AS STRING), '[a-zA-Z]+', 0)").alias(primary_key),
    #         from_json(col("value").cast(StringType()), value_schema).alias("data")
    #     ) \
    #     .select(primary_key, "data.*") \
    #     .withColumn("ingestion_time", current_timestamp()) \
    #     .withColumn("ingestion_date", to_date(col("ingestion_time")))
    return dataframe \
        .select(
            regexp_replace(
                col("key").cast(StringType()),
                r"[^\x20-\x7EÀ-ÿ]",
                ""
            ).alias(primary_key),
            from_json(col("value").cast(StringType()), value_schema).alias("data")
        ) \
        .select(primary_key, "data.*") \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("ingestion_date", to_date(col("ingestion_time")))

def show_parse_dataframe(dataframe):
    (
        dataframe
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 10) 
        .start()
    )

def write_parse_dataframe_to_postgres(
    dataframe,
    primary_key: str,
    postgres_url: str,
    table_name: str,
    postgres_properties: str
):
    query = (
        dataframe
        .writeStream 
        .foreachBatch(
            lambda df, epoch_id: (
                df
                .dropDuplicates([primary_key])
                .write
                .mode("append")  # remplace les données à chaque micro-batch
                .option("truncate", "true")
                .jdbc(postgres_url, table_name, properties=postgres_properties)
            )
        )
        .outputMode("append")
        .start()
        .awaitTermination()
    )

    # Maintient l'application en vie
    query.awaitTermination()
    
if __name__ == '__main__':

    kafka_server = "broker:29092"
    kafka_topic = os.getenv('KAFKA_TOPIC')
    
    logger = configure_logger(kafka_topic)
    
    table_info = get_primary_key_and_schema(table_name=kafka_topic)

    postgres_url = "jdbc:postgresql://db:5432/mydatabase"
    postgres_properties = {
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }

    logger.info("Lancement de l'application Spark Streaming...")

    spark = SparkSession.builder \
        .appName(f"KafkaConsumer_{kafka_topic}") \
        .master("local[*]") \
        .getOrCreate()

    logger.info("Session Spark créée.")

    logger.info(f"Tentative de connexion à Kafka sur {kafka_server} et abonnement au topic {kafka_topic}.")

    stream_result = read_stream(
        spark=spark,
        kafka_server=kafka_server,
        kafka_topic=kafka_topic
    )
    
    logger.info("Données chargées avec succès.")
    
    logger.info("Vérification du format initial.")
    show_dataframe_parsed_key_value(stream_result)
    show_dataframe_hex_code(stream_result) 
    
    primary_key = table_info.get('primary_key')
    schema_value = table_info.get('value_schema')
    logger.info("Schémas définis pour la clé et la valeur.")
    logger.info(f"Primary key: {primary_key}, Schema: {schema_value}")
    
    logger.info("Données en cours de transformation...")
    dataframe_parsed = parse_dataframe_with_schema(
        dataframe=stream_result,
        primary_key=primary_key,
        value_schema=schema_value
    )
    logger.info("Données parsée avec succès !!!")

    logger.info("Démarrage de l'écriture en console pour le debug.")
    show_parse_dataframe(dataframe_parsed)

    logger.info("Initialisation de l'écriture vers postgresql...")
    write_parse_dataframe_to_postgres(
        dataframe=dataframe_parsed,
        primary_key=primary_key,
        postgres_url=postgres_url,
        table_name=kafka_topic,
        postgres_properties=postgres_properties
    )
    logger.info("L'écriture vers postgresql a démarrée. En attente des messages...")



