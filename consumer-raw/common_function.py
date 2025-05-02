from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField, IntegerType, TimestampType
import logging

def configure_logger(topic):
    logger = logging.getLogger(topic)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(f"%(asctime)s - {topic} - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def read_stream_writte_to_parquet_with_schema(
    spark_session: SparkSession,
    logger: logging,
    kafka_topic: str,
    schema: StructType,
    writte_mode: str, 
    kafka_server: str = "broker:29092"
):
    logger = configure_logger(kafka_topic)
    logger.info("Schéma du message défini.")

    # Lecture des messages Kafka
    logger.info(f"Tentative de connexion à Kafka sur broker:29092 et abonnement au topic {kafka_topic}.")

    df_raw = spark_session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info("Données chargées avec succès.")
    logger.info("Vérification du format initial.")       

    (
        df_raw
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    )

    logger.info("Connexion à Kafka réussie. Lecture des messages en streaming.")
        
    df_bronze = df_raw.selectExpr(
        "CAST(value AS STRING) as json_value"
    )   .select(from_json(col("json_value"), schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("ingestion_date", to_date(col("ingestion_time")))

    logger.info("Transformation JSON des messages terminée. Schéma résultant :")
    df_bronze.printSchema()

    # Affichage des premiers enregistrements dans la console (pour debug uniquement, facultatif)
    (
        df_bronze.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 10) 
        .start()
    )

    logger.info("Démarrage de l'écriture en console pour le debug.")

    # Écriture en fichiers Parquet
    logger.info("Initialisation de l'écriture en Parquet...")

    query = df_bronze.writeStream \
        .format("parquet") \
        .option("checkpointLocation", f"./checkpoints/{kafka_topic}") \
        .option("path", f"/app/data_lake/{kafka_topic}") \
        .partitionBy("ingestion_date") \
        .outputMode(writte_mode) \
        .start()

    logger.info("L'écriture en Parquet a démarré. En attente des messages...")

    # Maintient l'application en vie
    query.awaitTermination()