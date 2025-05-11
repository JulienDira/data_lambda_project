from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date,  expr, hex, trim, regexp_replace
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

# Configuration du logging
kafka_topic = 'TRANSACTION_STATUS_EVOLUTION'
logger = configure_logger(kafka_topic)

postgres_url = "jdbc:postgresql://db:5432/mydatabase"
postgres_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

logger.info("Lancement de l'application Spark Streaming...")

# Crée la session Spark
spark = SparkSession.builder \
    .appName(f"KafkaConsumer_{kafka_topic}") \
    .master("local[*]") \
    .getOrCreate()

logger.info("Session Spark créée.")

# Lecture des messages Kafka
logger.info(f"Tentative de connexion à Kafka sur broker:29092 et abonnement au topic {kafka_topic}.")

primary_key = 'TRANSACTION_ID'
value_schema = StructType([
    StructField("LATEST_STATUS", StringType(), True)
])

logger.info("Schémas définis pour la clé et la valeur.")

# Lecture des messages Kafka avec les schémas définis pour la clé et la valeur
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("keyDeserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .option("valueDeserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .load() \

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

logger.info("Données en cours de transformation...") 

(df_raw
 .select(
     hex(col("key")).alias("key_hex"),
     hex(col("value")).alias("value_hex")
 )
 .writeStream
 .format("console")
 .outputMode("append")
 .start()
)

df_parsed = (
    df_raw
    .select(
        # Traiter la clé comme un TRANSACTION_TYPE
        expr("regexp_extract(CAST(key AS STRING), '[a-zA-Z]+', 0)").alias(primary_key),
        # Désérialiser le JSON de la valeur selon le schéma défini
        from_json(col("value").cast(StringType()), value_schema).alias("data")
    ) \
    .select(primary_key, "data.*") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("ingestion_date", to_date(col("ingestion_time")))
)

logger.info("Données parsée avec succès !!!")
df_parsed.printSchema()

logger.info("Vérification des données chargées :") 
# Affichage des premiers enregistrements dans la console (pour debug uniquement, facultatif)
(
    df_parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 10) 
    .start()
)

logger.info("Démarrage de l'écriture en console pour le debug.")

# Écriture sur serveur postgresql
logger.info("Initialisation de l'écriture vers postgresql...")

query = (
    df_parsed
    .writeStream 
    .foreachBatch(
        lambda df, epoch_id: (
            df
            .dropDuplicates([primary_key])
            .write
            .mode("overwrite")  # remplace les données à chaque micro-batch
            .option("truncate", "true")
            .jdbc(postgres_url, kafka_topic, properties=postgres_properties)
        )
    )
    .outputMode("append")
    .start()
    .awaitTermination()
)

logger.info("L'écriture vers postgresql a démarrée. En attente des messages...")

# Maintient l'application en vie
query.awaitTermination()


