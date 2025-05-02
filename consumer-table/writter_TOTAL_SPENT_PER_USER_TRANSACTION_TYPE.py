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

end_file_path = 'test'

# Configuration du logging
kafka_topic = 'TOTAL_SPENT_PER_USER_TRANSACTION_TYPE'
logger = configure_logger(kafka_topic)

postgres_url = "jdbc:postgresql://100.117.134.55:30432/project_streaming"
postgres_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

logger.info("Lancement de l'application Spark Streaming...")

# Crée la session Spark
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

logger.info("Session Spark créée.")

schema = StructType([
    StructField("KSQL_COL_0", StringType(), True),
    StructField("TOTAL_SPENT", DoubleType(), True)
])

logger.info("Schéma du message défini pour les données flattened.")

# Lecture des messages Kafka
logger.info("Tentative de connexion à Kafka sur broker:29092 et abonnement au topic 'transaction_log'.")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

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

# Écriture sur serveur postgresql
logger.info("Initialisation de l'écriture vers postgresql...")

query = (
    df_bronze
    .writeStream 
    .foreachBatch(
        lambda df, epoch_id: df.write
            .mode("overwrite")  # remplace les données à chaque micro-batch
            .option("truncate", "true")  # évite de drop/recreate, juste un truncate
            .jdbc(postgres_url, kafka_topic, properties=postgres_properties)
    )
    .outputMode("append")  # nécessaire pour le streaming, même si le batch fait un overwrite
    .start()
    .awaitTermination()
)


logger.info("L'écriture vers postgresql a démarrée. En attente des messages...")

# Maintient l'application en vie
query.awaitTermination()


