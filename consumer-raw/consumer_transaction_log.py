from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField, IntegerType, TimestampType
import logging

# Configuration du logging
def configure_logger(topic):
    logger = logging.getLogger(topic)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(f"%(asctime)s - {topic} - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

end_file_path  = 'transaction_log'

# Configuration du logging
kafka_topic = 'transaction_log'
logger = configure_logger(kafka_topic)

logger.info("Lancement de l'application Spark Streaming...")

# Crée la session Spark
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

logger.info("Session Spark créée.")

# Schéma des structures imbriquées
# location_schema = StructType([
#     StructField("city", StringType(), True),
#     StructField("country", StringType(), True)
# ])

# shipping_address_schema = StructType([
#     StructField("street", StringType(), True),
#     StructField("zip", StringType(), True),
#     StructField("city", StringType(), True),
#     StructField("country", StringType(), True)
# ])

# device_info_schema = StructType([
#     StructField("os", StringType(), True),
#     StructField("browser", StringType(), True),
#     StructField("ip_address", StringType(), True)
# ])

# # Schéma complet du message
# schema = StructType([
#     StructField("transaction_id", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("user_id", StringType(), True),
#     StructField("user_name", StringType(), True),
#     StructField("product_id", StringType(), True),
#     StructField("amount", DoubleType(), True),
#     StructField("currency", StringType(), True),
#     StructField("transaction_type", StringType(), True),
#     StructField("status", StringType(), True),
#     StructField("location", location_schema, True),
#     StructField("payment_method", StringType(), True),
#     StructField("product_category", StringType(), True),
#     StructField("quantity", IntegerType(), True),
#     StructField("shipping_address", shipping_address_schema, True),
#     StructField("device_info", device_info_schema, True),
#     StructField("customer_rating", IntegerType(), True),
#     StructField("discount_code", StringType(), True),
#     StructField("tax_amount", DoubleType(), True),
#     StructField("thread", IntegerType(), True),
#     StructField("message_number", IntegerType(), True),
#     StructField("timestamp_of_reception_log", StringType(), True)
# ])

# logger.info("Schéma du message défini.")

# Lecture des messages Kafka
logger.info("Tentative de connexion à Kafka sur broker:29092 et abonnement au topic 'transaction_log'.")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

logger.info("Connexion à Kafka réussie. Lecture des messages en streaming.")

# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
#     .select(from_json(col("json_value"), schema=None).alias("data")) \
#     .select("data.*")
    
df_bronze = df_raw.selectExpr(
    "CAST(value AS STRING) as json_value",
    "CAST(key AS STRING) as key",
    "topic", "partition", "offset", "timestamp"
)   .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("ingestion_date", to_date(col("ingestion_time")))

logger.info("Transformation JSON des messages terminée. Schéma résultant :")
df_bronze.printSchema()

# Affichage des premiers enregistrements dans la console (pour debug uniquement, facultatif)
(
    df_bronze.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

logger.info("Démarrage de l'écriture en console pour le debug.")

# Écriture en fichiers Parquet
logger.info("Initialisation de l'écriture en Parquet...")

query = df_bronze.writeStream \
    .format("parquet") \
    .option("checkpointLocation", f"./checkpoints/{end_file_path}") \
    .option("path", f"/app/data_lake/{end_file_path}") \
    .partitionBy("ingestion_date") \
    .outputMode("append") \
    .start()

logger.info("L'écriture en Parquet a démarré. En attente des messages...")

# Maintient l'application en vie
query.awaitTermination()
