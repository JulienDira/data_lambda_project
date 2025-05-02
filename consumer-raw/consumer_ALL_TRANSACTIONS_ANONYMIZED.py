from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField, IntegerType, TimestampType
from common_function import read_stream_writte_to_parquet_with_schema, configure_logger


# Configuration du logging
kafka_topic = 'ALL_TRANSACTIONS_ANONYMIZED'
logger = configure_logger(topic=kafka_topic)

logger.info("Lancement de l'application Spark Streaming...")

# Crée la session Spark
spark = SparkSession.builder \
    .appName(f"KafkaConsumer_{kafka_topic}") \
    .master("local[*]") \
    .getOrCreate()

logger.info("Session Spark créée.")

schema = StructType([
    StructField("TRANSACTION_ID", StringType(), True),
    StructField("TIMESTAMP", StringType(), True),
    StructField("USER_ID_HASHED", StringType(), True),
    StructField("USER_NAME_HASHED", StringType(), True),
    StructField("PRODUCT_ID", StringType(), True),
    StructField("AMOUNT", DoubleType(), True),
    StructField("CURRENCY", StringType(), True),
    StructField("TRANSACTION_TYPE", StringType(), True),
    StructField("STATUS", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("PAYMENT_METHOD", StringType(), True),
    StructField("PRODUCT_CATEGORY", StringType(), True),
    StructField("QUANTITY", IntegerType(), True),
    StructField("SHIPPING_STREET", StringType(), True),
    StructField("SHIPPING_ZIP", StringType(), True),
    StructField("SHIPPING_CITY", StringType(), True),
    StructField("SHIPPING_COUNTRY", StringType(), True),
    StructField("DEVICE_OS", StringType(), True),
    StructField("DEVICE_BROWSER", StringType(), True),
    StructField("MASKED_IP", StringType(), True),
    StructField("CUSTOMER_RATING", IntegerType(), True),
    StructField("DISCOUNT_CODE", StringType(), True),
    StructField("TAX_AMOUNT", DoubleType(), True),
    StructField("THREAD", IntegerType(), True),
    StructField("MESSAGE_NUMBER", IntegerType(), True),
    StructField("TIMESTAMP_OF_RECEPTION_LOG", StringType(), True)
])

logger.info("Schéma du message défini.")

# Lecture des messages Kafka

read_stream_writte_to_parquet_with_schema(
    spark_session=spark,
    logger=logger,
    kafka_topic=kafka_topic,
    schema=schema, 
    writte_mode="append"
)