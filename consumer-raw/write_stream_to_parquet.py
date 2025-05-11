from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField, IntegerType, TimestampType
from schema import get_table_schema
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
    .load() 

def show_dataframe_parsed_key_value(dataframe):
    (
        dataframe
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
    )
    
def parse_dataframe_with_schema(dataframe, schema: StructType):
    
    dataframe.printSchema()

    return dataframe \
    .selectExpr(
        "CAST(value AS STRING) as json_value"
    ) \
    .select(from_json(col("json_value"), schema=schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("ingestion_date", to_date(col("ingestion_time")))

def parse_dataframe_without_schema(dataframe):
    
    dataframe.printSchema()
      
    return dataframe \
    .selectExpr(
        "CAST(value AS STRING) as json_value",
        "CAST(key AS STRING) as key",
        "topic", "partition", "offset", "timestamp"
    )   .withColumn("ingestion_time", current_timestamp()) \
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

def write_parse_dataframe_to_parquet(
    dataframe,
    kafka_topic: str,
    write_mode: str
):
    query = (
        dataframe
        .writeStream
        .format("parquet")
        .option("checkpointLocation", f"./checkpoints/{kafka_topic}")
        .option("path", f"/app/data_lake/{kafka_topic}")
        .partitionBy("ingestion_date")
        .outputMode(write_mode)
        .start()
    )

    query.awaitTermination()

if __name__ == '__main__':

    kafka_server = "broker:29092"
    kafka_topic = os.getenv('KAFKA_TOPIC')
    schema_true_false = os.getenv('SCHEMA')
    write_mode =  os.getenv('WRITE_MODE')
    
    logger = configure_logger(kafka_topic)

    logger.info("Lancement de l'application Spark Streaming...")

    # Crée la session Spark
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

    if schema_true_false == 'True' : 
        schema = get_table_schema(table_name=kafka_topic)
        logger.info("Schéma du message défini.")        

        logger.info("Données en cours de transformation...")
        dataframe_parsed = parse_dataframe_with_schema(
            dataframe=stream_result,
            schema=schema
        )
        logger.info("Données parsée avec succès !!!")
        
    elif schema_true_false == 'False' : 
        
        logger.info("Données en cours de transformation...")
        dataframe_parsed = parse_dataframe_without_schema(
            dataframe=stream_result
        )
        logger.info("Données parsée avec succès !!!")
        
    logger.info("Démarrage de l'écriture en console pour le debug.")
    show_parse_dataframe(dataframe_parsed)

    logger.info("Initialisation de l'écriture en Parquet...")
    write_parse_dataframe_to_parquet(
        dataframe=dataframe_parsed,
        kafka_topic=kafka_topic,
        write_mode=write_mode
    )
    logger.info("L'écriture en Parquet a démarré. En attente des messages...")