from components.spark_function import (
    configure_logger
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col
import os

def get_batch_metrics(df, col_to_group, output_path):
    df.withColumn("day", date_format(to_timestamp("timestamp"), "yyyy-MM-dd")) \
      .groupBy(col_to_group) \
      .count() \
      .write \
      .mode("overwrite") \
      .json(output_path)

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("BatchMetrics") \
        .master("local[*]") \
        .getOrCreate() 
    spark.sparkContext.setLogLevel("ERROR")
    
    logger = configure_logger('batchApp')
    logger.info("Lancement de l'application Spark batch...")

    df = spark.read.json(f"/app/data_lake/logs")
    logger.info("Métrics lues avec succès !")

    get_batch_metrics(df, "ip", f"/app/data_lake/metrics/batch/ip")
    get_batch_metrics(df, "agent", f"/app/data_lake/metrics/batch/agent")
    get_batch_metrics(df, "day", f"/app/data_lake/metrics/batch/daily")
    
    logger.info("Métrics écrites avec succès !")

    print("Batch terminé.")
    
    spark.stop()
