from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff
import argparse

def purge_data(spark: SparkSession, data_lake_path: str, table_name: str, purge_intervalle: int):
    
    path = f"{data_lake_path}/{table_name}"
    df = spark.read.parquet(path)

    df_filtered = df.filter(datediff(current_date(), col("ingestion_date")) <= purge_intervalle)

    df_filtered.write.mode("overwrite").parquet(path)
    
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Purge les données du data lake')
    parser.add_argument('--tables', nargs='+', required=True, help='Liste des tables à purger')
    parser.add_argument('--data_lake_path', required=True, help='Chemin vers le data lake')
    parser.add_argument('--purge_intervalle', type=int, required=True, help='Intervalle de purge en jours')
    
    args = parser.parse_args()
    
    list_table = args.tables
    data_lake_path = args.data_lake_path
    purge_intervalle = args.purge_intervalle
    
    spark = SparkSession.builder.appName("DataLakePurge").getOrCreate()
    
    {
        purge_data(
            spark=spark,
            data_lake_path=data_lake_path,
            table_name=table_name,
            purge_intervalle=purge_intervalle
        )
        for table_name in list_table
    }
