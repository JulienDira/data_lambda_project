from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder \
    .appName("DeltaLakeAppendOnly") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

input_path = "/app/data_lake/logs"
delta_path = "/app/data_lake/delta"

# Charger les nouvelles données (format JSON)
new_data = spark.read.json(input_path)

# Essayer d’écrire en append, ou créer la table si elle n’existe pas
try:
    # Si la table Delta existe, on append
    new_data.write.format("delta").mode("append").save(delta_path)
except Exception as e:
    # Si la table n’existe pas, création
    new_data.write.format("delta").mode("overwrite").save(delta_path)

# Lire la table Delta
delta_df = spark.read.format("delta").load(delta_path)
delta_df.show()
spark.stop()
