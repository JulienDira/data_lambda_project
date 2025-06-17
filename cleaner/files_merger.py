import shutil
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Merger").getOrCreate()

def merge_small_files(input_dir, temp_dir, output_dir):
    input_path = Path(input_dir)
    temp_path = Path(temp_dir)
    
    # Copier les fichiers JSON dans temp_dir
    temp_path.mkdir(exist_ok=True)
    files_to_merge = list(input_path.glob("*.json"))
    for f in files_to_merge:
        shutil.copy(f, temp_path / f.name)
    
    # Lire et fusionner depuis temp_dir
    df = spark.read.json(str(temp_path))
    df.coalesce(1).write.mode("overwrite").json(output_dir)
    
    # Supprimer tous les fichiers JSON et CRC dans le dossier d'entrée
    for f in input_path.glob("*"):
        if f.suffix in [".json", ".crc"]:
            try:
                f.unlink()
            except Exception as e:
                print(f"Erreur en supprimant {f}: {e}")
    
    # Supprimer le dossier temporaire
    shutil.rmtree(temp_path)

merge_small_files("/app/data_lake/logs", "/app/data_lake/logs_to_merge", "/app/data_lake/merged")
spark.stop()
