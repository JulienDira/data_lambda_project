#!/bin/bash
set -e

JARS_PATH="/spark/jars"
JARS=$(find $JARS_PATH -name "*.jar" | paste -sd "," -)

# Configuration des packages Kafka pour Spark
# KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},org.apache.kafka:kafka-clients:${SPARK_KAFKA_VERSION},org.apache.commons:commons-pool2:2.11.1"

echo "=== Configuration de l'environnement Spark ==="
echo "Version Spark: ${SPARK_VERSION}"
echo "Version Kafka: ${SPARK_KAFKA_VERSION}"
echo "=== Démarrage des applications Spark ==="

# Créer le répertoire de logs s'il n'existe pas
mkdir -p /app/log

# Parcours tous les fichiers .py dans le répertoire /app/consumer-raw/
for PY_FILE in /app/consumer-raw/*.py; do
  BASENAME=$(basename "$PY_FILE")
  if [ "$BASENAME" = "common_function.py" ]; then
    continue  # Ignore ce fichier
  fi

  echo "Démarrage de $BASENAME"
  /opt/spark/bin/spark-submit \
    --master local[*] \
    --jars "$JARS" \
    --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
    --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
    --conf "spark.executor.memory=2g" \
    --conf "spark.driver.memory=2g" \
    "$PY_FILE" >> /app/log/consumer/"$(basename "$PY_FILE" .py)".log 2>&1 &
done


echo "=== Les jobs Spark Streaming sont en cours d'exécution ==="
echo "=== Logs disponibles dans le répertoire /app/log ==="
echo "=== Conteneur actif ==="

# Garder le conteneur en vie
tail -f /dev/null
