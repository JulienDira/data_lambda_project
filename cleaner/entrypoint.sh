#!/bin/bash
set -e

JARS_PATH="/spark/jars"
JARS=$(find "$JARS_PATH" -name "*.jar" | paste -sd "," -)

echo "=== Configuration de l'environnement Spark ==="
echo "Fichier Python $PY_FILE pour le topic $KAFKA_TOPIC"
echo "JARs: $JARS"

mkdir -p /app/log/writer

echo "Démarrage de $PY_FILE > $KAFKA_TOPIC"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.memory=2g" \
  --conf "spark.executor.memory=2g" \
  "/app/cleaner/$PY_FILE"
  # >> "/app/log/cleaner/$(basename $KAFKA_TOPIC).log" 2>&1

echo "=== Job terminé (ou en streaming continu) ==="

tail -f /dev/null