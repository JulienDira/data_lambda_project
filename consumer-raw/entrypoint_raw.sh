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

echo "Démarrage de consumer_transaction_log.py"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  ./consumer-raw/consumer_transaction_log.py >> /app/log/consumer/transaction_log.log 2>&1 &

echo "Démarrage de consumer_transaction_flattened.py"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  ./consumer-raw/consumer_transaction_flattened.py >> /app/log/consumer/transaction_flattened.log 2>&1 &

echo "=== Les jobs Spark Streaming sont en cours d'exécution ==="
echo "=== Logs disponibles dans le répertoire /app/log ==="
echo "=== Conteneur actif ==="

# Afficher les logs en temps réel (optionnel)
# tail -f /app/log/*.log

# Garder le conteneur en vie
tail -f /dev/null