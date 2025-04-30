#!/bin/bash
set -e

# Configuration des packages Kafka pour Spark
KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},org.apache.kafka:kafka-clients:${SPARK_KAFKA_VERSION},org.apache.commons:commons-pool2:2.11.1"

echo "=== Configuration de l'environnement Spark ==="
echo "Version Spark: ${SPARK_VERSION}"
echo "Version Kafka: ${SPARK_KAFKA_VERSION}"
echo "=== Démarrage des applications Spark ==="

# Créer le répertoire de logs s'il n'existe pas
mkdir -p /app/log

# echo "Démarrage de consumer_transaction_log.py"
# /opt/spark/bin/spark-submit \
#   --master local[*] \
#   --packages "${KAFKA_PACKAGES}" \
#   --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
#   --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
#   --conf "spark.executor.memory=2g" \
#   --conf "spark.driver.memory=2g" \
#   consumer_transaction_log.py >> /app/log/consumer_transaction_log.log 2>&1 &

# echo "Démarrage de consumer_transaction_flattened.py"
# /opt/spark/bin/spark-submit \
#   --master local[*] \
#   --packages "${KAFKA_PACKAGES}" \
#   --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
#   --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
#   --conf "spark.executor.memory=2g" \
#   --conf "spark.driver.memory=2g" \
#   consumer_transaction_flattened.py >> /app/log/consumer_transaction_flattened.log 2>&1 &

echo "Démarrage de writter_TOTAL_SPENT_PER_USER_TRANSACTION_TYPE.py (si présent)"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages "${KAFKA_PACKAGES}" \
  --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
  --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  writter_TOTAL_SPENT_PER_USER_TRANSACTION_TYPE.py >> /app/log/writter_TOTAL_SPENT_PER_USER_TRANSACTION_TYPE.log 2>&1 &

echo "=== Les jobs Spark Streaming sont en cours d'exécution ==="
echo "=== Logs disponibles dans le répertoire /app/log ==="
echo "=== Conteneur actif ==="

# Afficher les logs en temps réel (optionnel)
# tail -f /app/log/*.log

# Garder le conteneur en vie
tail -f /dev/null