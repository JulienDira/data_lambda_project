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

echo "Démarrage de AMOUNT_PER_TYPE_WINDOWED.py"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  ./consumer-table/writter_AMOUNT_PER_TYPE_WINDOWED.py >> /app/log/writter/AMOUNT_PER_TYPE_WINDOWED.log 2>&1 &

echo "Démarrage de writter_COUNT_NUMB_BUY_PER_PRODUCT.py"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  ./consumer-table/writter_COUNT_NUMB_BUY_PER_PRODUCT.py >> /app/log/writter/COUNT_NUMB_BUY_PER_PRODUCT.log 2>&1 &

echo "Démarrage de writter_TOTAL_SPENT_PER_USER_TRANSACTION_TYPE.py"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  ./consumer-table/writter_TOTAL_SPENT_PER_USER_TRANSACTION_TYPE.py >> /app/log/writter/TOTAL_SPENT_PER_USER_TRANSACTION_TYPE.log 2>&1 &

echo "Démarrage de writter_TOTAL_TRANSACTION_AMOUNT_PER_PAYMENT_METHOD.py"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  ./consumer-table/writter_TOTAL_TRANSACTION_AMOUNT_PER_PAYMENT_METHOD.py >> /app/log/writter/TOTAL_TRANSACTION_AMOUNT_PER_PAYMENT_METHOD.log 2>&1 &

echo "Démarrage de writter_TRANSACTION_STATUS_EVOLUTION.py"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=2g" \
  ./consumer-table/writter_TRANSACTION_STATUS_EVOLUTION.py >> /app/log/writter/TRANSACTION_STATUS_EVOLUTION.log 2>&1 &

echo "=== Les jobs Spark Streaming sont en cours d'exécution ==="
echo "=== Logs disponibles dans le répertoire /app/log ==="
echo "=== Conteneur actif ==="

# Afficher les logs en temps réel (optionnel)
# tail -f /app/log/*.log

# Garder le conteneur en vie
tail -f /dev/null