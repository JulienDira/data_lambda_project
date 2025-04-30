#!/bin/bash
set -e

echo "Démarrage de consumer_transaction_log.py"
spark-submit \
  --master local[*] \
  --jars "$SHARED_JARS" \
  consumer_transaction_log.py >> /app/log/consumer_transaction_log.log 2>&1 &

echo "Démarrage de consumer_transaction_flattened.py"
spark-submit \
  --master local[*] \
  --jars "$SHARED_JARS" \
  consumer_transaction_flattened.py >> /app/log/consumer_transaction_flattened.log 2>&1 &

# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.kafka:kafka-clients:3.4.1 \

echo "Les jobs Spark Streaming sont en cours d'exécution. Conteneur actif."
tail -f /dev/null  # Cela garde le conteneur en vie
