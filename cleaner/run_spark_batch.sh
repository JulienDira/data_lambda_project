#!/bin/bash
set -e

BASENAME=$(basename "$PY_FILE" .py)

# Définir les JARs Delta Lake 2.4.0 compatibles avec Spark 3.5.0
DELTA_JARS="/spark/jars/delta-core_2.12-3.1.0.jar,/spark/jars/delta-storage-3.1.0.jar,/spark/jars/delta-spark_2.12-3.1.0.jar"

cat <<EOF > /tmp/mycron
* * * * * echo "cron tick: \$(date)" >> /var/log/cron/cron.log
* * * * * /opt/spark/bin/spark-submit \
  --jars ${DELTA_JARS} \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  "/app/cleaner/${PY_FILE}" >> "/var/log/cron/${BASENAME}.log" 2>&1
EOF

crontab /tmp/mycron

cron -f