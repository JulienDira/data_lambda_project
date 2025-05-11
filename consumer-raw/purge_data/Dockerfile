FROM spark:3.5.0-python3

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Installation des dépendances Python
RUN pip install pyspark==3.3.0

# Création du répertoire de travail
WORKDIR /app

# Copie du script PySpark
COPY purge_data.py /app/

# Configuration de l'environnement Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Création du fichier crontab
RUN echo "0 0 * * * cd /app && spark-submit /app/purge_data.py --tables table1 table2 --data_lake_path /chemin/vers/datalake --purge_intervalle 30 >> /var/log/cron.log 2>&1" > /etc/cron.d/purge-cron
RUN chmod 0644 /etc/cron.d/purge-cron

# Création du fichier de log
RUN touch /var/log/cron.log

# Copie du script d'entrée
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Commande par défaut qui lance le script d'entrée
CMD ["/app/entrypoint.sh"]