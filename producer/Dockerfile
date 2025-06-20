FROM spark:3.5.0-python3

USER root

# Mettre à jour et installer les dépendances nécessaires
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installer les packages Python nécessaires
RUN pip install psycopg2-binary kafka-python

# Vérifier l'emplacement des JARs Spark
# (Ajustez ces chemins selon la structure de votre image)
RUN mkdir -p /spark/jars

# Télécharger les JARs spécifiques nécessaires
RUN cd /spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.23/postgresql-42.2.23.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Créer les répertoires nécessaires
RUN mkdir -p /app/log /app/checkpoints /app/producer

# Définir la variable d'environnement pour les JARS
ENV SPARK_KAFKA_VERSION=3.3.1
ENV SPARK_VERSION=3.5.0
# Assurez-vous que ce chemin correspond à l'emplacement réel des JARs dans votre image
ENV SPARK_JARS_DIR=/spark/jars

# Copier les fichiers de l'application
COPY ./ /app/producer

# Définir le répertoire de travail
WORKDIR /app

# Rendre le script de démarrage exécutable
RUN chmod +x /app/producer/entrypoint.sh
