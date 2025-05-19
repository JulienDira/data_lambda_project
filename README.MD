# Projet Data Lake & Data Warehouse

Ce projet implémente une infrastructure de traitement de données en temps réel à l’aide de **Kafka**, **KSQL**, **PostgreSQL**, et **Docker**. Il utilise des **consumers** pour lire des **streams** et des **tables**, puis écrit ces données respectivement dans un **Data Lake** et un **Data Warehouse**. Le projet est entièrement orchestré avec **Docker Compose**.

Ce repo est égalememnt composé du projet d'**API 1** ainsi que d'**API 2** qui se trouvent dans leurs dossiers respectifs. L'**API 2** est également compososé d'un **README** et d'une documentation via **swagger** en vue de sa compléxitée.

## Structure du Projet

Voici la structure du projet :

```bash
.
├── .gitignore                # Fichiers à ignorer
├── create_user_table.sql     # Création table utilisateur
├── docker-compose.yml        # Déploiement infrastructure
├── structure.txt             # Structure du projet
├── api_tp_1/                      # API du projet 1 sur les API
├── api_tp_2/                      # API du projet 2 sur les API (intéragir avec le data lake)
├── consumer-raw/             # Consumers pour Data Lake
│   ├── docker-compose.yaml   # Compose pour Data Lake
│   ├── Dockerfile.consumer-raw # Dockerfile Data Lake
│   ├── entrypoint_raw.sh     # Entrée pour consumers
│   ├── schema.py             # Schéma des streams
│   ├── write_stream_to_parquet.py # Ecriture Parquet
│   ├── old_script/           # Scripts obsolètes
│   └── purge_data/           # Purge des données
│       ├── data_purge.py     # Purge des anciennes données
│       ├── Dockerfile        # Dockerfile purge
│       └── entrypoint.sh     # Entrée pour purge
├── consumer-table/           # Consumers pour Data Warehouse
│   ├── create_table_sql_queries.sql # Création des tables
│   ├── docker-compose.yaml   # Compose pour Data Warehouse
│   ├── Dockerfile.writer-table # Dockerfile Data Warehouse
│   ├── entrypoint_table.sh   # Entrée pour consumers
│   ├── schema.py             # Schéma des tables
│   ├── write_table_to_postgres.py # Ecriture PostgreSQL
│   └── old_script/           # Scripts obsolètes
├── data_lake/                # Stockage Data Lake
├── db_tp/                    # Stockage Data Warehouse
├── log/                      # Logs des applications
│   ├── consumer/             # Logs Data Lake
│   └── writer/               # Logs Data Warehouse
└── producer/                 # Producteur Kafka
    ├── kafka_producer_transaction.py # Producteur de transactions
    └── ksql_queries.sql      # Requêtes KSQL
```

## Description des Composants

### 1. **docker-compose.yml** (à la racine)

Le fichier `docker-compose.yml` à la racine du projet sert à déployer l'infrastructure de la stack, incluant :
- **Kafka** : pour la gestion des messages et des streams.
- **KSQL** : pour le traitement des flux de données en SQL.
- **PostgreSQL** : pour le Data Warehouse.
- **Confluent** : pour l'intégration avec Kafka.
  
Il gère également les volumes montés, les environnements et l’orchestration des containers.

### 2. **create_user_table.sql**

Le fichier `create_user_table.sql` contient le script SQL nécessaire pour créer la table des permissions des utilisateurs dans **PostgreSQL**. Ce script est essentiel pour la gestion des utilisateurs et leurs droits d’accès aux données du Data Warehouse.

### 3. **consumer-raw** - Application des Consumers de Streams

Le dossier `consumer-raw` est dédié aux **consumers** qui traitent les **streams** de données provenant de Kafka. Ces données sont ensuite écrites en **Parquet** dans le **Data Lake**.

- `docker-compose.yaml` : Déclare les services nécessaires pour le traitement des streams (Kafka, Spark, etc.).
- `Dockerfile.consumer-raw` : Contient l’image Docker spécifique aux consumers de stream.
- `entrypoint_raw.sh` : Script d’entrée pour démarrer les consumers dans le conteneur Docker.
- `schema.py` : Contient la définition du schéma des streams de données.
- `write_stream_to_parquet.py` : Application Python pour écrire les données des streams en format Parquet dans le Data Lake.
- `old_script/` : Contient des scripts anciens ou obsolètes (si nécessaire).
- `purge_data/` : Contient les scripts pour purger les données anciennes du Data Lake.
  - `data_purge.py` : Script Python pour effectuer la purge.
  - `Dockerfile` : Dockerfile associé à la purge des données.
  - `entrypoint.sh` : Script d’entrée pour démarrer la purge dans le conteneur.

### 4. **consumer-table** - Application des Consumers de Tables

Le dossier `consumer-table` est dédié aux **consumers** qui traitent les **tables** provenant de Kafka, puis écrivent ces données dans **PostgreSQL**.

- `create_table_sql_queries.sql` : Contient les requêtes SQL nécessaires pour créer des tables dans **PostgreSQL**.
- `docker-compose.yaml` : Déclare les services nécessaires pour traiter les tables (Kafka, Spark, etc.).
- `Dockerfile.writer-table` : Contient l’image Docker pour les consumers de tables.
- `entrypoint_table.sh` : Script d’entrée pour démarrer les consumers de tables dans le conteneur.
- `schema.py` : Définit le schéma des tables à ingérer.
- `write_table_to_postgres.py` : Application Python pour écrire les données des tables dans PostgreSQL.
- `old_script/` : Contient des scripts anciens ou obsolètes (si nécessaire).

### 5. **data_lake et db_tp** - Espaces de Stockage

Les dossiers `data_lake` et `db_tp` sont des volumes montés dans Docker pour stocker les données écrites par les consumers.

- **data_lake** : Volume où les données des streams sont stockées sous forme de fichiers Parquet.
- **db_tp** : Volume où les données des tables sont stockées dans PostgreSQL.

### 6. **log** - Logs des Applications

Le dossier `log` contient les logs des applications de traitement des données.

- `consumer/` : Logs générés par les consumers de streams.
- `writer/` : Logs générés par les writers pour l’écriture dans PostgreSQL.

### 7. **producer** - Producteur de Données

Le dossier `producer` contient les éléments liés au producteur de données Kafka.

- `kafka_producer_transaction.py` : Script Python pour produire des messages dans les topics Kafka.
- `ksql_queries.sql` : Fichier contenant des requêtes KSQL utilisées pour interroger et manipuler les données dans Kafka.

## Déploiement

Pour déployer l'infrastructure, il suffit de lancer le fichier `docker-compose.yml` à la racine du projet :

```bash
docker-compose up -d
```
