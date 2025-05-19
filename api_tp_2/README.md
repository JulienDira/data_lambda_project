# 📦 Streaming API Project – Documentation & Structure

Ce projet Django expose une API sécurisée pour interagir avec un data lake, fournir des fonctionnalités analytiques, de recherche, de permissions et de machine learning. Il repose sur **Django REST Framework**, **Swagger (`drf-yasg`)**, **Kafka**, **Whoosh**, et **Parquet**.

---

## 🚀 Fonctionnalités principales

- 🔍 Recherche plein texte dans les fichiers du data lake
- 📊 Statistiques et agrégation sur les données
- 🔐 Permissions d’accès par utilisateur et dossier
- 🧠 Entraînement d’un modèle ML (Random Forest)
- 📤 Republier des transactions dans Kafka
- 🧾 Audit automatique des requêtes

---

## 📁 Structure du projet

### `permissions/`
Gère les droits d’accès aux dossiers du data lake.

- `models.py` : Modèle `UserPermission`
- `serializers.py` : Sérialiseur pour les permissions
- `views.py` :
  - `GrantPermissionView` : Accorder une permission
  - `RevokePermissionView` : Révoquer une permission
  - `UserPermissionViewSet` : CRUD des permissions
- 🔐 Utilise `IsAuthenticated` + `CanAccessTablePermission`

---

### `datalake/`
Contient les vues principales d’accès et de manipulation des données du data lake.

- `views.py` :
  - `RetrieveTableView` : Lire une table parquet avec pagination, filtres, sélection de colonnes
  - `MetricsView` : Générer des métriques globales sur une table (total, top produits…)
  - `ListResourcesView` : Explorer les ressources disponibles dans le data lake
- 🧼 Utilise `pyarrow.dataset`, `pandas`, `numpy`
- 🔐 Intègre la permission personnalisée `CanAccessTablePermission`

---

### `advanced/`
Expose des vues avancées de manipulation et d'analyse des données.

- `views.py` :
  - `FullTextSearchView` : Recherche dans les fichiers via **Whoosh**
  - `TrainModelRPC` : Entraîne un modèle `RandomForestClassifier`
  - `RepushTransaction`, `RepushAll` : Publier des transactions dans Kafka
- 📚 Swagger intégré avec `drf-yasg` (`swagger_auto_schema`)
- 🔄 Intégration Kafka + Machine Learning

---

### `datalake_access/`
Dossier central contenant les **fichiers de configuration Django** et les vues avancées de manipulation.

- `settings.py`, `urls.py`, `wsgi.py`, etc.

---

### `audit/`
Gère la **journalisation automatique** des accès API par utilisateur.

- `middleware.py` : Middleware `AuditMiddleware` qui enregistre :
  - Utilisateur, méthode, endpoint, corps de requête
- `models.py` : Modèle `AccessLog` pour stocker chaque requête
- 🧾 Très utile pour le suivi de conformité, de sécurité ou debug

---

### `index_data.py`
Ce script Python permet de créer un index plein texte à partir de tous les fichiers .parquet du data lake, grâce à la bibliothèque Whoosh.

🔧 Fonctionnement :

- Parcourt récursivement les fichiers .parquet dans le dossier data_lake
- Lit chaque fichier avec Pandas
- Convertit le contenu en texte brut
- Indexe le chemin (path) et le contenu (content) dans un index Whoosh

Le dossier index/ est automatiquement créé (s’il n’existe pas), et contient l’index exploité par FullTextSearchView.

---

### `index/`
Contient l’index **Whoosh** utilisé pour la recherche plein texte sur le contenu du data lake.

- Généré dynamiquement à partir des fichiers
- Utilisé dans `FullTextSearchView`
- 📁 Indexé par ingestion (`ingestion_date=...`)

---

### `users/`
Ce dossier est actuellement **vide**. Il pourrait à terme :

- Centraliser la logique de gestion d’utilisateurs personnalisés
- Étendre le modèle utilisateur de Django
- Gérer l’authentification ou les profils

⚠️ **Note** : Ce dossier peut être supprimé ou intégré plus tard si besoin.

---

## 📘 Documentation Swagger (drf-yasg)

Une documentation interactive est disponible :

📍 [http://localhost:8000/swagger/](http://localhost:8000/swagger/)

Elle documente :
- Les paramètres d’entrée (`query`, `body`)
- Les schémas de réponse
- Les permissions requises

---

## ✅ Prérequis

- Python 3.9+
- Django 4.x
- Kafka (local ou distant)
- Whoosh
- Pandas, NumPy, Scikit-Learn
- PyArrow (lecture Parquet)
- drf-yasg (pour Swagger)

---

## 🛠️ Démarrer le projet

```bash
# Installer les dépendances
pip install -r requirements.txt

# Appliquer les migrations
python manage.py migrate

# Lancer le serveur
python manage.py runserver

# Accéder à la documentation Swagger
http://localhost:8000/swagger/
