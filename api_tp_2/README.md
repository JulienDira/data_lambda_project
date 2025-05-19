# Crée un environnement virtuel (optionnel mais recommandé)
python -m venv env
source env/bin/activate  # ou .\env\Scripts\activate sous Windows

# Installe Django et les packages utiles
pip install -r requierement.txt

# Crée ton projet
django-admin startproject datalake_access
cd datalake_access

# Crée les apps nécessaires
python manage.py startapp permissions
python manage.py startapp audit
python manage.py startapp users


admin
djangoadmin
