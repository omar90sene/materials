# Use the official Airflow image as base
FROM apache/airflow:2.5.1

# Copie ton requirements.txt
COPY requirements.txt .

# Installe les dépendances via pip (en tant que root, temporairement)
RUN pip install --no-cache-dir -r requirements.txt

# Nettoyer
RUN rm requirements.txt

# Revenir à l'utilisateur airflow (recommandé)