from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from ckanapi import RemoteCKAN
from pandasdmx import Request

# Configuration
CKAN_URL = 'http://localhost:5001'
CKAN_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJUdHBSM1RDWnlKdTJVb0ZsUjlvV0dTbWJxaUZ5eTNTZFdFay0tQngzbDhJIiwiaWF0IjoxNzI0MTY4NDg0fQ.5P9BNeehIi4aU5pSBWsUoZW0ICqivVma4_H81kr6gco'
SDMX_PROVIDER = 'ESTAT'  # Exemple : ESTAT, ECB, etc.
LOG_FILE = '/var/log/ckan_sdmx_import.log'

# Initialisation logger
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Connexion à CKAN
def connect_to_ckan():
    return RemoteCKAN(CKAN_URL, apikey=CKAN_API_KEY)

# Extraction des données SDMX
def fetch_sdmx_data(provider):
    try:
        estat = Request(provider)
        response = estat.dataflow()  # ou .data() selon votre source
        return response
    except Exception as e:
        logging.error(f"Erreur lors de la récupération SDMX ({provider}): {e}")
        raise

# Création d'un jeu de données dans CKAN
def create_ckan_dataset(key, dataset_title, metadata, ckan_client):
    try:
        ckan_client.action.package_create(
            name=f"{key.lower()}",
            title=dataset_title,
            notes=metadata.get('description', ''),
            tags=[{'name': 'sdmx'}, {'name': metadata.get('agencyID', '').lower()}],
            extras=[
                {'key': 'source', 'value': metadata.get('source', '')},
                {'key': 'keywords', 'value': ', '.join(metadata.get('keywords', []))},
                {'key': 'period', 'value': metadata.get('period', '')}
            ]
        )
        logging.info(f"Dataset créé: {dataset_title} (ID: {key})")
    except Exception as e:
        logging.error(f"Erreur création CKAN dataset {key}: {e}")

# Pipeline principal
def run_pipeline():
    logging.info("Début du pipeline SDMX → CKAN")
    ckan = connect_to_ckan()
    dataflows = fetch_sdmx_data(SDMX_PROVIDER)

    for key, df in dataflows.items():
        dataset_title = df.name.en if hasattr(df, 'name') and hasattr(df.name, 'en') else key
        metadata = {
            'description': df.description.en if hasattr(df, 'description') and hasattr(df.description, 'en') else '',
            'agencyID': df.agencyID if hasattr(df, 'agencyID') else '',
            'source': df.source if hasattr(df, 'source') else '',
            'keywords': [tag for tag in dir(df) if not callable(getattr(df, tag)) and not tag.startswith('__')],
            'period': df.dimensions.get('time', '')
        }

        create_ckan_dataset(key, dataset_title, metadata, ckan)

    logging.info("Pipeline terminé avec succès.")

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ckan_sdmx_pipeline',
    default_args=default_args,
    description='Pipeline d\'intégration SDMX vers CKAN',
    schedule_interval=timedelta(days=1),  # Changer selon besoin
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

run_pipeline_task = PythonOperator(
    task_id='run_sdmx_ckan_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

run_pipeline_task