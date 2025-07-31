from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from ckanapi import RemoteCKAN
import logging
import requests
from xml.etree import ElementTree as ET
import json

# Configuration
CKAN_URL = 'http://localhost:5001'
CKAN_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJUdHBSM1RDWnlKdTJVb0ZsUjlvV0dTbWJxaUZ5eTNTZFdFay0tQngzbDhJIiwiaWF0IjoxNzI0MTY4NDg0fQ.5P9BNeehIi4aU5pSBWsUoZW0ICqivVma4_H81kr6gco'
SDMX_PROVIDER = 'ESTAT'  # Eurostat
## SDMX_REST_URL = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1'
SDMX_REST_URL = 'http://opendata.ansd.sn/admin/ws/nsi_ws/rest/'
LOG_FILE = '/var/log/ckan_sdmx_import.log'

# Initialisation logger
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_ckan():
    """Connexion à CKAN"""
    return RemoteCKAN(CKAN_URL, apikey=CKAN_API_KEY)


def fetch_eurostat_datasets():
    ## url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all?format=json"
    url = "http://opendata.ansd.sn/admin/ws/nsi_ws/rest/"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur lors de la récupération des données Eurostat: {response.status_code}")
        return None

def extract_metadata(dataset):
    extension = dataset['extension']
    metadata = {
        'id': extension['id'],
        'title': dataset['label'],
        'description': next((a['title'] for a in extension['annotation'] if a['type'] == 'ESMS_HTML'), ''),
        'source': next((a['text'] for a in extension['annotation'] if a['type'] == 'SOURCE_INSTITUTIONS'), ''),
        'keywords': [a['text'] for a in extension['annotation'] if a['type'] == 'LINK'],
        'created': next((a['date'] for a in extension['annotation'] if a['type'] == 'CREATED'), ''),
        'updated': next((a['date'] for a in extension['annotation'] if a['type'] == 'UPDATE_DATA'), ''),
        'doi': next((a['title'] for a in extension['annotation'] if a['type'] == 'DISSEMINATION_DOI_XML'), '')
    }
    return metadata


def create_ckan_dataset(ckan_url, ckan_api_key, metadata):
    ckan = RemoteCKAN(ckan_url, apikey=ckan_api_key)

    try:
        ckan.action.package_create(
            name=metadata['id'].lower(),
            title=metadata['title'],
            notes=metadata['description'],
            tags=[{'name': tag} for tag in metadata['keywords']],
            extras=[
                {'key': 'source', 'value': metadata['source']},
                {'key': 'created', 'value': metadata['created']},
                {'key': 'updated', 'value': metadata['updated']},
                {'key': 'doi', 'value': metadata['doi']}
            ]
        )
        print(f"Dataset créé: {metadata['title']} (ID: {metadata['id']})")
    except Exception as e:
        print(f"Erreur lors de la création du dataset {metadata['id']}: {e}")


def run_pipeline(ckan_url, ckan_api_key):
    data = fetch_eurostat_datasets()
    if not data or 'item' not in data['link']:
        print("Aucun dataset trouvé.")
        return

    for dataset in data['link']['item']:
        metadata = extract_metadata(dataset)
        create_ckan_dataset(ckan_url, ckan_api_key, metadata)

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
    'eurostat_to_ckan_pipeline',
    default_args=default_args,
    description='Pipeline pour importer les jeux de données Eurostat dans CKAN',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

run_task = PythonOperator(
    task_id='run_eurostat_ckan_pipeline',
    python_callable=run_pipeline(ckan_url=CKAN_URL, ckan_api_key=CKAN_API_KEY),
    dag=dag,
)

run_task


# with DAG(
#     dag_id='ckan_sdmx_pipeline',
#     start_date=datetime(2025, 1, 1),
#     schedule_interval=None,
# ) as dag:
#     task = DummyOperator(task_id='dummy_task')