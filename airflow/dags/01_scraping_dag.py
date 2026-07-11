from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import glob
import os
import json
from utils.bfs_scraper_universal import bfs_scrape_universal

from utils.openalex_ingest import ingest_openalex
from utils.crossref_ingest import ingest_crossref

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

CONFIG_DIR = '/opt/airflow/configs'

# Récupération des fichiers de configuration
config_files = glob.glob(os.path.join(CONFIG_DIR, '*.json'))

with DAG('scraping_pipeline', default_args=default_args,
         schedule_interval='@weekly', catchup=False) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    # ==================== APIS ====================
    task_openalex = PythonOperator(
        task_id='ingest_openalex',
        python_callable=ingest_openalex,
        op_kwargs={'max_works': 5000}
    )
    task_crossref = PythonOperator(
        task_id='ingest_crossref',
        python_callable=ingest_crossref,
        op_kwargs={'max_works': 2000}
    )
    
    
    # ==================== SCRAPING WEB ====================
    scrape_tasks = []
    for config_path in config_files:
        domain_name = os.path.basename(config_path).replace('.json', '')
        task = PythonOperator(
            task_id=f'scrape_{domain_name}',
            python_callable=bfs_scrape_universal,   # ← fonction unique
            op_kwargs={'config_path': config_path}
        )
        scrape_tasks.append(task)
    
    # ==================== FICHIER LOCAL ====================
    def ingest_local_file_if_exists():
        from utils.minio_client import upload_bytes, compute_hash
        csv_path = '/opt/airflow/data/sample_courses.csv'
        if not os.path.exists(csv_path):
            print(f"Fichier CSV non trouvé : {csv_path}. Ignorer l'ingestion.")
            return
        with open(csv_path, 'rb') as f:
            content = f.read()
        metadata = {
            "source_system": "local_file",
            "crawl_timestamp": datetime.now().isoformat(),
            "content_hash": compute_hash(content)
        }
        upload_bytes('raw-documents', 'local/courses.csv', content, metadata)
        print("Fichier CSV importé avec succès")
    
    task_local_file = PythonOperator(
        task_id='ingest_local_csv_if_exists',
        python_callable=ingest_local_file_if_exists
    )
    
    # ==================== DÉPENDANCES ====================
    start >> [task_openalex, task_crossref]
    for t in scrape_tasks:
        start >> t
    
    all_api_done = DummyOperator(task_id='all_api_done')
    [task_openalex, task_crossref] >> all_api_done
    
    all_scraping_done = DummyOperator(task_id='all_scraping_done')
    for t in scrape_tasks:
        t >> all_scraping_done
    
    all_ingestion_done = DummyOperator(task_id='all_ingestion_done')
    [all_api_done, all_scraping_done] >> all_ingestion_done
    
    all_ingestion_done >> task_local_file >> end