from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import glob
import os
import json

from utils.openalex_ingest import ingest_openalex

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

CONFIG_DIR = '/opt/airflow/configs'

def scrape_router(config_path, **context):
    """Charge le fichier JSON et appelle le scraper adapté (statique ou Playwright)."""
    with open(config_path, 'r') as f:
        config = json.load(f)
    use_playwright = config.get('use_playwright', False)
    if use_playwright:
        from utils.bfs_scraper_playwright import bfs_scrape_with_playwright
        return bfs_scrape_with_playwright(config_path, **context)
    else:
        from utils.bfs_scraper import bfs_scrape_from_config
        return bfs_scrape_from_config(config_path, **context)

config_files = glob.glob(os.path.join(CONFIG_DIR, '*.json'))

with DAG('scraping_pipeline', default_args=default_args,
         schedule_interval='@daily', catchup=False) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    task_openalex = PythonOperator(
        task_id='ingest_openalex',
        python_callable=ingest_openalex
    )
    
    scrape_tasks = []
    for config_path in config_files:
        domain_name = os.path.basename(config_path).replace('.json', '')
        task = PythonOperator(
            task_id=f'scrape_{domain_name}',
            python_callable=scrape_router,
            op_kwargs={'config_path': config_path}
        )
        scrape_tasks.append(task)
    
    def ingest_local_file_if_exists():
        from utils.minio_client import upload_bytes, compute_hash
        import os
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
    
    start >> task_openalex
    for t in scrape_tasks:
        start >> t
    
    all_done = DummyOperator(task_id='all_scraping_and_openalex_done')
    all_scraping_and_openalex = [task_openalex] + scrape_tasks
    for t in all_scraping_and_openalex:
        t >> all_done
    
    all_done >> task_local_file >> end