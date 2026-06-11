from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
}

with DAG('index_elasticsearch', default_args=default_args,
         schedule_interval='@daily', catchup=False) as dag:
    
    index_job = BashOperator(
        task_id='index_to_elasticsearch',
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark/jobs/index_elastic.py
        """
    )
    
    index_job