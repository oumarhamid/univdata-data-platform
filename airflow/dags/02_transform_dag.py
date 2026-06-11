from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG('transform_pipeline', default_args=default_args,
         schedule_interval='@daily', catchup=False) as dag:
    
    # Soumettre le job Spark (en utilisant spark-submit)
    # On suppose que les jars Hudi et Elasticsearch sont déjà dans l'image Spark
    spark_submit = BashOperator(
        task_id='run_spark_etl',
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.14.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.10.0 \
            /opt/spark/jobs/etl_to_hudi.py
        """
    )
    
    spark_submit