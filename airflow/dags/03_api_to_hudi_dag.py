from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=60),
}

with DAG('03_api_to_hudi_pipeline',
         default_args=default_args,
         schedule_interval=None,  # Désactivé pour éviter les exécutions automatiques "fantômes"
         catchup=False,
         max_active_runs=1) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Commande Bash avec limitation stricte de la RAM (512m) pour ton environnement local
    spark_submit_cmd = """
    sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.driver.memory=512m \
      --conf spark.executor.memory=512m \
      --conf spark.sql.shuffle.partitions=4 \
      --conf spark.default.parallelism=4 \
      --conf spark.memory.fraction=0.8 \
      --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
      /opt/spark/jobs/etl_apis_to_hudi.py
    """

    etl_apis_to_hudi = BashOperator(
        task_id='etl_api_to_hudi_via_docker',
        bash_command=spark_submit_cmd,
    )

    start >> etl_apis_to_hudi >> end