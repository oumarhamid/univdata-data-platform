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

with DAG('html_to_hudi_pipeline',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         max_active_runs=1) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Ajout de "sudo" au tout début de la commande
    spark_submit_cmd = """
    sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.driver.memory=1g \
      --conf spark.executor.memory=1g \
      --conf spark.sql.shuffle.partitions=10 \
      --conf spark.default.parallelism=10 \
      --conf spark.memory.fraction=0.8 \
      --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
      /opt/spark/jobs/etl_html_to_hudi.py
    """

    etl_html = BashOperator(
        task_id='etl_html_to_hudi_via_docker',
        bash_command=spark_submit_cmd,
        execution_timeout=timedelta(minutes=120)
    )

    start >> etl_html >> end