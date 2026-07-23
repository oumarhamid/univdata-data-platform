from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=180),
}

with DAG('export_to_postgres_pipeline',
         default_args=default_args,
         schedule_interval=None,          # ou None si déclenché manuellement
         catchup=False,
         max_active_runs=1) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Commande spark-submit pour exécuter export_to_postgres.py
    # Ajustez les mémoires si l'export est plus léger (par ex. driver 512m, executor 1g)
    spark_submit_cmd = """
    sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 1g \
      --executor-memory 1536m \
      --executor-cores 2 \
      --conf spark.cores.max=2 \
      --conf spark.sql.shuffle.partitions=8 \
      --conf spark.default.parallelism=8 \
      --conf spark.memory.fraction=0.6 \
      --conf spark.memory.storageFraction=0.3 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.sql.adaptive.coalescePartitions.enabled=true \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.executor.heartbeatInterval=60s \
      --conf spark.network.timeout=300s \
      --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0,org.postgresql:postgresql:42.5.1 \
      /opt/spark/jobs/export_to_postgres.py
    """

    export_task = BashOperator(
        task_id='export_to_postgres',
        bash_command=spark_submit_cmd,
        execution_timeout=timedelta(minutes=180)
    )

    start >> export_task >> end