from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),  # ⬅️ réduit à 30 min pour éviter les timeouts trop longs
}

with DAG('06_index_elasticsearch_pipeline',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         max_active_runs=1,
         tags=['elasticsearch', 'index']) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Commande optimisée pour réduire la mémoire et augmenter les timeouts
    spark_submit_cmd = """
    sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 512m \
      --executor-memory 1024m \
      --executor-cores 1 \
      --conf spark.cores.max=1 \
      --conf spark.sql.shuffle.partitions=4 \
      --conf spark.default.parallelism=4 \
      --conf spark.memory.fraction=0.5 \
      --conf spark.memory.storageFraction=0.3 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.sql.adaptive.coalescePartitions.enabled=true \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.executor.heartbeatInterval=60s \
      --conf spark.network.timeout=600s \
      --conf spark.executor.heartbeatTimeout=360s \
      --conf spark.storage.blockManagerSlaveTimeout=300s \
      --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
      /opt/spark/jobs/index_elasticsearch.py
    """

    index_es = BashOperator(
        task_id='index_elasticsearch',
        bash_command=spark_submit_cmd,
        execution_timeout=timedelta(minutes=30),  # ⬅️ cohérent avec le timeout du DAG
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    start >> index_es >> end