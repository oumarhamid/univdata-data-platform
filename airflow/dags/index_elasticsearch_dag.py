from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=60),
}

with DAG('index_elasticsearch_pipeline',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         max_active_runs=1,
         tags=['elasticsearch', 'index', 'ultra-optimized']) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # ============================================================
    # ULTRA-OPTIMISÉ: BashOperator + docker exec (standalone)
    # ============================================================
    spark_submit_cmd = """
    set -e

    echo "🚀 Lancement du job Spark ultra-optimisé..."

    sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --driver-memory 2g \
      --executor-memory 2g \
      --executor-cores 2 \
      --total-executor-cores 4 \
      --conf spark.executor.instances=2 \
      --conf spark.sql.shuffle.partitions=16 \
      --conf spark.default.parallelism=16 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.kryo.registrationRequired=false \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.sql.adaptive.coalescePartitions.enabled=true \
      --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m \
      --conf spark.sql.adaptive.coalescePartitions.minPartitionSize=1m \
      --conf spark.sql.autoBroadcastJoinThreshold=100m \
      --conf spark.memory.fraction=0.7 \
      --conf spark.memory.storageFraction=0.4 \
      --conf spark.memory.offHeap.enabled=true \
      --conf spark.memory.offHeap.size=512m \
      --conf spark.sql.parquet.compression.codec=zstd \
      --conf spark.sql.files.maxPartitionBytes=134217728 \
      --conf spark.sql.files.openCostInBytes=4194304 \
      --conf spark.executor.heartbeatInterval=30s \
      --conf spark.network.timeout=300s \
      --conf spark.executor.heartbeatTimeout=120s \
      --conf spark.locality.wait=3s \
      --conf spark.scheduler.mode=FAIR \
      --conf spark.speculation=true \
      --conf spark.speculation.multiplier=1.5 \
      --conf spark.speculation.quantile=0.75 \
      --conf spark.sql.hive.convertMetastoreParquet=false \
      --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
      /opt/spark/jobs/index_elasticsearch.py

    echo "✅ Job terminé avec succès"
    """

    index_es = BashOperator(
        task_id='index_elasticsearch',
        bash_command=spark_submit_cmd,
        execution_timeout=timedelta(minutes=60),
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    start >> index_es >> end
