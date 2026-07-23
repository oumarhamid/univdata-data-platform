from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pdf_to_hudi_pipeline_optimized",
    default_args=default_args,
    description="Parse PDF documents from raw-documents and write documents_catalog to Hudi (optimized)",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "hudi", "pdf", "optimized"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Commandes optimisées : mémoire réduite, partitions adaptées, plus de retry
    spark_submit_cmd = """
    sudo docker exec -u root univdata-spark-master bash -lc 'export PDF_MAX_FILES=5 PDF_MAX_PAGES_PER_PDF=2 PDF_MAX_FILE_SIZE_MB=6 PDF_MAX_TEXT_CHARS=8000 && \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 512m \
      --executor-memory 1024m \
      --conf spark.executor.cores=1 \
      --conf spark.cores.max=1 \
      --conf spark.sql.shuffle.partitions=2 \
      --conf spark.default.parallelism=2 \
      --conf spark.memory.fraction=0.4 \
      --conf spark.memory.storageFraction=0.3 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
      /opt/spark/jobs/etl_pdf_to_hudi.py'
    """

    etl_pdf_to_hudi = BashOperator(
        task_id="etl_pdf_to_hudi_via_docker",
        bash_command=spark_submit_cmd,
        execution_timeout=timedelta(minutes=30),
        retries=1,
    )

    end = EmptyOperator(task_id="end")

    start >> etl_pdf_to_hudi >> end