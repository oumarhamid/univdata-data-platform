from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="pdf_to_hudi_pipeline",
    default_args=default_args,
    description="Parse PDF documents from raw-documents and write documents_catalog to Hudi",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "hudi", "pdf", "documents"],
) as dag:

    start = EmptyOperator(task_id="start")

    spark_submit_cmd = """
    sudo docker exec -u root univdata-spark-master bash -lc 'PDF_MAX_FILES=5 PDF_MAX_PAGES_PER_PDF=3 PDF_MAX_FILE_SIZE_MB=8 PDF_MAX_TEXT_CHARS=12000 /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 1g \
      --executor-memory 1536m \
      --conf spark.executor.cores=1 \
      --conf spark.cores.max=2 \
      --conf spark.sql.shuffle.partitions=4 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
      /opt/spark/jobs/etl_pdf_to_hudi.py'
    """

    etl_pdf_to_hudi = BashOperator(
        task_id="etl_pdf_to_hudi_via_docker",
        bash_command=spark_submit_cmd,
        execution_timeout=timedelta(minutes=120),
    )

    end = EmptyOperator(task_id="end")

    start >> etl_pdf_to_hudi >> end
