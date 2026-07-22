#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"
PG_DATABASE = "metabase"

CURATED_ZONE = "s3a://curated-zone"

TABLES = {
    "faculty_profiles": f"{CURATED_ZONE}/faculty_profiles",
    "programs": f"{CURATED_ZONE}/programs",
    "publications": f"{CURATED_ZONE}/publications",
    "documents_catalog": f"{CURATED_ZONE}/documents_catalog",
}

spark = (
    SparkSession.builder
    .appName("ExportHudiToPostgres")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

connection_properties = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

HUDI_METADATA_COLUMNS = [
    "_hoodie_commit_time",
    "_hoodie_commit_seqno",
    "_hoodie_record_key",
    "_hoodie_partition_path",
    "_hoodie_file_name",
]


def read_hudi_table(table_name, table_path):
    print(f"Reading Hudi table {table_name} from {table_path}")

    try:
        df = spark.read.format("hudi").load(table_path)
        return df
    except Exception as exc:
        print(f"Table {table_name} not readable from {table_path}: {exc}")
        return None


def remove_hudi_metadata_columns(df):
    clean_columns = [col for col in df.columns if col not in HUDI_METADATA_COLUMNS]
    return df.select(*clean_columns)


def export_table(table_name, table_path):
    print(f"Exporting {table_name} to PostgreSQL...")

    df = read_hudi_table(table_name, table_path)

    if df is None:
        print(f"Skipping {table_name}.")
        return False

    total_rows = df.count()

    if total_rows == 0:
        print(f"Table {table_name} is empty. Skipping.")
        return False

    df = remove_hudi_metadata_columns(df)

    pg_table = f"metabase_{table_name}"

    try:
        (
            df.write
            .jdbc(
                url=jdbc_url,
                table=pg_table,
                mode="overwrite",
                properties=connection_properties,
            )
        )

        print(f"Exported {total_rows} rows from {table_name} to {pg_table}")
        return True

    except Exception as exc:
        print(f"Error while exporting {table_name}: {exc}")
        return False


for table_name, table_path in TABLES.items():
    export_table(table_name, table_path)

spark.stop()
print("Export finished.")
