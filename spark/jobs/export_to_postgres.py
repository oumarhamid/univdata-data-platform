#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

# Configuration PostgreSQL
PG_HOST = "postgres"
PG_PORT = 5432
PG_USER = "airflow"
PG_PASSWORD = "airflow"
PG_DATABASE = "metabase"

# Liste des tables Hudi à exporter
TABLES = ["faculty_profiles", "programs", "publications"]

# Spark session (avec support Hive)
spark = SparkSession.builder \
    .appName("ExportToPostgres") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# URL JDBC pour PostgreSQL
jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
connection_properties = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def table_exists(table):
    """Vérifie si la table existe dans le metastore Hive."""
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 1")
        return True
    except Exception:
        return False

def export_table(table):
    """Exporte une table Hudi vers PostgreSQL."""
    print(f"📤 Export de {table}...")
    if not table_exists(table):
        print(f"⚠️  Table {table} n'existe pas, ignorée.")
        return False

    df = spark.sql(f"SELECT * FROM {table}")
    if df.count() == 0:
        print(f"⚠️  Table {table} vide, ignorée.")
        return False

    pg_table = f"metabase_{table}"
    try:
        df.write.jdbc(
            url=jdbc_url,
            table=pg_table,
            mode="overwrite",
            properties=connection_properties
        )
        print(f"✅ {table} exporté dans {pg_table}")
        return True
    except Exception as e:
        print(f"❌ Erreur lors de l'export de {table}: {e}")
        return False

# Exporter chaque table
for table in TABLES:
    export_table(table)

spark.stop()
print("🎉 Export terminé.")