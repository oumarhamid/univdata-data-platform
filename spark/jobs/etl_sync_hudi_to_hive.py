#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

DATABASE = "default"
TABLES = ["publications"]
CURATED_BASE = "s3a://curated-zone"

spark = SparkSession.builder \
    .appName("SyncHudiToHive") \
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

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

for table in TABLES:
    table_path = f"{CURATED_BASE}/{table}"
    print(f"\n========== {table} ==========")

    try:
        df = spark.read.format("hudi").load(table_path)
        if df.limit(1).count() == 0:
            print(f"⚠️ Table vide : {table}")
            continue

        print(f"✅ Données trouvées dans {table_path}")

        # Options Hudi pour la synchronisation via JDBC
        hudi_options = {
            "hoodie.table.name": table,
            "hoodie.datasource.write.recordkey.field": "_hoodie_record_key",
            "hoodie.datasource.write.precombine.field": "_hoodie_commit_time",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",

            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.mode": "jdbc",
            "hoodie.datasource.hive_sync.database": DATABASE,
            "hoodie.datasource.hive_sync.table": table,
            "hoodie.datasource.hive_sync.jdbc_url": "jdbc:hive2://hiveserver2:10000/default",
            "hoodie.datasource.hive_sync.use_jdbc": "true",
            "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
            "hoodie.datasource.hive_sync.support_timestamp": "true",
        }

        df.write.format("hudi").options(**hudi_options).mode("append").save(table_path)
        print(f"✅ Hive Sync OK : {DATABASE}.{table}")

    except Exception as e:
        print(f"❌ Erreur sur {table}")
        print(str(e))
        raise

print("\n===== TABLES HIVE =====")
spark.sql(f"SHOW TABLES IN {DATABASE}").show(truncate=False)
spark.stop()