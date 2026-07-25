#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Job Spark d indexation vers Elasticsearch.
Mode : ECRASEMENT (suppression de l index existant + recreation)
Optimise pour une memoire limitee (8 Go RAM).
Lit les tables Hudi via Hive Metastore et les pousse dans Elasticsearch.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, md5, lit
import subprocess
import sys
import time

# ======================= CONFIGURATION =======================
ES_HOST = "http://elasticsearch:9200"
INDEX_NAME = "university_docs"

TABLES = ["faculty_profiles", "publications", "programs", "documents_catalog"]

# ======================= FONCTIONS =======================

def delete_index(es_url, index):
    """Supprime l index Elasticsearch s il existe (pour overwrite)."""
    print(f"Suppression de l index {index}...")
    try:
        cmd = ["curl", "-X", "DELETE", "-s", "-w", "%{http_code}", f"{es_url}/{index}"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        http_code = result.stdout.strip()
        if http_code in ["200", "404"]:
            print(f"Index {index} supprime (ou inexistant). Code: {http_code}")
            return True
        else:
            print(f"Erreur inattendue: {http_code} - {result.stderr}")
            return False
    except Exception as e:
        print(f"Exception: {e}")
        return False

def wait_for_elasticsearch(es_url, retries=30, delay=2):
    """Attend qu Elasticsearch soit pret avant de demarrer."""
    print("Verification d Elasticsearch...")
    for i in range(retries):
        try:
            cmd = ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", f"{es_url}/_cluster/health"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.stdout.strip() == "200":
                print("Elasticsearch est pret.")
                return True
        except Exception:
            pass
        print(f"   Attente... ({i+1}/{retries})")
        time.sleep(delay)
    print("Elasticsearch inaccessible.")
    return False

def main():
    # --- Spark session optimisee ---
    spark = SparkSession.builder \
        .appName("IndexToElasticsearch") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1536m") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "2") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- Verification Elasticsearch ---
    if not wait_for_elasticsearch(ES_HOST):
        print("Elasticsearch indisponible. Arret.")
        spark.stop()
        sys.exit(1)

    # --- Suppression de l index (overwrite) ---
    delete_index(ES_HOST, INDEX_NAME)

    # --- Indexation des tables ---
    for table in TABLES:
        print(f"\nTraitement de la table : {table}")
        try:
            df = spark.sql(f"SELECT * FROM {table}")
            count = df.count()
            if count == 0:
                print(f"Table {table} vide. Ignoree.")
                continue
            print(f"   Lignes : {count}")

            # Supprimer les colonnes internes Hudi
            cols_to_drop = [c for c in df.columns if c.startswith("_hoodie")]
            if cols_to_drop:
                df = df.drop(*cols_to_drop)

            # --- Gestion du champ _id (pour Elasticsearch) ---
            # On va utiliser une colonne existante comme ID, ou en générer une
            # Mais on ne l'ajoute PAS comme colonne dans le DataFrame final
            if "record_id" in df.columns:
                id_col_name = "record_id"
            elif "doi" in df.columns:
                id_col_name = "doi"
            else:
                # Générer un hash MD5 sur toutes les colonnes, on le garde en colonne temporaire
                df = df.withColumn("_id_temp", md5(concat_ws("_", *df.columns)))
                id_col_name = "_id_temp"

            # --- Ecriture vers Elasticsearch ---
            # On utilise es.mapping.id pour spécifier la colonne à utiliser comme ID
            # On écrit le DataFrame SANS la colonne _id (sinon elle serait indexée)
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", INDEX_NAME) \
                .option("es.write.operation", "index") \
                .option("es.mapping.id", id_col_name) \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.nodes.wan.only", "true") \
                .option("es.batch.size.entries", "500") \
                .option("es.batch.size.bytes", "1mb") \
                .mode("append") \
                .save()

            print(f"Table {table} indexee ({count} documents).")

        except Exception as e:
            print(f"Erreur sur {table}: {e}")
            # On continue avec les autres tables

    spark.stop()
    print("\nIndexation terminee.")

if __name__ == "__main__":
    main()