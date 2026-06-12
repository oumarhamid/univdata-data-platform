#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, row_number, collect_list, struct, to_json, coalesce
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.window import Window
from datetime import datetime

# ------------------------------------------------------------
# 1. Spark session (catalogue en mémoire, pas de Hive)
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("OpenAlexToHudi") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# 2. Lecture des fichiers JSON OpenAlex (multiline = true)
# ------------------------------------------------------------
print("📖 Lecture des fichiers OpenAlex depuis raw-json...")
df_raw = spark.read.option("multiline", "true").json("s3a://raw-json/openalex/*/*/*/*.json")

if "results" in df_raw.columns:
    df_works = df_raw.select(explode("results").alias("work")).select("work.*")
else:
    df_works = df_raw

# Compter les œuvres (optionnel, peut être long)
# print(f"✅ {df_works.count()} œuvres chargées.")

# Pour un test rapide, on peut limiter le nombre d'œuvres
# df_works = df_works.limit(1000)

# ------------------------------------------------------------
# 3. Extraction des auteurs avec leurs affiliations
# ------------------------------------------------------------
# Explode les authorships (certains peuvent être vides -> on les ignore)
df_authors = df_works.select(
    col("id").alias("work_id"),
    col("title").alias("work_title"),
    col("publication_date"),
    col("doi"),
    col("concepts"),
    explode("authorships").alias("authorship")
).select(
    "work_id", "work_title", "publication_date", "doi", "concepts",
    col("authorship.author.display_name").alias("author_name"),
    col("authorship.author.orcid").alias("orcid"),
    col("authorship.institutions").alias("institutions")
)

# Explode les institutions (peut être vide) -> on garde quand même l'auteur
# On utilise explode_outer pour conserver l'auteur même si pas d'institution
from pyspark.sql.functions import explode_outer
df_auth_inst = df_authors.withColumn("institution", explode_outer("institutions")) \
    .select(
        "work_id", "work_title", "publication_date", "doi", "concepts",
        "author_name", "orcid",
        col("institution.display_name").alias("institution_name"),
        col("institution.country_code").alias("country_code")
    )

# ------------------------------------------------------------
# 4. Table research_publications (une ligne par publication)
# ------------------------------------------------------------
df_publications = df_auth_inst.groupBy("work_id", "work_title", "publication_date", "doi") \
    .agg(
        collect_list(
            struct(
                col("author_name").alias("author"),
                col("orcid"),
                col("institution_name").alias("institution"),
                col("country_code")
            )
        ).alias("authors"),
        collect_list("concepts").alias("concepts_list")
    ) \
    .withColumn("record_id", col("work_id")) \
    .withColumn("crawl_timestamp", lit(datetime.now().isoformat()))

# Dédoublonnage
windowPub = Window.partitionBy("work_id").orderBy(col("crawl_timestamp").desc())
df_publications_dedup = df_publications.withColumn("rn", row_number().over(windowPub)) \
    .filter(col("rn") == 1).drop("rn")

# ------------------------------------------------------------
# 5. Table faculty_publications (une ligne par auteur avec ses publications)
# ------------------------------------------------------------
df_author_pubs = df_auth_inst.groupBy("author_name", "orcid") \
    .agg(
        collect_list(
            struct(
                col("work_id").alias("publication_id"),
                col("work_title").alias("title"),
                col("publication_date").alias("date")
            )
        ).alias("publications")
    ) \
    .withColumn("record_id", col("author_name")) \
    .withColumn("crawl_timestamp", lit(datetime.now().isoformat()))

windowAuth = Window.partitionBy("record_id").orderBy(col("crawl_timestamp").desc())
df_author_pubs_dedup = df_author_pubs.withColumn("rn", row_number().over(windowAuth)) \
    .filter(col("rn") == 1).drop("rn")

# ------------------------------------------------------------
# 6. Écriture Hudi (sans Hive)
# ------------------------------------------------------------
def write_hudi(df, table_name, key_field, precombine_field="crawl_timestamp"):
    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": key_field,
        "hoodie.datasource.write.precombine.field": precombine_field,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.clean.automatic": "false",
        "hoodie.datasource.hive_sync.enable": "false",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "path": f"s3a://curated-zone/{table_name}"
    }
    df.write.format("hudi").options(**opts).mode("append").save()
    print(f"✅ Table {table_name} écrite avec {df.count()} lignes.")

print("💾 Écriture de research_publications...")
write_hudi(df_publications_dedup, "research_publications", "work_id")

print("💾 Écriture de faculty_publications...")
write_hudi(df_author_pubs_dedup, "faculty_publications", "record_id")

print("🎉 Transformation OpenAlex terminée.")
spark.stop()