#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, collect_list, lit, current_timestamp, to_date,
    concat_ws, row_number, coalesce, array, element_at
)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession.builder \
    .appName("APIsToHudi") \
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

# ------------------------------------------------------------
# 1. Lecture OpenAlex
# ------------------------------------------------------------
print("📖 Lecture OpenAlex...")
df_oa_raw = spark.read.option("multiline", "true").json("s3a://raw-json/openalex/*/*/*/*.json")
if "results" in df_oa_raw.columns:
    df_oa_works = df_oa_raw.select(explode("results").alias("work")).select("work.*")
else:
    df_oa_works = df_oa_raw

df_oa_auth = df_oa_works.select(
    col("id").alias("work_id"),
    to_date(col("publication_date")).alias("publication_date"),
    col("title"),
    col("doi"),
    explode("authorships").alias("authorship")
).select(
    "work_id", "publication_date", "title", "doi",
    col("authorship.author.display_name").alias("author_name"),
    col("authorship.author.orcid").alias("author_orcid"),
    concat_ws("; ", col("authorship.institutions.display_name")).alias("institution")
)

df_oa_grouped = df_oa_auth.groupBy("work_id", "publication_date", "title", "doi").agg(
    collect_list("author_name").alias("author_names"),
    collect_list("author_orcid").alias("author_orcids"),
    collect_list("institution").alias("author_institutions")
)

df_oa_final = df_oa_grouped \
    .withColumn("source_system", lit("openalex")) \
    .withColumn("crawl_timestamp", current_timestamp()) \
    .withColumn("type", lit("unknown")) \
    .withColumn("cited_by_count", lit(0)) \
    .withColumn("publisher", lit(None).cast("string"))

# ------------------------------------------------------------
# 2. Lecture Crossref
# ------------------------------------------------------------
print("📖 Lecture Crossref...")
df_cr_raw = spark.read.option("multiline", "true").json("s3a://raw-json/crossref/*/*/*/*.json")
if "message" in df_cr_raw.columns:
    df_cr_works = df_cr_raw.select(explode("message.items").alias("work")).select("work.*")
else:
    df_cr_works = df_cr_raw

df_cr_auth = df_cr_works.select(
    col("DOI").alias("doi"),
    to_date(col("issued.date-parts")[0][0]).alias("publication_date"),
    element_at(col("title"), 1).alias("title"),
    col("type"),
    col("is-referenced-by-count").alias("cited_by_count"),
    col("publisher"),
    explode("author").alias("author")
).select(
    "doi", "publication_date", "title", "type", "cited_by_count", "publisher",
    concat_ws(" ", col("author.given"), col("author.family")).alias("author_name"),
    col("author.ORCID").alias("author_orcid"),
    concat_ws("; ", col("author.affiliation.name")).alias("institution")
)

df_cr_grouped = df_cr_auth.groupBy("doi", "publication_date", "title", "type", "cited_by_count", "publisher").agg(
    collect_list("author_name").alias("author_names"),
    collect_list("author_orcid").alias("author_orcids"),
    collect_list("institution").alias("author_institutions")
)

df_cr_final = df_cr_grouped \
    .withColumn("work_id", concat_ws("", lit("https://doi.org/"), col("doi"))) \
    .withColumn("source_system", lit("crossref")) \
    .withColumn("crawl_timestamp", current_timestamp())

df_cr_final = df_cr_final.select(
    "work_id", "title", "publication_date", "doi", "type", "cited_by_count",
    "author_names", "author_orcids", "author_institutions",
    "source_system", "crawl_timestamp", "publisher"
)

# ------------------------------------------------------------
# 3. Union et dédoublonnage
# ------------------------------------------------------------
df_union = df_oa_final.unionByName(df_cr_final, allowMissingColumns=True)

df_union = df_union.fillna({
    "cited_by_count": 0,
    "type": "unknown",
    "publisher": "unknown"
})

empty_array = array().cast(ArrayType(StringType()))
df_union = df_union.withColumn("author_names", coalesce(col("author_names"), empty_array)) \
                   .withColumn("author_orcids", coalesce(col("author_orcids"), empty_array)) \
                   .withColumn("author_institutions", coalesce(col("author_institutions"), empty_array))

window = Window.partitionBy("work_id").orderBy(col("crawl_timestamp").desc())
df_dedup = df_union.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

# ------------------------------------------------------------
# 4. Écriture Hudi avec synchronisation Hive via HMS
# ------------------------------------------------------------
hudi_options = {
    "hoodie.table.name": "publications",
    "hoodie.datasource.write.recordkey.field": "work_id",
    "hoodie.datasource.write.precombine.field": "crawl_timestamp",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.clean.automatic": "false",

    # ----- Synchronisation Hive via HMS -----
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "publications",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.partition_fields": "",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    # -------------------------------------------------------

    "path": "s3a://curated-zone/publications"
}

print("💾 Écriture dans curated-zone/publications avec sync Hive (HMS)...")
df_dedup.write.format("hudi").options(**hudi_options).mode("append").save()

print("🎉 ETL terminé. Table 'publications' synchronisée dans Hive via HMS.")
spark.stop()