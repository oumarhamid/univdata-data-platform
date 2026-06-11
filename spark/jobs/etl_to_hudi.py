#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, lit, coalesce, concat_ws, when, regexp_extract, input_file_name, regexp_replace, md5, concat
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, BooleanType
import hashlib
import re
from bs4 import BeautifulSoup
from datetime import datetime

# ------------------------------------------------------------
# 1. Initialisation Spark avec support Hudi et Elasticsearch
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("UniversityETL") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# 2. Fonctions d'extraction (UDF)
# ------------------------------------------------------------

def extract_faculty_from_html(html_content, source_url, domain):
    """Extrait une liste de dict pour les professeurs trouvés dans une page HTML."""
    if not html_content:
        return []
    soup = BeautifulSoup(html_content, 'html.parser')
    faculty_list = []
    # Critères simples : présence de "Professeur", "Pr.", "Dr." dans des balises de texte
    for elem in soup.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'div', 'span', 'td', 'li']):
        text = elem.get_text(strip=True)
        if re.search(r'(Professeur|Pr\.|Dr\.|Prof\.)', text):
            name = re.sub(r'\s+', ' ', text).strip()
            name = name[:200]
            if name and len(name) > 3:
                record_id = hashlib.md5((name + domain).encode()).hexdigest()
                faculty_list.append({
                    "record_id": record_id,
                    "full_name": name,
                    "title": "Professeur" if "Professeur" in name else "Enseignant",
                    "department": "",
                    "faculty": "",
                    "university": domain,
                    "email": "",
                    "research_areas": [],
                    "source_system": "web_scraping",
                    "source_url": source_url,
                    "crawl_timestamp": datetime.now().isoformat(),
                    "content_hash": hashlib.md5(html_content.encode()).hexdigest(),
                    "is_deleted": False
                })
    return faculty_list

def extract_course_from_html(html_content, source_url, domain):
    """Extrait des cours à partir de pages de formations (liens vers PDF)."""
    if not html_content:
        return []
    soup = BeautifulSoup(html_content, 'html.parser')
    courses = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        text = link.get_text(strip=True)
        if href.endswith('.pdf') and ('cours' in text.lower() or 'syllabus' in text.lower() or 'programme' in text.lower()):
            course_id = hashlib.md5(href.encode()).hexdigest()
            courses.append({
                "course_id": course_id,
                "course_name": text[:100],
                "description": "",
                "credits": 0,
                "department": "",
                "faculty": "",
                "semester": "",
                "instructor_name": "",
                "source_url": source_url,
                "crawl_timestamp": datetime.now().isoformat()
            })
    return courses

# Schémas UDF
faculty_schema = ArrayType(StructType([
    StructField("record_id", StringType()),
    StructField("full_name", StringType()),
    StructField("title", StringType()),
    StructField("department", StringType()),
    StructField("faculty", StringType()),
    StructField("university", StringType()),
    StructField("email", StringType()),
    StructField("research_areas", ArrayType(StringType())),
    StructField("source_system", StringType()),
    StructField("source_url", StringType()),
    StructField("crawl_timestamp", StringType()),
    StructField("content_hash", StringType()),
    StructField("is_deleted", BooleanType())
]))

course_schema = ArrayType(StructType([
    StructField("course_id", StringType()),
    StructField("course_name", StringType()),
    StructField("description", StringType()),
    StructField("credits", IntegerType()),
    StructField("department", StringType()),
    StructField("faculty", StringType()),
    StructField("semester", StringType()),
    StructField("instructor_name", StringType()),
    StructField("source_url", StringType()),
    StructField("crawl_timestamp", StringType())
]))

extract_faculty_udf = udf(extract_faculty_from_html, faculty_schema)
extract_course_udf = udf(extract_course_from_html, course_schema)

# ------------------------------------------------------------
# 3. Lecture des données brutes depuis MinIO (raw-web-html)
# ------------------------------------------------------------
df_raw = spark.read.text("s3a://raw-web-html/*/*/*.html") \
    .withColumn("file_path", input_file_name()) \
    .withColumn("domain", regexp_extract(col("file_path"), r"s3a://raw-web-html/([^/]+)/", 1))

df_html = df_raw.groupBy("file_path", "domain").agg(concat_ws("\n", collect_list("value")).alias("html_content"))

# Extraction professeurs
df_faculty_raw = df_html.select(
    col("domain"),
    col("file_path").alias("source_url"),
    explode(extract_faculty_udf(col("html_content"), col("file_path"), col("domain"))).alias("faculty")
).select("faculty.*")

# Extraction cours
df_course_raw = df_html.select(
    col("domain"),
    col("file_path").alias("source_url"),
    explode(extract_course_udf(col("html_content"), col("file_path"), col("domain"))).alias("course")
).select("course.*")

# ------------------------------------------------------------
# 4. Enrichissement avec OpenAlex (fichiers JSON chunkés)
# ------------------------------------------------------------
df_openalex = spark.read.json("s3a://raw-json/openalex/*.json")
df_works = df_openalex.select(explode("results").alias("work"))
df_authors = df_works.select(
    explode("work.authorships").alias("authorship")
).select(
    col("authorship.author.display_name").alias("author_name"),
    col("authorship.institutions").alias("institutions")
).filter(col("institutions").isNotNull())

df_publications = df_authors.groupBy("author_name").agg(
    collect_list(struct(lit("unknown").alias("title"), lit("unknown").alias("year"))).alias("publications")
)

# Jointure approximative sur le nom (normalisé)
from pyspark.sql.functions import lower, regexp_replace
df_faculty_norm = df_faculty_raw.withColumn(
    "name_norm", lower(regexp_replace(col("full_name"), "[^a-z]", ""))
)
df_pub_norm = df_publications.withColumn(
    "name_norm", lower(regexp_replace(col("author_name"), "[^a-z]", ""))
)
df_faculty_enriched = df_faculty_norm.join(df_pub_norm, on="name_norm", how="left").select(
    df_faculty_raw["*"],
    col("publications")
).drop("name_norm")

# ------------------------------------------------------------
# 5. Dédoublonnage
# ------------------------------------------------------------
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("record_id").orderBy(col("crawl_timestamp").desc())
df_faculty_dedup = df_faculty_enriched.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") == 1).drop("row_num")

df_course_dedup = df_course_raw.dropDuplicates(["course_id"])

# ------------------------------------------------------------
# 6. Écriture dans Hudi
# ------------------------------------------------------------
hudi_options = {
    "hoodie.table.name": "faculty_profiles",
    "hoodie.datasource.write.recordkey.field": "record_id",
    "hoodie.datasource.write.precombine.field": "crawl_timestamp",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "faculty_profiles",
    "hoodie.datasource.hive_sync.mode": "hms",
    "path": "s3a://curated-zone/faculty_profiles"
}
df_faculty_dedup.write.format("hudi").options(**hudi_options).mode("append").save()

hudi_options_course = {
    "hoodie.table.name": "course_catalog",
    "hoodie.datasource.write.recordkey.field": "course_id",
    "hoodie.datasource.write.precombine.field": "crawl_timestamp",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "course_catalog",
    "hoodie.datasource.hive_sync.mode": "hms",
    "path": "s3a://curated-zone/course_catalog"
}
df_course_dedup.write.format("hudi").options(**hudi_options_course).mode("append").save()

spark.stop()