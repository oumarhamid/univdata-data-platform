#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL ameliore : OpenAlex + Crossref -> Hudi
Ne lit que la partition la plus récente pour chaque source (year/month/day).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, collect_list, lit, current_timestamp, to_date,
    concat_ws, row_number, coalesce, array, element_at, size,
    when, trim, regexp_replace, udf, monotonically_increasing_id,
    filter as spark_filter, desc_nulls_last, asc_nulls_last
)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
import unicodedata
import re

# ============================================================
# CONFIGURATION
# ============================================================
spark = (
    SparkSession.builder
    .appName("APIsToHudi_v2")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# FONCTION POUR TROUVER LA PARTITION LA PLUS RÉCENTE
# ============================================================
def get_latest_partition_path(base_prefix):
    """
    Parcourt les sous-dossiers de base_prefix (ex: s3a://raw-json/openalex/)
    pour trouver la partition year=YYYY/month=MM/day=DD la plus récente.
    Retourne le chemin complet vers les fichiers JSON de cette partition.
    Si aucun dossier n'est trouvé, retourne None.
    """
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    Path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    URI = spark.sparkContext._jvm.java.net.URI
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        URI.create(base_prefix),
        hadoop_conf
    )
    path = Path(base_prefix)
    if not fs.exists(path):
        print(f"[WARN] Le chemin {base_prefix} n'existe pas")
        return None

    year_pattern = re.compile(r"year=(\d{4})")
    years = []
    for status in fs.listStatus(path):
        if status.isDirectory():
            name = status.getPath().getName()
            m = year_pattern.match(name)
            if m:
                years.append((int(m.group(1)), status.getPath()))
    if not years:
        print(f"[WARN] Aucun dossier year=YYYY trouvé sous {base_prefix}")
        return None
    max_year = max(years, key=lambda x: x[0])
    year_path = max_year[1]

    month_pattern = re.compile(r"month=(\d{2})")
    months = []
    for status in fs.listStatus(year_path):
        if status.isDirectory():
            name = status.getPath().getName()
            m = month_pattern.match(name)
            if m:
                months.append((int(m.group(1)), status.getPath()))
    if not months:
        print(f"[WARN] Aucun dossier month=MM trouvé sous {year_path}")
        return None
    max_month = max(months, key=lambda x: x[0])
    month_path = max_month[1]

    day_pattern = re.compile(r"day=(\d{2})")
    days = []
    for status in fs.listStatus(month_path):
        if status.isDirectory():
            name = status.getPath().getName()
            m = day_pattern.match(name)
            if m:
                days.append((int(m.group(1)), status.getPath()))
    if not days:
        print(f"[WARN] Aucun dossier day=DD trouvé sous {month_path}")
        return None
    max_day = max(days, key=lambda x: x[0])
    day_path = max_day[1]

    latest_path = str(day_path) + "/*.json"
    print(f"[INFO] Partition la plus récente trouvée : {latest_path}")
    return latest_path

# ============================================================
# UDFs DE NETTOYAGE
# ============================================================

@udf(StringType())
def fix_encoding(text):
    if text is None:
        return None
    text = str(text)
    try:
        fixed = text.encode("latin1").decode("utf-8")
        if sum(1 for c in fixed if 0x80 <= ord(c) <= 0x10FFFF) > sum(1 for c in text if 0x80 <= ord(c) <= 0x10FFFF):
            text = fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    text = unicodedata.normalize("NFC", text)
    text = "".join(c for c in text if unicodedata.category(c)[0] != "C" or c in "\n\t\r")
    return text.strip()


@udf(ArrayType(StringType()))
def dedup_list_keep_order(items):
    if not items:
        return []
    seen = set()
    result = []
    for item in items:
        s = str(item).strip() if item is not None else ""
        if s and s not in seen:
            seen.add(s)
            result.append(s)
    return result


@udf(StringType())
def normalize_doi(doi):
    if not doi:
        return None
    doi = str(doi).strip().lower()
    doi = re.sub(r"\s+", "", doi)
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
    return doi if doi else None


@udf(StringType())
def normalize_work_id(work_id, doi):
    if doi and str(doi).strip():
        return "https://doi.org/" + str(doi).strip().lower()
    if work_id and str(work_id).strip():
        return str(work_id).strip()
    return None


@udf(StringType())
def map_publication_type(raw_type):
    if not raw_type:
        return "unknown"
    raw = str(raw_type).lower().strip()
    mapping = {
        "journal-article": "journal-article",
        "book-chapter": "book-chapter",
        "book": "book",
        "proceedings-article": "conference-paper",
        "conference-paper": "conference-paper",
        "dataset": "dataset",
        "report": "report",
        "preprint": "preprint",
        "posted-content": "preprint",
        "dissertation": "dissertation",
        "article": "journal-article",
        "review": "review",
        "paratext": "paratext",
        "letter": "letter",
        "editorial": "editorial",
        "erratum": "erratum",
        "retraction": "retraction",
        "other": "other",
    }
    return mapping.get(raw, raw)


# ============================================================
# 1. LECTURE OPENALEX (seulement la dernière partition)
# ============================================================
print("Recherche de la partition OpenAlex la plus récente...")
oa_latest = get_latest_partition_path("s3a://raw-json/openalex/")
if oa_latest is None:
    print("[ERREUR] Aucune donnée OpenAlex trouvée. Arrêt.")
    spark.stop()
    raise SystemExit("Aucune donnée OpenAlex disponible")
else:
    print(f"Lecture OpenAlex depuis : {oa_latest}")
    df_oa_raw = spark.read.option("multiline", "true").json(oa_latest)

if "results" in df_oa_raw.columns:
    df_oa_works = df_oa_raw.select(explode("results").alias("work")).select("work.*")
else:
    df_oa_works = df_oa_raw

df_oa_auth = (
    df_oa_works
    .select(
        col("id").alias("work_id"),
        col("doi").alias("raw_doi"),
        to_date(col("publication_date")).alias("publication_date"),
        fix_encoding(col("title")).alias("title"),
        explode("authorships").alias("authorship")
    )
    .select(
        "work_id", "raw_doi", "publication_date", "title",
        col("authorship.author.display_name").alias("author_name"),
        col("authorship.author.orcid").alias("author_orcid"),
        col("authorship.author.id").alias("author_oa_id"),
        concat_ws("; ", col("authorship.institutions.display_name")).alias("institution"),
        col("authorship.is_corresponding").alias("is_corresponding")
    )
)

df_oa_auth = (
    df_oa_auth
    .withColumn("author_name_clean", fix_encoding(trim(col("author_name"))))
    .withColumn(
        "author_orcid_clean",
        when(col("author_orcid").isNull() | (trim(col("author_orcid")) == ""), None)
        .otherwise(trim(col("author_orcid")))
    )
    .withColumn(
        "institution_clean",
        when(col("institution").isNull() | (trim(col("institution")) == ""), None)
        .otherwise(fix_encoding(trim(col("institution"))))
    )
)

win_oa = Window.partitionBy("work_id", "author_name_clean").orderBy(
    desc_nulls_last("author_orcid_clean"),
    desc_nulls_last("is_corresponding")
)
df_oa_auth_dedup = (
    df_oa_auth
    .withColumn("rn_auth", row_number().over(win_oa))
    .filter(col("rn_auth") == 1)
    .drop("rn_auth")
)

df_oa_grouped = (
    df_oa_auth_dedup
    .groupBy("work_id", "raw_doi", "publication_date", "title")
    .agg(
        collect_list("author_name_clean").alias("author_names"),
        collect_list("author_orcid_clean").alias("author_orcids"),
        collect_list("institution_clean").alias("author_institutions"),
        collect_list("author_oa_id").alias("author_oa_ids")
    )
)

df_oa_final = (
    df_oa_grouped
    .withColumn("doi", normalize_doi(col("raw_doi")))
    .withColumn("work_id_norm", normalize_work_id(col("work_id"), col("doi")))
    .withColumn("source_system", lit("openalex"))
    .withColumn("crawl_timestamp", current_timestamp())
    .withColumn("type", lit("unknown"))
    .withColumn("publisher", lit("unknown"))
    .withColumn("cited_by_count", lit(0))
)

# ============================================================
# 2. LECTURE CROSSREF (seulement la dernière partition)
# ============================================================
print("Recherche de la partition Crossref la plus récente...")
cr_latest = get_latest_partition_path("s3a://raw-json/crossref/")
if cr_latest is None:
    print("[ERREUR] Aucune donnée Crossref trouvée. Arrêt.")
    spark.stop()
    raise SystemExit("Aucune donnée Crossref disponible")
else:
    print(f"Lecture Crossref depuis : {cr_latest}")
    df_cr_raw = spark.read.option("multiline", "true").json(cr_latest)

if "message" in df_cr_raw.columns:
    df_cr_works = df_cr_raw.select(explode("message.items").alias("work")).select("work.*")
else:
    df_cr_works = df_cr_raw

df_cr_auth = (
    df_cr_works
    .select(
        col("DOI").alias("doi"),
        to_date(col("issued.date-parts")[0][0]).alias("publication_date"),
        fix_encoding(element_at(col("title"), 1)).alias("title"),
        col("type").alias("raw_type"),
        col("is-referenced-by-count").alias("cited_by_count"),
        fix_encoding(col("publisher")).alias("publisher_cr"),
        col("container-title").alias("journal_name"),
        explode("author").alias("author")
    )
    .select(
        "doi", "publication_date", "title", "raw_type", "cited_by_count",
        "publisher_cr", "journal_name",
        concat_ws(" ", col("author.given"), col("author.family")).alias("author_name"),
        col("author.ORCID").alias("author_orcid"),
        concat_ws("; ", col("author.affiliation.name")).alias("institution"),
        col("author.sequence").alias("author_sequence")
    )
)

df_cr_auth = (
    df_cr_auth
    .withColumn("author_name_clean", fix_encoding(trim(col("author_name"))))
    .withColumn(
        "author_orcid_clean",
        when(col("author_orcid").isNull() | (trim(col("author_orcid")) == ""), None)
        .otherwise(trim(col("author_orcid")))
    )
    .withColumn(
        "institution_clean",
        when(col("institution").isNull() | (trim(col("institution")) == ""), None)
        .otherwise(fix_encoding(trim(col("institution"))))
    )
)

win_cr = Window.partitionBy("doi", "author_name_clean").orderBy(
    desc_nulls_last("author_orcid_clean"),
    asc_nulls_last("author_sequence")
)
df_cr_auth_dedup = (
    df_cr_auth
    .withColumn("rn_auth", row_number().over(win_cr))
    .filter(col("rn_auth") == 1)
    .drop("rn_auth")
)

df_cr_grouped = (
    df_cr_auth_dedup
    .groupBy("doi", "publication_date", "title", "raw_type", "cited_by_count", "publisher_cr", "journal_name")
    .agg(
        collect_list("author_name_clean").alias("author_names"),
        collect_list("author_orcid_clean").alias("author_orcids"),
        collect_list("institution_clean").alias("author_institutions")
    )
)

df_cr_final = (
    df_cr_grouped
    .withColumn("doi_norm", normalize_doi(col("doi")))
    .withColumn("work_id", concat_ws("", lit("https://doi.org/"), col("doi_norm")))
    .withColumn("source_system", lit("crossref"))
    .withColumn("crawl_timestamp", current_timestamp())
    .withColumn("type", map_publication_type(col("raw_type")))
    .withColumn(
        "publisher",
        when(col("publisher_cr").isNotNull() & (trim(col("publisher_cr")) != ""), col("publisher_cr"))
        .when(col("journal_name").isNotNull() & (size(col("journal_name")) > 0), element_at(col("journal_name"), 1))
        .otherwise(lit("unknown"))
    )
    .withColumn("cited_by_count", coalesce(col("cited_by_count").cast("int"), lit(0)))
)

df_cr_final = df_cr_final.select(
    col("work_id"),
    col("doi_norm").alias("doi"),
    "publication_date", "title", "type", "cited_by_count",
    "author_names", "author_orcids", "author_institutions",
    "source_system", "crawl_timestamp", "publisher",
    lit(None).cast(ArrayType(StringType())).alias("author_oa_ids")
)

df_oa_final = df_oa_final.select(
    col("work_id_norm").alias("work_id"),
    col("doi"),
    "publication_date", "title", "type", "cited_by_count",
    "author_names", "author_orcids", "author_institutions",
    "source_system", "crawl_timestamp", "publisher",
    "author_oa_ids"
)

# ============================================================
# 3. UNION ET DEDOUBLONNAGE CROSS-SOURCE
# ============================================================
print("Union et dedoublonnage cross-source...")

df_union = df_oa_final.unionByName(df_cr_final, allowMissingColumns=True)

df_union = (
    df_union
    .withColumn("author_names", spark_filter(col("author_names"), lambda x: x.isNotNull() & (trim(x) != "")))
    .withColumn("author_orcids", spark_filter(col("author_orcids"), lambda x: x.isNotNull() & (trim(x) != "")))
    .withColumn("author_institutions", spark_filter(col("author_institutions"), lambda x: x.isNotNull() & (trim(x) != "")))
)

window_source = Window.partitionBy("work_id").orderBy(
    when(col("source_system") == "openalex", 1).otherwise(2).asc(),
    col("crawl_timestamp").desc()
)

df_dedup = (
    df_union
    .withColumn("rn", row_number().over(window_source))
    .filter(col("rn") == 1)
    .drop("rn")
)

df_dedup = (
    df_dedup
    .withColumn("author_names", dedup_list_keep_order(col("author_names")))
    .withColumn("author_orcids", dedup_list_keep_order(col("author_orcids")))
    .withColumn("author_institutions", dedup_list_keep_order(col("author_institutions")))
)

df_dedup = df_dedup.withColumn("title", regexp_replace(col("title"), r"<[^>]+>", ""))

df_dedup = df_dedup.fillna({
    "cited_by_count": 0,
    "type": "unknown",
    "publisher": "unknown"
})

empty_array = array().cast(ArrayType(StringType()))
df_dedup = (
    df_dedup
    .withColumn("author_names", coalesce(col("author_names"), empty_array))
    .withColumn("author_orcids", coalesce(col("author_orcids"), empty_array))
    .withColumn("author_institutions", coalesce(col("author_institutions"), empty_array))
    .withColumn("author_oa_ids", coalesce(col("author_oa_ids"), empty_array))
)

# ============================================================
# 4. ECRITURE HUDI
# ============================================================
hudi_options = {
    "hoodie.table.name": "publications",
    "hoodie.datasource.write.recordkey.field": "work_id",
    "hoodie.datasource.write.precombine.field": "crawl_timestamp",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.clean.automatic": "false",

    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "publications",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.partition_fields": "",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.support_timestamp": "true",

    "path": "s3a://curated-zone/publications"
}

print("Ecriture dans curated-zone/publications avec sync Hive (HMS)...")
df_dedup.write.format("hudi").options(**hudi_options).mode("append").save()

print("\nETL v2 termine. Table 'publications' synchronisee dans Hive via HMS.")
spark.stop()