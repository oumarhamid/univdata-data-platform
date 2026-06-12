#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, input_file_name, regexp_extract, row_number, lit
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, BooleanType
from pyspark.sql.window import Window
import hashlib
import re
from datetime import datetime

# ------------------------------------------------------------
# 1. Spark session (sans Hive support)
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("UniversityETL") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# 2. Fonctions d'extraction
# ------------------------------------------------------------
def extract_faculty(html):
    """Extrait les noms de professeurs via regex."""
    if not html:
        return []
    patterns = [
        r'(?:Professeur|Pr\.|Dr\.|Prof\.)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
        r'(?:M\.|Mme|Mlle|Enseignant)\s*:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
        r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*(?:est\s+)?(?:Professeur|enseignant)'
    ]
    names = set()
    for pattern in patterns:
        for match in re.findall(pattern, html, re.IGNORECASE):
            name = ' '.join(match.split())
            if len(name) > 3:
                names.add(name)
    return list(names)

def extract_courses(html, base_url):
    """Extrait les cours (liens PDF avec mots-clés)."""
    if not html:
        return []
    courses = []
    pdf_links = re.findall(r'<a\s+[^>]*href="([^"]+\.pdf)"[^>]*>([^<]*)</a>', html, re.IGNORECASE)
    for pdf_url, text in pdf_links:
        text = text.strip()
        if text and any(kw in text.lower() for kw in ['cours', 'syllabus', 'programme', 'td', 'tp', 'fiche']):
            if not pdf_url.startswith('http'):
                pdf_url = base_url.rstrip('/') + '/' + pdf_url.lstrip('/')
            courses.append((text[:100], pdf_url))
    return courses

def extract_programs(html):
    """Extrait les noms de formations."""
    if not html:
        return []
    programs = []
    patterns = [
        r'Licence\s+(?:professionnelle\s+)?[dD]e\s+([^<,.]+)',
        r'Master\s+(?:spécialisé\s+)?[dD]e\s+([^<,.]+)',
        r'Filière\s*:\s*([^<,.]+)',
        r'Département\s*:\s*([^<,.]+)',
        r'(?:Licence|Master)\s+en\s+([^<,.]+)'
    ]
    for pattern in patterns:
        for match in re.findall(pattern, html, re.IGNORECASE):
            prog = match.strip()
            if len(prog) > 5:
                programs.append(prog)
    return list(set(programs))

# ------------------------------------------------------------
# 3. UDF
# ------------------------------------------------------------
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

def faculty_udf(html, source_url, domain):
    names = extract_faculty(html)
    if not names:
        names = ["Exemple Professeur"]
    result = []
    for name in names:
        record_id = hashlib.md5((name + domain + source_url).encode()).hexdigest()
        result.append({
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
            "content_hash": hashlib.md5(html.encode()).hexdigest(),
            "is_deleted": False
        })
    return result

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

def course_udf(html, source_url, domain):
    courses = extract_courses(html, source_url)
    if not courses:
        courses = [("Exemple Cours", source_url)]
    result = []
    for name, pdf_url in courses:
        course_id = hashlib.md5((pdf_url + name).encode()).hexdigest()
        result.append({
            "course_id": course_id,
            "course_name": name,
            "description": "",
            "credits": 0,
            "department": "",
            "faculty": "",
            "semester": "",
            "instructor_name": "",
            "source_url": source_url,
            "crawl_timestamp": datetime.now().isoformat()
        })
    return result

program_schema = ArrayType(StructType([
    StructField("program_id", StringType()),
    StructField("program_name", StringType()),
    StructField("level", StringType()),
    StructField("department", StringType()),
    StructField("faculty", StringType()),
    StructField("source_url", StringType()),
    StructField("crawl_timestamp", StringType())
]))

def program_udf(html, source_url, domain):
    progs = extract_programs(html)
    if not progs:
        progs = ["Exemple Formation"]
    result = []
    for prog in progs:
        level = "Licence" if "Licence" in prog else "Master" if "Master" in prog else "Formation"
        program_id = hashlib.md5((prog + domain).encode()).hexdigest()
        result.append({
            "program_id": program_id,
            "program_name": prog,
            "level": level,
            "department": "",
            "faculty": "",
            "source_url": source_url,
            "crawl_timestamp": datetime.now().isoformat()
        })
    return result

from pyspark.sql.functions import udf
faculty_udf_spark = udf(faculty_udf, faculty_schema)
course_udf_spark = udf(course_udf, course_schema)
program_udf_spark = udf(program_udf, program_schema)

# ------------------------------------------------------------
# 4. Lecture HTML (limité à 100 fichiers pour test)
# ------------------------------------------------------------
print("Lecture des fichiers HTML depuis MinIO...")
df_html = spark.read.option("wholetext", "true").text("s3a://raw-web-html/*/*/*.html") \
    .withColumn("file_path", input_file_name()) \
    .withColumn("domain", regexp_extract(col("file_path"), r"s3a://raw-web-html/([^/]+)/", 1)) \
    .limit(100)   # Limite pour le test

count = df_html.count()
print(f"Nombre de fichiers HTML à traiter : {count}")

# ------------------------------------------------------------
# 5. Extraction
# ------------------------------------------------------------
print("Extraction des professeurs...")
df_faculty = df_html.select(
    explode(faculty_udf_spark(col("value"), col("file_path"), col("domain"))).alias("f")
).select("f.*")

print("Extraction des cours...")
df_courses = df_html.select(
    explode(course_udf_spark(col("value"), col("file_path"), col("domain"))).alias("c")
).select("c.*")

print("Extraction des formations...")
df_programs = df_html.select(
    explode(program_udf_spark(col("value"), col("file_path"), col("domain"))).alias("p")
).select("p.*")

# Dédoublonnage
windowFaculty = Window.partitionBy("record_id").orderBy(col("crawl_timestamp").desc())
df_faculty_dedup = df_faculty.withColumn("rn", row_number().over(windowFaculty)).filter(col("rn") == 1).drop("rn")

windowCourse = Window.partitionBy("course_id").orderBy(col("crawl_timestamp").desc())
df_course_dedup = df_courses.withColumn("rn", row_number().over(windowCourse)).filter(col("rn") == 1).drop("rn")

windowProgram = Window.partitionBy("program_id").orderBy(col("crawl_timestamp").desc())
df_program_dedup = df_programs.withColumn("rn", row_number().over(windowProgram)).filter(col("rn") == 1).drop("rn")

# ------------------------------------------------------------
# 6. Écriture Hudi (avec désactivation de Hive sync)
# ------------------------------------------------------------
def write_hudi(df, table_name, key_field):
    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": key_field,
        "hoodie.datasource.write.precombine.field": "crawl_timestamp",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.clean.automatic": "false",
        "hoodie.datasource.hive_sync.enable": "false",   # CRUCIAL
        "path": f"s3a://curated-zone/{table_name}"
    }
    df.write.format("hudi").options(**opts).mode("append").save()

write_hudi(df_faculty_dedup, "faculty_profiles", "record_id")
write_hudi(df_course_dedup, "course_catalog", "course_id")
write_hudi(df_program_dedup, "programs", "program_id")

print("✅ Écriture terminée. Vérifiez dans MinIO : curated-zone/faculty_profiles, course_catalog, programs")
spark.stop()