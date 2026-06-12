#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, input_file_name, regexp_extract, row_number
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, BooleanType
from pyspark.sql.window import Window
from bs4 import BeautifulSoup
import hashlib
import re
from datetime import datetime
from urllib.parse import urljoin

# ------------------------------------------------------------
# 1. Spark session (sans Hive)
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("UniversityETL_Advanced") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# 2. Fonctions d'extraction avancées (BeautifulSoup)
# ------------------------------------------------------------
def extract_faculty(html, base_url):
    """Extrait les noms de professeurs en analysant le DOM."""
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    faculty = []
    # Mots-clés indicateurs de professeurs
    keywords = ['professeur', 'pr.', 'dr.', 'prof', 'enseignant', 'm.', 'mme', 'm lle', 'responsable', 'intervenant']
    # Balises candidates
    tags = soup.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'div', 'li', 'td', 'span'])
    for tag in tags:
        text = tag.get_text(strip=True)
        if not text:
            continue
        # Vérifier la présence d'un mot-clé
        lower_text = text.lower()
        if any(kw in lower_text for kw in keywords):
            # Nettoyer pour ne garder qu'une phrase courte
            # On cherche à isoler le nom (on suppose qu'il commence par une majuscule et contient au moins deux mots)
            # On utilise une regex simple pour capturer un nom probable
            match = re.search(r'([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+)+)', text)
            if match:
                name = match.group(1).strip()
                if len(name) > 5:
                    faculty.append(name)
    # Supprimer les doublons
    faculty = list(set(faculty))
    return faculty

def extract_courses(html, base_url):
    """Extrait les cours à partir des liens vers des documents."""
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    courses = []
    doc_extensions = ('.pdf', '.doc', '.docx', '.odt', '.rtf')
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        if href.endswith(doc_extensions):
            text = a.get_text(strip=True)
            if text and any(kw in text.lower() for kw in ['cours', 'syllabus', 'programme', 'td', 'tp', 'polycopié', 'support', 'fiche']):
                full_url = urljoin(base_url, a['href'])
                courses.append((text[:150], full_url))
    return courses

def extract_programs(html):
    """Extrait les noms de formations (Licence, Master, etc.)."""
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    programs = []
    # Mots-clés pour les formations
    keywords = ['licence', 'master', 'doctorat', 'filière', 'département', 'parcours', 'formation']
    # Balises de titre et paragraphes
    for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'div']):
        text = tag.get_text(strip=True)
        if not text:
            continue
        lower_text = text.lower()
        if any(kw in lower_text for kw in keywords):
            # On capture une phrase courte (max 150 caractères)
            # On nettoie les retours à la ligne et espaces multiples
            clean_text = ' '.join(text.split())
            # Tronquer après une virgule ou un point si c'est trop long
            if len(clean_text) > 150:
                clean_text = clean_text[:150].rsplit('.', 1)[0] + '.'
            programs.append(clean_text)
    # Supprimer les doublons
    programs = list(set(programs))
    return programs

# ------------------------------------------------------------
# 3. UDF (User Defined Functions) avec schémas
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
    names = extract_faculty(html, source_url)
    if not names:
        names = ["Exemple Professeur"]
    result = []
    for name in names:
        record_id = hashlib.md5((name + domain + source_url).encode()).hexdigest()
        result.append({
            "record_id": record_id,
            "full_name": name,
            "title": "Professeur" if any(kw in name.lower() for kw in ['professeur', 'pr.', 'prof']) else "Enseignant",
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
    for name, doc_url in courses:
        course_id = hashlib.md5((doc_url + name).encode()).hexdigest()
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
        # Déterminer le niveau
        lower_prog = prog.lower()
        if 'licence' in lower_prog:
            level = "Licence"
        elif 'master' in lower_prog:
            level = "Master"
        elif 'doctorat' in lower_prog:
            level = "Doctorat"
        else:
            level = "Formation"
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
# 4. Lecture des fichiers HTML
# ------------------------------------------------------------
print("🔍 Lecture des fichiers HTML depuis MinIO...")
df_html = spark.read.option("wholetext", "true").text("s3a://raw-web-html/*/*/*.html") \
    .withColumn("file_path", input_file_name()) \
    .withColumn("domain", regexp_extract(col("file_path"), r"s3a://raw-web-html/([^/]+)/", 1))

# Pour test, limiter à 200 fichiers (supprimez .limit() pour tout traiter)
df_html = df_html.limit(200)
total = df_html.count()
print(f"📄 {total} fichiers HTML à traiter")

# ------------------------------------------------------------
# 5. Extraction et écriture Hudi
# ------------------------------------------------------------
print("🔎 Extraction des professeurs...")
df_faculty = df_html.select(
    explode(faculty_udf_spark(col("value"), col("file_path"), col("domain"))).alias("f")
).select("f.*")

print("📚 Extraction des cours...")
df_courses = df_html.select(
    explode(course_udf_spark(col("value"), col("file_path"), col("domain"))).alias("c")
).select("c.*")

print("🎓 Extraction des formations...")
df_programs = df_html.select(
    explode(program_udf_spark(col("value"), col("file_path"), col("domain"))).alias("p")
).select("p.*")

# Dédoublonnage
windowFaculty = Window.partitionBy("record_id").orderBy(col("crawl_timestamp").desc())
df_faculty = df_faculty.withColumn("rn", row_number().over(windowFaculty)).filter(col("rn") == 1).drop("rn")

windowCourse = Window.partitionBy("course_id").orderBy(col("crawl_timestamp").desc())
df_courses = df_courses.withColumn("rn", row_number().over(windowCourse)).filter(col("rn") == 1).drop("rn")

windowProgram = Window.partitionBy("program_id").orderBy(col("crawl_timestamp").desc())
df_programs = df_programs.withColumn("rn", row_number().over(windowProgram)).filter(col("rn") == 1).drop("rn")

# Écriture Hudi (désactivation totale de Hive)
def write_hudi(df, table_name, key_field):
    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": key_field,
        "hoodie.datasource.write.precombine.field": "crawl_timestamp",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.clean.automatic": "false",
        "hoodie.datasource.hive_sync.enable": "false",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "path": f"s3a://curated-zone/{table_name}"
    }
    df.write.format("hudi").options(**opts).mode("append").save()
    print(f"✅ Table {table_name} écrite avec {df.count()} lignes.")

print("💾 Écriture des tables Hudi...")
write_hudi(df_faculty, "faculty_profiles", "record_id")
write_hudi(df_courses, "course_catalog", "course_id")
write_hudi(df_programs, "programs", "program_id")

print("🎉 Toutes les tables ont été créées dans curated-zone.")
spark.stop()