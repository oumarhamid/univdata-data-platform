#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, regexp_extract, row_number
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType
from pyspark.sql.window import Window
import hashlib
from datetime import datetime
from bs4 import BeautifulSoup
import html

# Tentative d'import de chardet pour détection d'encodage
try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False

# ------------------------------------------------------------
# 1. Spark session (identique à l'API)
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("UniversalHTMLToHudi") \
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
# 2. Nettoyage d'encodage
# ------------------------------------------------------------
def clean_text(s):
    if not s:
        return s
    s = html.unescape(s)
    try:
        s = s.encode('latin1').decode('utf-8', errors='ignore')
    except:
        pass
    return s

# ------------------------------------------------------------
# 3. Extraction des professeurs
# ------------------------------------------------------------
def extract_faculty_from_html(html_content, source_url, domain):
    if not html_content:
        return []
    html_content = clean_text(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')
    faculty_list = []
    
    name_pattern = r'(?:Pr\.|Professeur|Prof\.|Dr\.|M\.|Mme|Mlle)\s+([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})'
    
    for element in soup.find_all(['p', 'div', 'li', 'td', 'span', 'article', 'section', 'h1', 'h2', 'h3', 'h4']):
        text = element.get_text(' ', strip=True)
        text = clean_text(text)
        if not text:
            continue
        for match in re.finditer(name_pattern, text, re.IGNORECASE):
            full_name = clean_text(match.group(1).strip())
            word_count = len(full_name.split())
            if word_count < 2 or word_count > 4:
                continue
            if re.search(r'Pr\.|Professeur|Prof\.', match.group(0), re.I):
                title = "Professeur"
            elif re.search(r'Dr\.', match.group(0), re.I):
                title = "Docteur"
            else:
                title = "Enseignant"
            
            department = ""
            context = text[max(0, text.find(full_name)-200):text.find(full_name)+200]
            dept_match = re.search(r'(?:département|filière|équipe|laboratoire)\s+de\s+([A-Za-zÀ-ÖØ-Þ\s]+?)(?:\.|,|$)', context, re.I)
            if dept_match:
                department = clean_text(dept_match.group(1).strip())
            
            record_id = hashlib.md5((full_name + domain + source_url).encode()).hexdigest()
            faculty_list.append({
                "record_id": record_id,
                "full_name": full_name,
                "title": title,
                "department": department,
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
    
    # Deuxième passe pour les responsables
    for element in soup.find_all(['div', 'p', 'li']):
        text = clean_text(element.get_text(' ', strip=True))
        leader_match = re.search(r'(?:Chef de département|Responsable|Coordonnateur|Directeur)\s*:\s*(?:Pr\.|Prof\.|Dr\.)?\s*([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})', text, re.I)
        if leader_match:
            full_name = clean_text(leader_match.group(1).strip())
            word_count = len(full_name.split())
            if 2 <= word_count <= 4 and not any(f['full_name'] == full_name for f in faculty_list):
                record_id = hashlib.md5((full_name + domain + source_url).encode()).hexdigest()
                faculty_list.append({
                    "record_id": record_id,
                    "full_name": full_name,
                    "title": "Professeur",
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

# ------------------------------------------------------------
# 4. Extraction des formations
# ------------------------------------------------------------
def extract_programs_from_html(html_content, source_url, domain):
    if not html_content:
        return []
    html_content = clean_text(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')
    programs = []
    
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                name = clean_text(cells[0].get_text(strip=True))
                if name and len(name) > 5 and not re.search(r'Intitulé|Titre|Nom', name, re.I):
                    level = "Formation"
                    txt = table.get_text().lower()
                    if 'master' in txt:
                        level = "Master"
                    elif 'licence' in txt:
                        level = "Licence"
                    elif 'doctorat' in txt:
                        level = "Doctorat"
                    responsible = ""
                    if len(cells) > 1:
                        resp_cell = clean_text(cells[1].get_text(strip=True))
                        m = re.search(r'(?:Pr\.|Prof\.|Dr\.)?\s*([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})', resp_cell)
                        if m:
                            responsible = clean_text(m.group(1).strip())
                    program_id = hashlib.md5((name + domain).encode()).hexdigest()
                    programs.append({
                        "program_id": program_id,
                        "program_name": name,
                        "level": level,
                        "department": "",
                        "faculty": domain,
                        "responsible": responsible,
                        "source_url": source_url,
                        "crawl_timestamp": datetime.now().isoformat()
                    })
    
    for card in soup.find_all(['div', 'article'], class_=re.compile(r'card|formation|program|course|fil')):
        name_tag = card.find(['h3', 'h4', 'strong', 'span'], class_=re.compile(r'title|name|intitule')) or card.find(['h3', 'h4', 'strong'])
        if name_tag:
            name = clean_text(name_tag.get_text(strip=True))
            if len(name) >= 5:
                card_text = card.get_text().lower()
                level = "Formation"
                if 'master' in card_text:
                    level = "Master"
                elif 'licence' in card_text or 'license' in card_text:
                    level = "Licence"
                elif 'doctorat' in card_text or 'phd' in card_text:
                    level = "Doctorat"
                elif 'lp' in card_text or 'professionnelle' in card_text:
                    level = "Licence Professionnelle"
                responsible = ""
                m = re.search(r'(?:Responsable|Coordonnateur|Directeur)\s*:\s*(?:Pr\.|Prof\.|Dr\.)?\s*([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})', card.get_text(), re.I)
                if m:
                    responsible = clean_text(m.group(1).strip())
                program_id = hashlib.md5((name + domain).encode()).hexdigest()
                programs.append({
                    "program_id": program_id,
                    "program_name": name,
                    "level": level,
                    "department": "",
                    "faculty": domain,
                    "responsible": responsible,
                    "source_url": source_url,
                    "crawl_timestamp": datetime.now().isoformat()
                })
    
    for a in soup.find_all('a', href=True):
        if '.pdf' in a['href'].lower() and not a.get_text(strip=True).startswith('télécharger'):
            name = clean_text(a.get_text(strip=True))
            if name and len(name) > 5:
                href = a['href'].lower()
                level = "Formation"
                if 'master' in href or 'master' in name.lower():
                    level = "Master"
                elif 'licence' in href or 'licence' in name.lower():
                    level = "Licence"
                elif 'doctorat' in href or 'doctorat' in name.lower():
                    level = "Doctorat"
                program_id = hashlib.md5((name + domain).encode()).hexdigest()
                programs.append({
                    "program_id": program_id,
                    "program_name": name,
                    "level": level,
                    "department": "",
                    "faculty": domain,
                    "responsible": "",
                    "source_url": source_url,
                    "crawl_timestamp": datetime.now().isoformat()
                })
    
    unique = {}
    for p in programs:
        if p["program_name"] not in unique:
            unique[p["program_name"]] = p
    return list(unique.values())

# ------------------------------------------------------------
# 5. Schémas et UDF
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

program_schema = ArrayType(StructType([
    StructField("program_id", StringType()),
    StructField("program_name", StringType()),
    StructField("level", StringType()),
    StructField("department", StringType()),
    StructField("faculty", StringType()),
    StructField("responsible", StringType()),
    StructField("source_url", StringType()),
    StructField("crawl_timestamp", StringType())
]))

faculty_udf = udf(extract_faculty_from_html, faculty_schema)
program_udf = udf(extract_programs_from_html, program_schema)

# ------------------------------------------------------------
# 6. Lecture des fichiers HTML depuis MinIO (sans filtre de taille)
# ------------------------------------------------------------
print("📖 Récupération de la liste des fichiers HTML via MinIO API...")

def list_valid_html_keys():
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        use_ssl=False,
        config=Config(signature_version='s3v4')
    )
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket='raw-web-html', Prefix='')
    valid = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.html') and not re.search(r'%20|\s|:', key):
                    valid.append(key)
    return valid

valid_keys = list_valid_html_keys()
print(f"📄 {len(valid_keys)} fichiers HTML valides trouvés.")
if not valid_keys:
    print("Aucun fichier valide. Arrêt.")
    spark.stop()
    exit(0)

def read_s3_file(key):
    try:
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            use_ssl=False,
            config=Config(signature_version='s3v4')
        )
        response = s3.get_object(Bucket='raw-web-html', Key=key)
        raw = response['Body'].read()
        if HAS_CHARDET:
            enc = chardet.detect(raw)['encoding'] or 'utf-8'
        else:
            enc = 'utf-8'
        content = raw.decode(enc, errors='replace')
        content = html.unescape(content)
        # Plus de filtre de taille
        return (f"s3a://raw-web-html/{key}", content, key)
    except Exception as e:
        print(f"Erreur lecture {key}: {e}")
        return None

rdd = spark.sparkContext.parallelize(valid_keys, min(spark.sparkContext.defaultParallelism, len(valid_keys)))
file_content_rdd = rdd.map(read_s3_file).filter(lambda x: x is not None)
df_html = file_content_rdd.toDF(["file_path", "content", "raw_key"])

df_html = df_html.withColumn("domain", regexp_extract(col("raw_key"), r"^([^/]+)/", 1))

print(f"📄 {df_html.count()} fichiers HTML lus.")
print(f"   Domaines: {[row.domain for row in df_html.select('domain').distinct().collect()]}")

# ------------------------------------------------------------
# 7. Extraction
# ------------------------------------------------------------
print("🔎 Extraction des professeurs...")
df_faculty = df_html.select(explode(faculty_udf(col("content"), col("file_path"), col("domain"))).alias("f")).select("f.*")

print("🎓 Extraction des formations...")
df_programs = df_html.select(explode(program_udf(col("content"), col("file_path"), col("domain"))).alias("p")).select("p.*")

# ------------------------------------------------------------
# 8. Dédoublonnage
# ------------------------------------------------------------
windowFaculty = Window.partitionBy("record_id").orderBy(col("crawl_timestamp").desc())
df_faculty = df_faculty.withColumn("rn", row_number().over(windowFaculty)).filter(col("rn") == 1).drop("rn")

windowProgram = Window.partitionBy("program_id").orderBy(col("crawl_timestamp").desc())
df_programs = df_programs.withColumn("rn", row_number().over(windowProgram)).filter(col("rn") == 1).drop("rn")

# ------------------------------------------------------------
# 9. Écriture Hudi avec synchronisation Hive (mode HMS)
# ------------------------------------------------------------
def write_hudi_with_sync(df, table_name, key_field):
    if df.count() == 0:
        print(f"⚠️ Aucune donnée pour {table_name}, écriture ignorée.")
        return

    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": key_field,
        "hoodie.datasource.write.precombine.field": "crawl_timestamp",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.clean.automatic": "false",

        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
        "hoodie.datasource.hive_sync.partition_fields": "",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.support_timestamp": "true",

        "path": f"s3a://curated-zone/{table_name}"
    }

    df.write.format("hudi").options(**opts).mode("append").save()
    print(f"✅ Table {table_name} écrite avec {df.count()} lignes et synchronisée dans Hive.")

print("💾 Écriture des tables Hudi avec sync Hive (mode HMS)...")
write_hudi_with_sync(df_faculty, "faculty_profiles", "record_id")
write_hudi_with_sync(df_programs, "programs", "program_id")

print("🎉 Transformation HTML terminée. Tables disponibles dans Hive (base default).")
spark.stop()