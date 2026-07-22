#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL PDF -> Hudi enrichi (documents_catalog)

Pipeline:
raw-documents/*.pdf
-> extraction texte avec pypdf
-> extraction metier generale
-> table Hudi documents_catalog
-> curated-zone/documents_catalog
-> Hive default.documents_catalog

Objectif:
Construire un catalogue documentaire universitaire exploitable:
- metadata technique
- texte brut
- type de document
- entites academiques importantes
- score qualite
- indicateur OCR
"""

import os
import re
import json
import hashlib
import unicodedata
from io import BytesIO
from datetime import datetime, timezone

from pypdf import PdfReader

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, BooleanType, DoubleType
)


# ============================================================
# 1. Spark session
# ============================================================

spark = (
    SparkSession.builder
    .appName("PDFToHudiDocumentsCatalogEnriched")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.cores.max", "2")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Constantes
# ============================================================

RAW_DOCS_PATH = "s3a://raw-documents/"
CURATED_PATH_PREFIX = "s3a://curated-zone"

MAX_FILES = int(os.getenv("PDF_MAX_FILES", "10"))
MAX_FILE_SIZE_MB = int(os.getenv("PDF_MAX_FILE_SIZE_MB", "8"))
MAX_PAGES_PER_PDF = int(os.getenv("PDF_MAX_PAGES_PER_PDF", "5"))
MAX_TEXT_CHARS = int(os.getenv("PDF_MAX_TEXT_CHARS", "20000"))

DAYS_FR = [
    "lundi", "mardi", "mercredi", "jeudi",
    "vendredi", "samedi", "dimanche"
]

ACADEMIC_KEYWORDS = [
    "master", "licence", "filiere", "formation", "semestre",
    "module", "departement", "faculte", "universite",
    "emploi du temps", "planning", "examen", "concours",
    "inscription", "candidature", "soutenance", "resultat",
    "etudiant", "professeur", "coordonnateur"
]

REQUIRED_DOCUMENT_KEYWORDS = [
    "cin", "cne", "apogee", "photo", "diplome", "baccalaureat",
    "releve", "attestation", "cv", "lettre", "demande",
    "copie", "passeport", "formulaire", "quittance"
]

MODULE_HINTS = [
    "cloud", "big data", "data", "spark", "hadoop", "kafka",
    "machine learning", "deep learning", "intelligence artificielle",
    "base de donnees", "sql", "nosql", "reseaux", "securite",
    "programmation", "algorithmique", "mathematiques", "statistique",
    "optimisation", "architecture", "systeme", "web", "mobile"
]


# ============================================================
# 3. Fonctions utilitaires generales
# ============================================================

def normalize_text(text):
    if not text:
        return ""

    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_text(text):
    if not text:
        return ""

    text = text.replace("\x00", " ")

    # Corrige certains textes PDF extraits avec des lettres trop espacees
    # Exemple: D e v e l o p p e r -> Developper
    spaced_letters_pattern = r"(?:\b[A-Za-zÀ-ÿ]\s+){4,}[A-Za-zÀ-ÿ]\b"

    def compact_spaced_letters(match):
        return match.group(0).replace(" ", "")

    text = re.sub(spaced_letters_pattern, compact_spaced_letters, text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()

def to_json(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def unique_keep_order(values, max_items=30):
    seen = set()
    result = []

    for value in values:
        if value is None:
            continue

        value = str(value).strip()
        value = re.sub(r"\s+", " ", value)

        if not value:
            continue

        key = normalize_text(value)
        if key in seen:
            continue

        seen.add(key)
        result.append(value)

        if len(result) >= max_items:
            break

    return result


def join_values(values, max_items=30):
    return "; ".join(unique_keep_order(values, max_items=max_items))


def first_or_empty(values):
    values = unique_keep_order(values, max_items=1)
    return values[0] if values else ""


# ============================================================
# 4. Detection du type/categorie du document
# ============================================================

def guess_document_type(path, text):
    content = normalize_text(f"{path} {text}")

    if any(w in content for w in ["emploi du temps", "planning", "horaire"]):
        return "schedule"

    if any(w in content for w in ["liste des etudiants", "liste d'etudiants", "apogee", "cne"]):
        return "student_list"

    if any(w in content for w in ["resultat", "admis", "liste principale", "liste d'attente"]):
        return "results"

    if any(w in content for w in ["appel", "candidature", "inscription", "concours", "avis aux etudiants"]):
        return "announcement"

    if any(w in content for w in ["master", "licence", "formation", "filiere", "module", "semestre"]):
        return "academic_program"

    if any(w in content for w in ["reglement", "charte", "procedure", "article"]):
        return "administrative"

    return "other"


def guess_document_category(text, document_type):
    content = normalize_text(text)

    if document_type == "schedule":
        return "pedagogy_schedule"

    if document_type == "student_list":
        return "pedagogy_student_list"

    if document_type == "results":
        return "exam_results"

    if any(w in content for w in ["soutenance", "jury", "memoire", "pfe"]):
        return "defense"

    if any(w in content for w in ["examen", "controle", "rattrapage"]):
        return "exam"

    if any(w in content for w in ["inscription", "preinscription", "candidature", "concours"]):
        return "registration"

    if document_type == "administrative":
        return "administration"

    return document_type


# ============================================================
# 5. Extraction metier generale
# ============================================================

def extract_title(text):
    lines = [clean_text(line) for line in text.splitlines()]
    candidates = []

    bad_title_patterns = [
        "www.",
        "http",
        ".ma",
        ".com",
        "page ",
        "universite hassan",
        "faculte des sciences ain chock"
    ]

    for line in lines[:35]:
        line_clean = line.strip(" -_:;")
        nline = normalize_text(line_clean)

        if not line_clean:
            continue

        if any(bad in nline for bad in bad_title_patterns):
            continue

        if re.fullmatch(r"\d+", line_clean):
            continue

        if len(line_clean.split()) < 2:
            continue

        if 8 <= len(line_clean) <= 180:
            candidates.append(line_clean)

    return first_or_empty(candidates)

def extract_academic_year(text):
    normalized = normalize_text(text)

    patterns = [
        r"(20\d{2})\s*[/\-]\s*(20\d{2})",
        r"(20\d{2})\s*[/\-]\s*(\d{2})",
        r"annee universitaire\s*[:\-]?\s*(20\d{2})\s*[/\-]\s*(20\d{2})",
        r"annee universitaire\s*[:\-]?\s*(20\d{2})\s*[/\-]\s*(\d{2})",
        r"au titre de l'?annee universitaire\s*(20\d{2})\s*[/\-]\s*(20\d{2})",
        r"au titre de l'?annee universitaire\s*(20\d{2})\s*[/\-]\s*(\d{2})"
    ]

    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            first_year = match.group(1)
            second_year = match.group(2)

            if len(second_year) == 2:
                second_year = "20" + second_year

            return f"{first_year}/{second_year}"

    return ""

def extract_cycle(text):
    content = normalize_text(text)

    if "master" in content:
        return "Master"

    if "licence" in content:
        return "Licence"

    if "doctorat" in content or "doctoral" in content:
        return "Doctorat"

    if "deust" in content:
        return "DEUST"

    return ""


def extract_semester(text):
    content = normalize_text(text)

    patterns = [
        r"\bsemestre\s*([1-6])\b",
        r"\bs\s*([1-6])\b",
        r"\bsemester\s*([1-6])\b"
    ]

    found = []
    for pattern in patterns:
        for match in re.findall(pattern, content):
            found.append(f"S{match}")

    return join_values(found, max_items=5)


def extract_department(text):
    lines = text.splitlines()
    found = []

    for line in lines:
        nline = normalize_text(line)
        if "departement" in nline:
            found.append(clean_text(line))

    return join_values(found, max_items=5)


def extract_program_name(text):
    lines = [clean_text(line) for line in text.splitlines()]
    found = []

    program_patterns = [
        r"(master\s+[A-Za-zÀ-ÿ0-9&'\- ]{3,100})",
        r"(licence\s+[A-Za-zÀ-ÿ0-9&'\- ]{3,100})",
        r"(filiere\s*[:\-]?\s*[A-Za-zÀ-ÿ0-9&'\- ]{3,100})",
        r"(formation\s*[:\-]?\s*[A-Za-zÀ-ÿ0-9&'\- ]{3,100})"
    ]

    for line in lines:
        for pattern in program_patterns:
            for match in re.findall(pattern, line, flags=re.IGNORECASE):
                value = re.sub(r"\s+", " ", match).strip(" :-")
                if 5 <= len(value) <= 120:
                    found.append(value)

    return join_values(found, max_items=5)


def extract_emails(text):
    pattern = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
    return unique_keep_order(re.findall(pattern, text), max_items=20)


def extract_links(text):
    pattern = r"(https?://[^\s\)\]\}]+|www\.[^\s\)\]\}]+)"
    return unique_keep_order(re.findall(pattern, text), max_items=20)


def extract_phones(text):
    patterns = [
        r"(?:\+212|\+33|\+235|\+221|\+216|\+213|\+1)?[\s.\-]?(?:0)?(?:[5-7])(?:[\s.\-]?\d{2}){4}",
        r"\+\d{1,3}(?:[\s.\-]?\d{2,3}){3,5}"
    ]

    phones = []
    for pattern in patterns:
        phones.extend(re.findall(pattern, text))

    return unique_keep_order(phones, max_items=20)


def extract_dates(text):
    patterns = [
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
        r"\b\d{1,2}\s+(?:janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+20\d{2}\b"
    ]

    normalized = normalize_text(text)
    dates = []

    for pattern in patterns:
        dates.extend(re.findall(pattern, normalized, flags=re.IGNORECASE))

    return unique_keep_order(dates, max_items=30)


def extract_deadline(text):
    normalized = normalize_text(text)

    deadline_patterns = [
        r"(?:date limite|dernier delai|avant le|jusqu au|jusqu'au)\s*[:\-]?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"(?:date limite|dernier delai|avant le|jusqu au|jusqu'au)\s*[:\-]?\s*(\d{1,2}\s+(?:janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+20\d{2})"
    ]

    for pattern in deadline_patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            return match.group(1)

    return ""


def extract_days(text):
    content = normalize_text(text)
    found = [day.capitalize() for day in DAYS_FR if day in content]
    return unique_keep_order(found, max_items=7)


def extract_times(text):
    patterns = [
        r"\b\d{1,2}h\d{0,2}\s*[-a]\s*\d{1,2}h\d{0,2}\b",
        r"\b\d{1,2}:\d{2}\s*[-a]\s*\d{1,2}:\d{2}\b",
        r"\b\d{1,2}h\d{0,2}\b",
        r"\b\d{1,2}:\d{2}\b"
    ]

    normalized = normalize_text(text)
    times = []

    for pattern in patterns:
        times.extend(re.findall(pattern, normalized))

    return unique_keep_order(times, max_items=30)


def extract_rooms(text):
    patterns = [
        r"\b(?:salle|amphi|amphitheatre|laboratoire|lab)\s*(?:[A-Za-z]?\s*\d{1,3}|[A-Za-z])\b",
        r"\b(?:salle|amphi|amphitheatre|laboratoire|lab)\s+[A-Za-z0-9\- ]{1,20}\b"
    ]

    rooms = []

    for pattern in patterns:
        rooms.extend(re.findall(pattern, text, flags=re.IGNORECASE))

    cleaned = []
    forbidden = {
        "s1", "s2", "s3", "s4", "s5", "s6",
        "si", "smi", "smp", "svt", "sta", "sme",
        "sciences", "structures", "soft skills", "skills"
    }

    for room in rooms:
        room = clean_text(room)
        nroom = normalize_text(room)

        if nroom in forbidden:
            continue

        if len(room) < 4 or len(room) > 35:
            continue

        cleaned.append(room)

    return unique_keep_order(cleaned, max_items=20)

def extract_professors(text):
    patterns = [
        r"\b(?:Pr\.?|Prof\.?|Professeur|Dr\.?|Docteur)\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'\-]+(?:\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'\-]+){0,3}",
        r"\b(?:Mme|Madame|M\.|Monsieur)\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'\-]+(?:\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'\-]+){0,3}"
    ]

    found = []
    for pattern in patterns:
        found.extend(re.findall(pattern, text))

    return unique_keep_order(found, max_items=30)


def extract_student_candidates(text):
    lines = [clean_text(line) for line in text.splitlines()]
    found = []

    student_context = False

    for line in lines:
        nline = normalize_text(line)

        if any(w in nline for w in ["liste des etudiants", "nom et prenom", "nom prenom", "etudiants inscrits"]):
            student_context = True
            continue

        if student_context:
            if any(w in nline for w in ["module", "professeur", "responsable", "departement", "signature"]):
                continue

            candidate = re.sub(r"^\d+[\).\-\s]*", "", line)
            candidate = re.sub(r"\b(?:cne|cin|apogee)\b.*", "", candidate, flags=re.IGNORECASE)
            candidate = clean_text(candidate)

            words = candidate.split()
            if 2 <= len(words) <= 5 and 6 <= len(candidate) <= 80:
                if not any(char.isdigit() for char in candidate):
                    found.append(candidate)

    return unique_keep_order(found, max_items=80)


def extract_modules(text):
    lines = [clean_text(line) for line in text.splitlines()]
    found = []

    for line in lines:
        nline = normalize_text(line)

        if "module" in nline or any(hint in nline for hint in MODULE_HINTS):
            candidate = re.sub(r"^(module|element de module|matiere)\s*[:\-]?", "", line, flags=re.IGNORECASE)
            candidate = clean_text(candidate)

            if 4 <= len(candidate) <= 120:
                found.append(candidate)

    return unique_keep_order(found, max_items=30)


def extract_required_documents(text):
    content = normalize_text(text)
    found = []

    for keyword in REQUIRED_DOCUMENT_KEYWORDS:
        if keyword in content:
            found.append(keyword)

    return unique_keep_order(found, max_items=30)


def extract_conditions(text):
    lines = [clean_text(line) for line in text.splitlines()]
    found = []

    for line in lines:
        nline = normalize_text(line)

        if any(w in nline for w in ["condition", "conditions", "doit", "doivent", "obligatoire", "admission", "eligibilite"]):
            if 10 <= len(line) <= 250:
                found.append(line)

    return unique_keep_order(found, max_items=15)


def extract_keywords(text):
    content = normalize_text(text)
    found = []

    for keyword in ACADEMIC_KEYWORDS + REQUIRED_DOCUMENT_KEYWORDS + MODULE_HINTS:
        if keyword in content:
            found.append(keyword)

    return unique_keep_order(found, max_items=40)


def compute_quality_score(text, page_count, extraction_status):
    text_length = len(text or "")

    if extraction_status == "failed":
        return 0.0

    if page_count <= 0:
        return 0.1 if text_length > 0 else 0.0

    density = text_length / max(page_count, 1)

    score = 0.0

    if text_length > 0:
        score += 0.25

    if density >= 300:
        score += 0.35
    elif density >= 100:
        score += 0.20
    elif density >= 30:
        score += 0.10

    if len(extract_keywords(text)) >= 3:
        score += 0.15

    if extract_dates(text):
        score += 0.10

    if extract_emails(text) or extract_phones(text) or extract_links(text):
        score += 0.10

    if extract_professors(text) or extract_student_candidates(text):
        score += 0.05

    return round(min(score, 1.0), 2)


def detect_needs_ocr(text, page_count, extraction_status):
    text_length = len(text or "")

    if extraction_status == "failed":
        return False

    if page_count > 0 and text_length == 0:
        return True

    if page_count > 0 and (text_length / page_count) < 30:
        return True

    return False


def extract_important_entities(text, path, document_type, extraction_status, page_count):
    title = extract_title(text)
    academic_year = extract_academic_year(text)
    cycle = extract_cycle(text)
    program_name = extract_program_name(text)
    semester = extract_semester(text)
    department = extract_department(text)
    document_category = guess_document_category(text, document_type)

    professors = extract_professors(text)
    students = extract_student_candidates(text)
    modules = extract_modules(text)
    rooms = extract_rooms(text)
    days = extract_days(text)
    times = extract_times(text)
    dates = extract_dates(text)
    emails = extract_emails(text)
    phones = extract_phones(text)
    links = extract_links(text)
    required_documents = extract_required_documents(text)
    conditions = extract_conditions(text)
    keywords = extract_keywords(text)
    deadline = extract_deadline(text)

    needs_ocr = detect_needs_ocr(text, page_count, extraction_status)
    quality_score = compute_quality_score(text, page_count, extraction_status)

    entities = {
        "title_detected": title,
        "academic_year": academic_year,
        "cycle": cycle,
        "program_name": program_name,
        "semester": semester,
        "department": department,
        "document_category": document_category,
        "professors_detected": professors,
        "students_detected": students,
        "student_count": len(students),
        "modules_detected": modules,
        "rooms_detected": rooms,
        "days_detected": days,
        "times_detected": times,
        "dates_detected": dates,
        "deadline_detected": deadline,
        "emails_detected": emails,
        "phones_detected": phones,
        "links_detected": links,
        "required_documents": required_documents,
        "conditions_detected": conditions,
        "keywords_detected": keywords,
        "needs_ocr": needs_ocr,
        "extraction_quality_score": quality_score,
        "source_path": path
    }

    return entities


# ============================================================
# 6. Extraction PDF
# ============================================================

def extract_pdf_record(path, content, file_size_bytes):
    now = datetime.now(timezone.utc).isoformat()
    file_name = path.split("/")[-1]
    content_hash = hashlib.md5(content).hexdigest()
    record_id = content_hash

    record = {
        "record_id": record_id,
        "source_path": path,
        "file_name": file_name,
        "file_extension": "pdf",
        "file_size_bytes": int(file_size_bytes),
        "page_count": 0,
        "extracted_pages": 0,
        "text_length": 0,
        "document_type": "unknown",
        "content_text": "",
        "source_system": "web_scraping",
        "crawl_timestamp": now,
        "content_hash": content_hash,
        "is_deleted": False,
        "extraction_status": "failed",
        "error_message": "",
        "extraction_method": "pdf_parsing",

        "title_detected": "",
        "academic_year": "",
        "cycle": "",
        "program_name": "",
        "semester": "",
        "department": "",
        "document_category": "",
        "professors_detected": "",
        "students_detected": "",
        "student_count": 0,
        "modules_detected": "",
        "rooms_detected": "",
        "days_detected": "",
        "times_detected": "",
        "dates_detected": "",
        "deadline_detected": "",
        "emails_detected": "",
        "phones_detected": "",
        "links_detected": "",
        "required_documents": "",
        "conditions_detected": "",
        "keywords_detected": "",
        "needs_ocr": False,
        "extraction_quality_score": 0.0,
        "important_entities_json": "{}"
    }

    try:
        reader = PdfReader(BytesIO(content))
        page_count = len(reader.pages)
        extracted_pages = min(page_count, MAX_PAGES_PER_PDF)

        texts = []

        for i in range(extracted_pages):
            try:
                page_text = reader.pages[i].extract_text() or ""
                texts.append(page_text)
            except Exception as page_error:
                texts.append(f" PAGE_{i + 1}_ERROR: {str(page_error)} ")

        final_text = clean_text("\n".join(texts))
        final_text = final_text[:MAX_TEXT_CHARS]

        extraction_status = "success" if final_text else "empty_text"
        document_type = guess_document_type(path, final_text)

        entities = extract_important_entities(
            text=final_text,
            path=path,
            document_type=document_type,
            extraction_status=extraction_status,
            page_count=page_count
        )

        record["page_count"] = int(page_count)
        record["extracted_pages"] = int(extracted_pages)
        record["text_length"] = int(len(final_text))
        record["document_type"] = document_type
        record["content_text"] = final_text
        record["extraction_status"] = extraction_status
        record["error_message"] = ""

        record["title_detected"] = entities["title_detected"]
        record["academic_year"] = entities["academic_year"]
        record["cycle"] = entities["cycle"]
        record["program_name"] = entities["program_name"]
        record["semester"] = entities["semester"]
        record["department"] = entities["department"]
        record["document_category"] = entities["document_category"]
        record["professors_detected"] = join_values(entities["professors_detected"], max_items=30)
        record["students_detected"] = join_values(entities["students_detected"], max_items=80)
        record["student_count"] = int(entities["student_count"])
        record["modules_detected"] = join_values(entities["modules_detected"], max_items=30)
        record["rooms_detected"] = join_values(entities["rooms_detected"], max_items=30)
        record["days_detected"] = join_values(entities["days_detected"], max_items=7)
        record["times_detected"] = join_values(entities["times_detected"], max_items=30)
        record["dates_detected"] = join_values(entities["dates_detected"], max_items=30)
        record["deadline_detected"] = entities["deadline_detected"]
        record["emails_detected"] = join_values(entities["emails_detected"], max_items=20)
        record["phones_detected"] = join_values(entities["phones_detected"], max_items=20)
        record["links_detected"] = join_values(entities["links_detected"], max_items=20)
        record["required_documents"] = join_values(entities["required_documents"], max_items=30)
        record["conditions_detected"] = join_values(entities["conditions_detected"], max_items=15)
        record["keywords_detected"] = join_values(entities["keywords_detected"], max_items=40)
        record["needs_ocr"] = bool(entities["needs_ocr"])
        record["extraction_quality_score"] = float(entities["extraction_quality_score"])
        record["important_entities_json"] = to_json(entities)

    except Exception as e:
        record["error_message"] = str(e)[:500]
        record["needs_ocr"] = False
        record["extraction_quality_score"] = 0.0

    return record


# ============================================================
# 7. Schema documents_catalog enrichi
# ============================================================

documents_schema = StructType([
    StructField("record_id", StringType()),
    StructField("source_path", StringType()),
    StructField("file_name", StringType()),
    StructField("file_extension", StringType()),
    StructField("file_size_bytes", LongType()),
    StructField("page_count", IntegerType()),
    StructField("extracted_pages", IntegerType()),
    StructField("text_length", IntegerType()),
    StructField("document_type", StringType()),
    StructField("content_text", StringType()),
    StructField("source_system", StringType()),
    StructField("crawl_timestamp", StringType()),
    StructField("content_hash", StringType()),
    StructField("is_deleted", BooleanType()),
    StructField("extraction_status", StringType()),
    StructField("error_message", StringType()),
    StructField("extraction_method", StringType()),

    StructField("title_detected", StringType()),
    StructField("academic_year", StringType()),
    StructField("cycle", StringType()),
    StructField("program_name", StringType()),
    StructField("semester", StringType()),
    StructField("department", StringType()),
    StructField("document_category", StringType()),
    StructField("professors_detected", StringType()),
    StructField("students_detected", StringType()),
    StructField("student_count", IntegerType()),
    StructField("modules_detected", StringType()),
    StructField("rooms_detected", StringType()),
    StructField("days_detected", StringType()),
    StructField("times_detected", StringType()),
    StructField("dates_detected", StringType()),
    StructField("deadline_detected", StringType()),
    StructField("emails_detected", StringType()),
    StructField("phones_detected", StringType()),
    StructField("links_detected", StringType()),
    StructField("required_documents", StringType()),
    StructField("conditions_detected", StringType()),
    StructField("keywords_detected", StringType()),
    StructField("needs_ocr", BooleanType()),
    StructField("extraction_quality_score", DoubleType()),
    StructField("important_entities_json", StringType()),
])


# ============================================================
# 8. Lecture des PDF depuis raw-documents
# ============================================================

print("Lecture des PDF depuis raw-documents...")

max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024

pdf_df = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.pdf")
    .load(RAW_DOCS_PATH)
    .select("path", "length", "content")
    .where(f"length <= {max_bytes}")
    .limit(MAX_FILES)
)

pdf_rows = pdf_df.collect()

print(f"PDF selectionnes : {len(pdf_rows)}")
print(
    f"Limites : "
    f"MAX_FILES={MAX_FILES}, "
    f"MAX_FILE_SIZE_MB={MAX_FILE_SIZE_MB}, "
    f"MAX_PAGES_PER_PDF={MAX_PAGES_PER_PDF}, "
    f"MAX_TEXT_CHARS={MAX_TEXT_CHARS}"
)

if not pdf_rows:
    print("Aucun PDF a traiter. Arret.")
    spark.stop()
    raise SystemExit(0)


# ============================================================
# 9. Extraction PDF + metier
# ============================================================

records = []

for row in pdf_rows:
    print(f"Traitement : {row.path} ({row.length} bytes)")
    record = extract_pdf_record(row.path, row.content, row.length)
    records.append(record)

documents_cols = [f.name for f in documents_schema.fields]

df_documents = spark.createDataFrame(
    [Row(**{c: r.get(c) for c in documents_cols}) for r in records],
    schema=documents_schema
)

df_documents = df_documents.dropDuplicates(["record_id"])

print("Apercu des documents extraits et enrichis :")
df_documents.select(
    "file_name",
    "document_type",
    "document_category",
    "academic_year",
    "cycle",
    "semester",
    "student_count",
    "needs_ocr",
    "extraction_quality_score",
    "extraction_status"
).show(20, truncate=False)


# ============================================================
# 10. Ecriture Hudi avec synchronisation Hive via HMS
# ============================================================

def write_hudi_with_sync(df, table_name, key_field):
    df.persist()
    count = df.count()

    if count == 0:
        print(f"Aucune donnee pour {table_name}, ecriture ignoree.")
        df.unpersist()
        return

    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": key_field,
        "hoodie.datasource.write.precombine.field": "crawl_timestamp",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.clean.automatic": "false",

        "hoodie.upsert.shuffle.parallelism": "4",
        "hoodie.insert.shuffle.parallelism": "4",
        "hoodie.bulkinsert.shuffle.parallelism": "4",
        "hoodie.delete.shuffle.parallelism": "4",
        "hoodie.finalize.write.parallelism": "4",
        "hoodie.metadata.enable": "false",

        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
        "hoodie.datasource.hive_sync.partition_fields": "",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.support_timestamp": "true",

        "path": f"{CURATED_PATH_PREFIX}/{table_name}"
    }

    df.write.format("hudi").options(**opts).mode("append").save()

    print(f"Table {table_name} ecrite avec {count} lignes et synchronisee dans Hive.")
    df.unpersist()


print("Ecriture de documents_catalog dans Hudi avec sync Hive...")
write_hudi_with_sync(df_documents, "documents_catalog", "record_id")

print("Transformation PDF enrichie terminee. Table disponible dans Hive : default.documents_catalog")

spark.stop()