#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL PDF -> Hudi reduit (documents_catalog)

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
- entites academiques importantes uniquement
- score qualite
- indicateur OCR

Le schema physique conserve uniquement les champs essentiels.
"""

import os
import re
import json
import hashlib
import unicodedata
from io import BytesIO
from datetime import datetime, timezone

import boto3
from pypdf import PdfReader

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, BooleanType, DoubleType
)


# ============================================================
# 1. Spark session
# ============================================================

# Configuration MinIO : variables d'environnement avec valeurs locales de secours.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")
)
MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
)

spark = (
    SparkSession.builder
    .appName("PDFToHudiDocumentsCatalogEnriched")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
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


def contains_term(normalized_text, term):
    """Recherche un mot ou une expression sans faux positif dans un mot plus long."""
    normalized_term = normalize_text(term)
    pattern = r"(?<!\w)" + re.escape(normalized_term).replace(r"\ ", r"\s+") + r"(?!\w)"
    return re.search(pattern, normalized_text) is not None


def clean_text(text):
    """Nettoie le texte extrait sans détruire sa structure de lignes."""
    if not text:
        return ""

    text = unicodedata.normalize("NFKC", str(text))
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\x00", " ").replace("\ufeff", " ")
    text = text.replace("\u00a0", " ").replace("\u202f", " ")
    text = text.replace("\u00ad", "")
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060\u2066-\u2069]", "", text)

    # Ligatures fréquemment rencontrées dans les PDF.
    ligatures = {
        "ﬀ": "ff", "ﬁ": "fi", "ﬂ": "fl",
        "ﬃ": "ffi", "ﬄ": "ffl", "ﬅ": "st", "ﬆ": "st",
    }
    for source, target in ligatures.items():
        text = text.replace(source, target)

    # Supprime le bruit généré par certaines polices PDF : /gid123, gid 123, etc.
    text = re.sub(r"(?i)(?<![A-Za-z0-9])/?gid\s*\d+\b", " ", text)

    # Les caractères de remplacement ne portent pas d'information exploitable.
    text = text.replace("\ufffd", " ")

    # Recolle les mots coupés uniquement à un saut de ligne : transfor-\nmation.
    text = re.sub(r"(?<=[A-Za-zÀ-ÖØ-öø-ÿ])[-‐‑]\s*\n\s*(?=[A-Za-zÀ-ÖØ-öø-ÿ])", "", text)

    # Corrige certains textes PDF extraits avec des lettres trop espacées.
    spaced_letters_pattern = r"(?:\b[A-Za-zÀ-ÿ]\s+){4,}[A-Za-zÀ-ÿ]\b"
    text = re.sub(spaced_letters_pattern, lambda m: re.sub(r"\s+", "", m.group(0)), text)

    # Sépare les mots CamelCase et les frontières lettre/chiffre évidentes.
    text = re.sub(r"(?<=[a-zà-öø-ÿ])(?=[A-ZÀ-ÖØ-Þ])", " ", text)
    text = re.sub(r"(?<=[A-Za-zÀ-ÖØ-öø-ÿ])(?=\d)", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Za-zÀ-ÖØ-öø-ÿ])", " ", text)

    cleaned_lines = []
    for line in text.split("\n"):
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            cleaned_lines.append(line)

    text = "\n".join(cleaned_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compute_text_quality_metrics(raw_text, cleaned_text, extracted_pages):
    """Mesure la propreté réelle du texte avant et après nettoyage."""
    raw_text = raw_text or ""
    cleaned_text = cleaned_text or ""

    gid_count = len(re.findall(r"(?i)(?<![A-Za-z0-9])/?gid\s*\d+\b", raw_text))
    replacement_char_count = raw_text.count("\ufffd")
    control_char_count = sum(
        1 for char in raw_text
        if unicodedata.category(char).startswith("C") and char not in "\n\r\t"
    )

    tokens = re.findall(r"\S+", cleaned_text)
    suspicious_long_token_count = sum(
        1 for token in tokens
        if len(token) >= 20 and not token.startswith(("http://", "https://", "www."))
    )

    visible_chars = [char for char in cleaned_text if not char.isspace()]
    alnum_ratio = (
        sum(char.isalnum() for char in visible_chars) / len(visible_chars)
        if visible_chars else 0.0
    )

    raw_length = len(raw_text)
    cleaned_length = len(cleaned_text)
    weighted_noise = (
        gid_count * 5
        + replacement_char_count * 3
        + control_char_count * 2
        + suspicious_long_token_count * 4
    )
    noise_ratio = min(weighted_noise / max(raw_length, 1), 1.0)
    density_per_extracted_page = cleaned_length / max(extracted_pages, 1)
    density_component = min(density_per_extracted_page / 500.0, 1.0)

    cleaning_quality_score = round(max(0.0, min(
        0.50 * alnum_ratio
        + 0.30 * (1.0 - noise_ratio)
        + 0.20 * density_component,
        1.0,
    )), 2)

    return {
        "raw_text_length": raw_length,
        "cleaned_text_length": cleaned_length,
        "gid_count": gid_count,
        "replacement_char_count": replacement_char_count,
        "control_char_count": control_char_count,
        "suspicious_long_token_count": suspicious_long_token_count,
        "alnum_ratio": round(alnum_ratio, 4),
        "noise_ratio": round(noise_ratio, 4),
        "density_per_extracted_page": round(density_per_extracted_page, 2),
        "cleaning_quality_score": cleaning_quality_score,
    }

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

    if any(contains_term(content, w) for w in ["emploi du temps", "planning", "horaire"]):
        return "schedule"

    if any(contains_term(content, w) for w in ["liste des etudiants", "liste d'etudiants", "apogee", "cne"]):
        return "student_list"

    if any(contains_term(content, w) for w in ["resultat", "admis", "liste principale", "liste d'attente"]):
        return "results"

    if any(contains_term(content, w) for w in ["appel", "candidature", "inscription", "concours", "avis aux etudiants"]):
        return "announcement"

    if any(contains_term(content, w) for w in ["master", "licence", "formation", "filiere", "module", "semestre"]):
        return "academic_program"

    if any(contains_term(content, w) for w in ["reglement", "charte", "procedure", "article"]):
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

    # Une simple mention de soutenance/PFE dans un programme ne suffit pas.
    # On exige un indice fort qui décrit réellement un avis ou un planning de soutenance.
    defense_signals = [
        "avis de soutenance", "jury de soutenance",
        "calendrier des soutenances", "planning des soutenances",
        "date de soutenance", "heure de soutenance", "lieu de soutenance",
    ]
    if any(contains_term(content, signal) for signal in defense_signals):
        return "defense"

    if any(contains_term(content, w) for w in ["examen", "controle", "rattrapage"]):
        return "exam"

    if any(contains_term(content, w) for w in ["inscription", "preinscription", "candidature", "concours"]):
        return "registration"

    if document_type == "academic_program":
        return "academic_program"

    if document_type == "administrative":
        return "administration"

    return document_type


# ============================================================
# 5. Extraction metier generale
# ============================================================

def looks_like_table_header(line):
    """Repère les en-têtes de tableaux pour éviter de les présenter comme titres."""
    normalized = normalize_text(line)
    header_terms = [
        "n apogee", "apogee", "nom", "prenom", "groupe", "gr",
        "code", "etape", "cne", "module", "note", "session",
    ]
    matched_terms = sum(contains_term(normalized, term) for term in header_terms)

    words = re.findall(r"[A-Za-zÀ-ÿ0-9]+", line)
    short_word_ratio = (
        sum(len(word) <= 4 for word in words) / len(words)
        if words else 0.0
    )

    repeated_numeric_columns = len(re.findall(r"\b\d{1,2}\b", line)) >= 3
    return (matched_terms >= 3) or (len(words) >= 10 and short_word_ratio >= 0.55) or repeated_numeric_columns


def extract_title(text, document_type=""):
    lines = [clean_text(line) for line in (text or "").splitlines()]
    lines = [line for line in lines if line]
    candidates = []

    bad_title_patterns = [
        "www.", "http", ".ma", ".com", "page ",
        "universite hassan", "faculte des sciences ain chock",
        "royaume du maroc", "telephone", "tel :", "email",
    ]
    title_hints = [
        "master", "licence", "emploi du temps", "planning",
        "avis", "concours", "resultat", "soutenance", "formation",
        "programme", "calendrier", "liste", "examen",
    ]

    for index, line in enumerate(lines[:40]):
        line_clean = line.strip(" -_:;|•")
        nline = normalize_text(line_clean)

        if not line_clean or any(bad in nline for bad in bad_title_patterns):
            continue
        if re.fullmatch(r"[\d\W_]+", line_clean):
            continue
        if len(line_clean.split()) < 2 or not (8 <= len(line_clean) <= 180):
            continue
        if "@" in line_clean or re.search(r"\b\d{6,}\b", line_clean):
            continue
        if looks_like_table_header(line_clean):
            continue

        words = re.findall(r"[A-Za-zÀ-ÿ]+", line_clean)
        has_suspicious_joined_word = any(len(word) >= 20 for word in words)
        has_title_hint = any(hint in nline for hint in title_hints)
        uppercase_ratio = (
            sum(char.isupper() for char in line_clean if char.isalpha())
            / max(sum(char.isalpha() for char in line_clean), 1)
        )
        title_case_ratio = (
            sum(word[:1].isupper() for word in words) / len(words)
            if words else 0.0
        )

        # Pas de reconstruction artificielle : une ligne douteuse reste sans titre.
        if has_suspicious_joined_word:
            continue
        if len(words) > 12 and not has_title_hint:
            continue
        if document_type == "student_list" and not has_title_hint:
            continue
        if not (has_title_hint or uppercase_ratio >= 0.65 or title_case_ratio >= 0.60):
            continue

        score = max(0, 20 - index) * 0.03
        score += 1.2 if has_title_hint else 0.0
        score += 0.4 if 15 <= len(line_clean) <= 100 else 0.0
        score += 0.2 if line_clean.isupper() else 0.0
        score -= 0.8 if line_clean.endswith((".", ";")) and len(line_clean) > 100 else 0.0
        candidates.append((score, index, line_clean))

        # Les titres PDF sont parfois répartis sur deux lignes consécutives.
        if index + 1 < len(lines):
            combined = f"{line_clean} {lines[index + 1]}".strip()
            normalized_combined = normalize_text(combined)
            next_normalized = normalize_text(lines[index + 1])
            if (
                15 <= len(combined) <= 180
                and not any(bad in normalized_combined for bad in bad_title_patterns)
                and not looks_like_table_header(combined)
                and not any(len(word) >= 20 for word in re.findall(r"[A-Za-zÀ-ÿ]+", combined))
                and not re.match(r"^(annee|année|20\d{2}|page|date)\b", next_normalized)
                and len(combined.split()) >= 3
            ):
                combined_score = score + 0.15
                candidates.append((combined_score, index, combined))

    if not candidates:
        return ""

    candidates.sort(key=lambda item: (-item[0], item[1], len(item[2])))
    best_score, _, best_title = candidates[0]
    return best_title if best_score >= 0.85 else ""

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
    """Extrait uniquement de vrais semestres et les normalise en S1...S6."""
    content = normalize_text(text)
    found = set()

    for match in re.findall(r"\bsemestre\s*(?:n[o°]?\s*)?([1-6])\b", content):
        found.add(int(match))

    roman_map = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6}
    for match in re.findall(r"\bsemestre\s*(i{1,3}|iv|v|vi)\b", content):
        value = roman_map.get(match)
        if value:
            found.add(value)

    # S1, S 1, S-1. On n'utilise jamais les codes M1/M2 des modules.
    for match in re.findall(r"(?<![a-z0-9])s\s*[-:]?\s*([1-6])(?!\d)", content):
        found.add(int(match))

    return "; ".join(f"S{value}" for value in sorted(found))

def extract_department(text):
    lines = text.splitlines()
    found = []

    for line in lines:
        nline = normalize_text(line)
        if "departement" in nline:
            found.append(clean_text(line))

    return join_values(found, max_items=5)


def extract_program_name(text):
    """Retourne un seul intitulé de formation fiable, sans phrases annexes."""
    lines = [clean_text(line) for line in (text or "").splitlines()]
    lines = [line for line in lines if line]

    stop_terms = [
        "programme de la formation", "semestre", "objectifs", "competences",
        "debouches", "conditions", "informations", "responsable", "email",
        "adresse", "deroulement", "cout de la formation", "facilites",
    ]

    def is_stop(line):
        nline = normalize_text(line)
        return any(term in nline for term in stop_terms)

    candidates = []

    for index, line in enumerate(lines[:80]):
        nline = normalize_text(line)

        # Cas des listes d'inscrits : FILIERE : SMIA S 1 -> SMIA.
        filiere_match = re.search(r"\bfili[eè]re\s*[:\-]\s*(.+)$", line, flags=re.IGNORECASE)
        if filiere_match:
            value = filiere_match.group(1).strip(" :-")
            value = re.sub(r"\s+S\s*[1-6](?:\s*[+;/]\s*S\s*[1-6])*$", "", value, flags=re.IGNORECASE)
            value = re.sub(r"\s+", " ", value).strip()
            normalized_filiere = normalize_text(value)
            should_extend_filiere = (
                normalized_filiere in {"master", "licence", "deust", "doctorat"}
                or value.lower().endswith((" de", " des", " d'", " et", " en"))
            )
            if should_extend_filiere and index + 1 < len(lines):
                next_line = lines[index + 1].strip(" :-")
                if (
                    next_line and not is_stop(next_line)
                    and not re.search(r"\b(fili[eè]re|objectif|semestre|s\s*[1-6])\b", normalize_text(next_line))
                    and len(next_line) <= 60
                    and len((value + " " + next_line).split()) <= 10
                ):
                    value = f"{value} {next_line}"
            if 2 <= len(value) <= 120 and not is_stop(value):
                candidates.append((3.0, value))

        if not re.match(r"^(master|licence|deust|doctorat)\b", nline):
            continue
        if any(term in nline for term in ["etre titulaire", "poursuite", "acceder", "titulaire du"]):
            continue

        value = line.strip(" :-")
        normalized_value = normalize_text(value)
        generic_titles = {
            "master", "master specialise", "master d'universite specialise",
            "master universitaire specialise", "licence", "licence professionnelle",
            "licence fondamentale", "doctorat", "deust",
        }
        should_extend = (
            normalized_value in generic_titles
            or value.lower().endswith((" et", " de", " d'", " en"))
        )

        # Les intitulés génériques sont parfois répartis sur 2 ou 3 lignes.
        if should_extend:
            for offset in (1, 2):
                if index + offset >= len(lines):
                    break
                next_line = lines[index + offset].strip(" :-")
                if not next_line or is_stop(next_line):
                    break
                if re.match(r"^(M\s*\d+\s*:|[•\-–—])", next_line, flags=re.IGNORECASE):
                    break
                if len(next_line) > 90 or len((value + " " + next_line).split()) > 16:
                    break
                value = f"{value} {next_line}"
                if not value.lower().endswith((" et", " de", " d'", " en")) and offset >= 2:
                    break

        value = re.sub(r"\s+", " ", value).strip(" :-")
        nvalue = normalize_text(value)
        if any(term in nvalue for term in ["responsable", "email", "samedi", "dimanche", "dh par an"]):
            continue
        if 5 <= len(value) <= 160:
            score = 2.5 if nvalue.startswith(("master", "licence", "deust", "doctorat")) else 1.0
            candidates.append((score, value))

    if not candidates:
        return ""

    candidates.sort(key=lambda item: (-item[0], len(item[1])))
    return candidates[0][1]

def extract_emails(text):
    """Extrait les e-mails standards et ceux espacés par l'OCR."""
    email_pattern = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
    found = re.findall(email_pattern, text or "")

    for line in (text or "").splitlines():
        if "@" not in line:
            continue
        # Ex.: reda.chbihi@univh 2 c.ma -> reda.chbihi@univh2c.ma
        compact = re.sub(r"\s+", "", line)
        found.extend(re.findall(email_pattern, compact))

    cleaned = []
    for email in found:
        email = email.strip(".,;:()[]{}<>").lower()
        if len(email) <= 254 and ".." not in email:
            cleaned.append(email)
    return unique_keep_order(cleaned, max_items=20)

def extract_links(text):
    pattern = r"(https?://[^\s\)\]\}]+|www\.[^\s\)\]\}]+)"
    return unique_keep_order(re.findall(pattern, text), max_items=20)


def extract_phones(text):
    """Extrait uniquement des numéros plausibles, jamais les numéros Apogée."""
    candidates = []
    # Le candidat doit commencer par 0, + ou 00. Cela exclut les lignes "5 20014128".
    pattern = r"(?<!\d)(?:\+\s*|00\s*|0)[0-9\s.()\-/]{8,28}"
    candidates.extend(re.findall(pattern, text or ""))

    phones = []
    for candidate in candidates:
        value = re.sub(r"[^0-9+]", "", candidate)
        if value.startswith("00"):
            value = "+" + value[2:]
        if value.startswith("+2120"):
            value = "+212" + value[5:]

        # Maroc local : 05/06/07 + 8 chiffres.
        if re.fullmatch(r"0[5-7]\d{8}", value):
            phones.append(value)
            continue

        # Maroc international : +212 5/6/7 + 8 chiffres.
        if re.fullmatch(r"\+212[5-7]\d{8}", value):
            phones.append(value)
            continue

        # Autre international : accepté uniquement avec un + explicite et 10 à 15 chiffres.
        if re.fullmatch(r"\+(?!212)\d{10,15}", value):
            phones.append(value)

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
    """Extrait des heures de séance, pas des volumes horaires comme 48h."""
    normalized = normalize_text(text)
    times = []

    range_patterns = [
        r"\b(?:[01]?\d|2[0-3])h(?:[0-5]\d)?\s*(?:-|a|à)\s*(?:[01]?\d|2[0-3])h(?:[0-5]\d)?\b",
        r"\b(?:[01]?\d|2[0-3]):[0-5]\d\s*(?:-|a|à)\s*(?:[01]?\d|2[0-3]):[0-5]\d\b",
    ]
    for pattern in range_patterns:
        times.extend(re.findall(pattern, normalized))

    # Heures isolées : minutes obligatoires, ou contexte explicite d'heure.
    times.extend(re.findall(r"\b(?:[01]?\d|2[0-3]):[0-5]\d\b", normalized))
    for match in re.finditer(r"\b(?:heure|a|à)\s*[:\-]?\s*((?:[01]?\d|2[0-3])h(?:[0-5]\d)?)\b", normalized):
        times.append(match.group(1))

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
    """Extrait seulement les personnes introduites par un rôle académique clair."""
    found = []
    role_pattern = (
        r"(?i:responsable(?:\s+de\s+la\s+fili[eè]re)?|coordonnateur|coordonnatrice|"
        r"coordinateur|coordinatrice)\s*[:\-]?\s*(?:Pr\.?\s*)?"
        r"([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-]+(?:\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-]+){1,4})"
    )
    for match in re.findall(role_pattern, text or ""):
        value = re.split(
            r"\b(?:Mail|Email|Adresse|T[eé]l|équivalent|equivalent)\b",
            match,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        value = clean_text(value).strip(" :-")
        if 5 <= len(value) <= 100:
            found.append(value)

    titled_pattern = (
        r"\b(?:Pr\.?|Prof\.?|Professeur|Dr\.?|Docteur)\s+"
        r"([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-]+(?:\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-]+){1,4})"
    )
    for match in re.findall(titled_pattern, text or ""):
        value = clean_text(match).strip(" :-")
        if 5 <= len(value) <= 100:
            found.append(value)

    # Cas OCR collé : Pr ELGHACHTOULISanae -> Sanae ELGHACHTOULI.
    for token in re.findall(r"\bPr\s*([A-ZÀ-Ý]{3,}[A-ZÀ-Ý][a-zà-ÿ]{2,})\b", text or ""):
        split = re.match(r"^([A-ZÀ-Ý]{3,})([A-ZÀ-Ý][a-zà-ÿ]{2,})$", token)
        if split:
            found.append(f"{split.group(2)} {split.group(1)}")

    return unique_keep_order(found, max_items=20)

def extract_student_candidates(text):
    """Les noms d'étudiants ne sont pas conservés dans le catalogue documentaire."""
    return []

def count_students_from_apogee(text, document_type):
    """Compte les lignes étudiantes à partir des numéros Apogée distincts."""
    if document_type != "student_list":
        return 0

    apogee_ids = []
    for line in (text or "").splitlines():
        # Numéro de ligne suivi d'un identifiant Apogée continu à 8 chiffres.
        match = re.match(r"^\s*\d{1,4}\s+(\d{8})(?:\s|$)", line)
        if match:
            apogee_ids.append(match.group(1))

    if not apogee_ids and contains_term(normalize_text(text), "apogee"):
        apogee_ids.extend(
            re.findall(r"(?<!\d)(?:17|18|19|20|21|22|23|24|25)\d{6}(?!\d)", text or "")
        )

    return len(unique_keep_order(apogee_ids, max_items=10000))

def extract_student_count(text, document_type):
    """Compte les étudiants via les numéros Apogée distincts, sans stocker leurs noms."""
    if document_type != "student_list":
        return 0

    normalized = normalize_text(text)
    if not contains_term(normalized, "apogee"):
        return 0

    apogee_ids = set()
    for line in (text or "").splitlines():
        # Format observé : numéro de ligne, puis numéro Apogée à 8 chiffres.
        match = re.match(r"^\s*\d{1,4}\s+(\d{8})\b", line)
        if match:
            apogee_ids.add(match.group(1))

    return len(apogee_ids)

def extract_modules(text):
    """Extrait les modules explicitement listés et rejette les phrases descriptives."""
    found = []
    rejected_terms = [
        "objectif", "competence", "debouche", "condition", "responsable",
        "email", "adresse", "cout", "facilite de paiement", "etre titulaire",
        "ingenierie informatique et intelligence artificielle, capables de",
    ]

    # Modules codés M1, M 2, etc., y compris plusieurs modules sur une même ligne.
    for line in (text or "").splitlines():
        # Corrige les codes OCR espacés : M 1 0 : -> M10:.
        normalized_line = re.sub(
            r"\bM\s+(\d)\s+(\d)\s*:",
            lambda match: f"M{match.group(1)}{match.group(2)}:",
            line,
            flags=re.IGNORECASE,
        )
        matches = list(re.finditer(r"\bM\s*(\d{1,2})\s*:\s*", normalized_line, flags=re.IGNORECASE))
        for index, match in enumerate(matches):
            start = match.end()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(normalized_line)
            label = clean_text(normalized_line[start:end]).strip(" -:;|•")
            label = re.sub(r"\s+SEMESTRE\s+(?:[1-6]|I{1,3}|IV|V|VI)\s*$", "", label, flags=re.IGNORECASE)
            nlabel = normalize_text(label)
            if 3 <= len(label) <= 110 and not any(term in nlabel for term in rejected_terms):
                found.append(label)

    # Listes à puces ou tirets utilisées dans les programmes et emplois du temps.
    for line in (text or "").splitlines():
        match = re.match(r"^\s*[•\-–—]\s*(.+)$", line)
        if not match:
            continue
        label = clean_text(match.group(1)).strip(" -:;|•")
        nlabel = normalize_text(label)
        if not label or any(term in nlabel for term in rejected_terms):
            continue
        if 3 <= len(label) <= 110 and len(label.split()) <= 14:
            found.append(label)

    return unique_keep_order(found, max_items=40)

def extract_required_documents(text):
    content = normalize_text(text)
    found = []

    for keyword in REQUIRED_DOCUMENT_KEYWORDS:
        if contains_term(content, keyword):
            found.append(keyword)

    return unique_keep_order(found, max_items=30)


def extract_conditions(text):
    """Extrait uniquement les vraies conditions d'admission ou d'éligibilité."""
    lines = [clean_text(line) for line in (text or "").splitlines()]
    found = []
    capture_next = False

    for index, line in enumerate(lines):
        nline = normalize_text(line)
        if not line:
            continue

        if "condition" in nline and "admission" in nline:
            capture_next = True
            # Conserve le texte après le titre lorsqu'il existe réellement.
            candidate = re.sub(r"^.*?conditions?\s+d['’ ]?admission\s*", "", line, flags=re.IGNORECASE).strip(" :-")
            candidate = re.split(r"\b(?:INFORMATIONS?|RESPONSABLE|EMAIL|ADRESSE)\b", candidate, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if len(candidate) >= 12:
                found.append(candidate)
            continue

        if capture_next or re.match(r"^(être|etre)\s+titulaire\b", line, flags=re.IGNORECASE):
            candidate = re.split(r"\b(?:Responsable|Email|Mail|Adresse|Téléphone|Telephone)\b", line, maxsplit=1, flags=re.IGNORECASE)[0]
            candidate = clean_text(candidate).strip(" :-")

            # Les colonnes PDF peuvent placer "équivalent" au début de la ligne suivante.
            if candidate.lower().endswith(" ou") and index + 1 < len(lines):
                next_line = lines[index + 1]
                if normalize_text(next_line).startswith("equivalent"):
                    candidate = f"{candidate} équivalent"

            if 12 <= len(candidate) <= 220:
                found.append(candidate)
            capture_next = False

    return unique_keep_order(found, max_items=5)

def extract_keywords(text):
    content = normalize_text(text)
    found = []

    for keyword in ACADEMIC_KEYWORDS + REQUIRED_DOCUMENT_KEYWORDS + MODULE_HINTS:
        if contains_term(content, keyword):
            found.append(keyword)

    return unique_keep_order(found, max_items=40)


def compute_quality_score(
    cleaned_text,
    raw_text,
    extracted_pages,
    extraction_status,
    quality_metrics=None,
):
    if extraction_status in {"failed", "empty_text"}:
        return 0.0

    quality_metrics = quality_metrics or compute_text_quality_metrics(
        raw_text, cleaned_text, extracted_pages
    )

    density = quality_metrics["density_per_extracted_page"]
    density_score = min(density / 500.0, 1.0)
    semantic_signal = 0.0

    document_type = guess_document_type("", cleaned_text)
    if document_type != "other":
        semantic_signal += 0.4
    if extract_dates(cleaned_text) or extract_semester(cleaned_text):
        semantic_signal += 0.2
    if extract_emails(cleaned_text) or extract_phones(cleaned_text) or extract_links(cleaned_text):
        semantic_signal += 0.2
    if extract_professors(cleaned_text) or extract_program_name(cleaned_text):
        semantic_signal += 0.2

    score = (
        0.60 * quality_metrics["cleaning_quality_score"]
        + 0.30 * density_score
        + 0.10 * min(semantic_signal, 1.0)
    )

    if extraction_status == "partial_success":
        score *= 0.85

    return round(max(0.0, min(score, 1.0)), 2)

def detect_needs_ocr(
    cleaned_text,
    raw_text,
    extracted_pages,
    extraction_status,
    quality_metrics=None,
):
    if extraction_status in {"failed", "empty_text"}:
        return True

    quality_metrics = quality_metrics or compute_text_quality_metrics(
        raw_text, cleaned_text, extracted_pages
    )

    if not cleaned_text.strip():
        return True
    if quality_metrics["density_per_extracted_page"] < 40:
        return True
    if quality_metrics["cleaning_quality_score"] < 0.45:
        return True
    if quality_metrics["alnum_ratio"] < 0.45:
        return True
    if quality_metrics["gid_count"] >= 10:
        return True
    if quality_metrics["replacement_char_count"] >= 5:
        return True
    if quality_metrics["suspicious_long_token_count"] >= 2:
        return True

    return False

def extract_important_entities(
    text, raw_text, path, document_type, extraction_status, extracted_pages
):
    program_name = extract_program_name(text)
    title = extract_title(text, document_type=document_type)
    if document_type == "academic_program" and program_name:
        normalized_title = normalize_text(title)
        if not title or normalized_title.startswith("programme de la formation"):
            title = program_name

    academic_year = extract_academic_year(text)
    cycle = extract_cycle(text)
    semester = extract_semester(text)
    department = extract_department(text)
    document_category = guess_document_category(text, document_type)

    professors = extract_professors(text)
    student_count = extract_student_count(text, document_type)
    modules = extract_modules(text)
    rooms = extract_rooms(text)
    days = extract_days(text)
    times = extract_times(text)
    dates = extract_dates(text)
    emails = extract_emails(text)
    phones = extract_phones(text)
    links = extract_links(text)
    conditions = extract_conditions(text)
    deadline = extract_deadline(text)

    quality_metrics = compute_text_quality_metrics(raw_text, text, extracted_pages)
    needs_ocr = detect_needs_ocr(
        text, raw_text, extracted_pages, extraction_status, quality_metrics
    )
    quality_score = compute_quality_score(
        text, raw_text, extracted_pages, extraction_status, quality_metrics
    )

    return {
        "title_detected": title,
        "academic_year": academic_year,
        "cycle": cycle,
        "program_name": program_name,
        "semester": semester,
        "department": department,
        "document_category": document_category,
        "professors_detected": professors,
        "student_count": student_count,
        "modules_detected": modules,
        "rooms_detected": rooms,
        "days_detected": days,
        "times_detected": times,
        "dates_detected": dates,
        "deadline_detected": deadline,
        "emails_detected": emails,
        "phones_detected": phones,
        "links_detected": links,
        "conditions_detected": conditions,
        "needs_ocr": needs_ocr,
        "extraction_quality_score": quality_score,
    }

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
        "is_partial": False,
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
        "student_count_is_partial": False,
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
        "needs_ocr": True,
        "extraction_quality_score": 0.0,
        "important_entities_json": "{}"
    }

    try:
        reader = PdfReader(BytesIO(content))
        page_count = len(reader.pages)
        extracted_pages = min(page_count, MAX_PAGES_PER_PDF)

        texts = []
        page_errors = []

        for i in range(extracted_pages):
            try:
                page = reader.pages[i]
                try:
                    page_text = page.extract_text(extraction_mode="layout") or ""
                except (TypeError, ValueError):
                    page_text = page.extract_text() or ""
                texts.append(page_text)
            except Exception as page_error:
                page_errors.append(f"page {i + 1}: {str(page_error)[:160]}")

        raw_text = "\n".join(texts)
        final_text = clean_text(raw_text)[:MAX_TEXT_CHARS]
        is_partial = extracted_pages < page_count or bool(page_errors)

        if final_text and is_partial:
            extraction_status = "partial_success"
        elif final_text:
            extraction_status = "success"
        elif page_errors:
            extraction_status = "failed"
        else:
            extraction_status = "empty_text"

        document_type = guess_document_type(path, final_text)

        entities = extract_important_entities(
            text=final_text,
            raw_text=raw_text,
            path=path,
            document_type=document_type,
            extraction_status=extraction_status,
            extracted_pages=extracted_pages,
        )

        record["page_count"] = int(page_count)
        record["extracted_pages"] = int(extracted_pages)
        record["is_partial"] = bool(is_partial)
        record["text_length"] = int(len(final_text))
        record["document_type"] = document_type
        record["content_text"] = final_text
        record["extraction_status"] = extraction_status
        record["error_message"] = "; ".join(page_errors)[:500]

        record["title_detected"] = entities["title_detected"]
        record["academic_year"] = entities["academic_year"]
        record["cycle"] = entities["cycle"]
        record["program_name"] = entities["program_name"]
        record["semester"] = entities["semester"]
        record["department"] = entities["department"]
        record["document_category"] = entities["document_category"]
        record["professors_detected"] = join_values(entities["professors_detected"], max_items=20)
        record["student_count"] = int(entities["student_count"])
        record["student_count_is_partial"] = bool(
            is_partial and entities["student_count"] > 0
        )
        record["modules_detected"] = join_values(entities["modules_detected"], max_items=30)
        record["rooms_detected"] = join_values(entities["rooms_detected"], max_items=30)
        record["days_detected"] = join_values(entities["days_detected"], max_items=7)
        record["times_detected"] = join_values(entities["times_detected"], max_items=30)
        record["dates_detected"] = join_values(entities["dates_detected"], max_items=30)
        record["deadline_detected"] = entities["deadline_detected"]
        record["emails_detected"] = join_values(entities["emails_detected"], max_items=20)
        record["phones_detected"] = join_values(entities["phones_detected"], max_items=20)
        record["links_detected"] = join_values(entities["links_detected"], max_items=20)
        record["conditions_detected"] = join_values(entities["conditions_detected"], max_items=8)
        record["needs_ocr"] = bool(entities["needs_ocr"])
        record["extraction_quality_score"] = float(entities["extraction_quality_score"])

        # Colonnes historiques conservées vides pour compatibilité avec la table Hudi.
        record["students_detected"] = ""
        record["required_documents"] = ""
        record["keywords_detected"] = ""
        record["important_entities_json"] = "{}"

    except Exception as e:
        record["error_message"] = str(e)[:500]
        record["needs_ocr"] = True
        record["extraction_quality_score"] = 0.0

    return record


# ============================================================
# 7. Schema physique reduit de documents_catalog
# ============================================================

documents_schema = StructType([
    StructField("record_id", StringType()),
    StructField("source_path", StringType()),
    StructField("file_name", StringType()),
    StructField("document_type", StringType()),
    StructField("program_name", StringType()),
    StructField("academic_year", StringType()),
    StructField("semester", StringType()),
    StructField("student_count", IntegerType()),
    StructField("is_partial", BooleanType()),
    StructField("needs_ocr", BooleanType()),
    StructField("extraction_status", StringType()),
    StructField("extraction_quality_score", DoubleType()),
    StructField("crawl_timestamp", StringType()),
])


# ============================================================
# 8. Lecture incrementale des PDF depuis raw-documents
# ============================================================

print("Lecture incrementale des PDF depuis raw-documents...")

max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024


def get_existing_source_paths():
    """Retourne les PDF deja presents dans le catalogue Hudi."""
    try:
        if not spark.catalog.tableExists("default.documents_catalog"):
            return set()

        return {
            row["source_path"]
            for row in (
                spark.table("default.documents_catalog")
                .select("source_path")
                .where("source_path IS NOT NULL AND trim(source_path) <> ''")
                .distinct()
                .collect()
            )
        }
    except Exception as read_error:
        print(
            "Avertissement : lecture du catalogue existant impossible "
            f"({read_error}). Aucun chemin ne sera exclu."
        )
        return set()


def list_new_pdf_paths_from_minio(
    max_files,
    max_file_size_bytes,
    existing_source_paths,
):
    """
    Liste seulement les PDF non encore catalogues.

    Les fichiers les plus anciens sont traites en premier afin que plusieurs
    executions successives du DAG vident progressivement tout le backlog.
    """
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    candidates = []
    continuation_token = None

    while True:
        request = {"Bucket": "raw-documents", "MaxKeys": 1000}
        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = client.list_objects_v2(**request)

        for item in response.get("Contents", []):
            key = item.get("Key", "")
            size = int(item.get("Size", 0))
            source_path = f"s3a://raw-documents/{key}"

            if (
                key.lower().endswith(".pdf")
                and size <= max_file_size_bytes
                and source_path not in existing_source_paths
            ):
                candidates.append(
                    (
                        item.get("LastModified"),
                        key.lower(),
                        source_path,
                    )
                )

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    candidates.sort(key=lambda item: (item[0], item[1]))
    return [source_path for _, _, source_path in candidates[:max_files]]


existing_source_paths = get_existing_source_paths()

try:
    selected_pdf_paths = list_new_pdf_paths_from_minio(
        MAX_FILES,
        max_bytes,
        existing_source_paths,
    )
except Exception as listing_error:
    print(
        f"Avertissement : listing MinIO indisponible ({listing_error}). "
        "Fallback Spark."
    )
    selected_pdf_paths = []

if selected_pdf_paths:
    pdf_df = (
        spark.read.format("binaryFile")
        .load(selected_pdf_paths)
        .select("path", "length", "content")
    )
else:
    # Fallback incremental si l'API S3 est indisponible.
    pdf_df = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.pdf")
        .load(RAW_DOCS_PATH)
        .where(f"length <= {max_bytes}")
    )

    if existing_source_paths:
        pdf_df = pdf_df.filter(
            ~pdf_df["path"].isin(sorted(existing_source_paths))
        )

    pdf_df = (
        pdf_df
        .orderBy("modificationTime", "path")
        .limit(MAX_FILES)
        .select("path", "length", "content")
    )

pdf_rows = pdf_df.collect()

print(f"PDF deja catalogues : {len(existing_source_paths)}")
print(f"Nouveaux PDF selectionnes : {len(pdf_rows)}")
print(
    f"Limites : "
    f"MAX_FILES={MAX_FILES}, "
    f"MAX_FILE_SIZE_MB={MAX_FILE_SIZE_MB}, "
    f"MAX_PAGES_PER_PDF={MAX_PAGES_PER_PDF}, "
    f"MAX_TEXT_CHARS={MAX_TEXT_CHARS}"
)

if not pdf_rows:
    print("Aucun nouveau PDF a traiter. Catalogue deja a jour.")
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

print("Apercu du catalogue PDF reduit :")
df_documents.select(
    "file_name",
    "document_type",
    "program_name",
    "academic_year",
    "semester",
    "student_count",
    "is_partial",
    "needs_ocr",
    "extraction_status",
    "extraction_quality_score",
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
        "hoodie.datasource.write.reconcile.schema": "true",
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

print("Transformation PDF reduite terminee. Table disponible dans Hive : default.documents_catalog")

spark.stop()