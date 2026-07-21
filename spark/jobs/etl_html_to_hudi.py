#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL universel HTML -> Hudi (faculty_profiles, programs) – v5 (corrigé)
========================================================================
- Correction d'encodage sur tout le contenu HTML avant parsing.
- Dédoublonnage des professeurs : clé normalisée (nom + université) pour
  éviter les doublons intra-page et inter-pages.
- Le reste (programmes, filtres, options) inchangé.
"""

import re
import html
import hashlib
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from bs4 import BeautifulSoup, Comment

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StringType, ArrayType, StructType, StructField, BooleanType
)

try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False

# ============================================================
# 1. Spark session
# ============================================================
spark = (
    SparkSession.builder
    .appName("UniversalHTMLToHudi_v5_fixed")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.cores.max", "2")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.files.maxPartitionBytes", str(8 * 1024 * 1024))
    .config("spark.python.worker.memory", "256m")
    .config("spark.rdd.compress", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.memory.fraction", "0.6")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.network.timeout", "300s")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

print("💾 Config mémoire :",
      "shuffle.partitions=", spark.conf.get("spark.sql.shuffle.partitions"),
      "| default.parallelism=", sc.defaultParallelism,
      "| cores.max=", spark.conf.get("spark.cores.max", "non défini"))

# ============================================================
# 2. Constantes MinIO
# ============================================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
RAW_BUCKET = "raw-web-html"
CURATED_PATH_PREFIX = "s3a://curated-zone"

# ============================================================
# 3. Nettoyage HTML et Encodage
# ============================================================
def clean_html_noise(raw_html):
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    for tag in soup.find_all(['script', 'style', 'svg', 'nav', 'footer', 'header',
                              'aside', 'template', 'noscript', 'iframe', 'form',
                              'button', 'input', 'select', 'textarea']):
        tag.decompose()
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    noise_classes = re.compile(r'nav|menu|header|footer|sidebar|cookie|popup|modal|banner|advert', re.I)
    for div in soup.find_all(['div', 'section'], class_=noise_classes):
        div.decompose()
    return str(soup)

def fix_text_encoding(text):
    """Répare les problèmes d'encodage (Mojibake) et les entités HTML."""
    if not text:
        return text
    text = html.unescape(text)
    try:
        if 'Ã' in text or 'â' in text or 'Â' in text:
            text = text.encode('latin1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return text.strip()

# ============================================================
# 4. Fonctions d'extraction (strictes et universelles)
# ============================================================

# ———————————— Faculty ————————————
def extract_faculty_from_html(html_content, source_url, domain):
    if not html_content:
        return []
    html_content = clean_html_noise(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')
    faculty_list = []
    seen_names = set()  # normalisé en minuscules pour éviter les doublons

    university = domain

    FORBIDDEN_WORDS = {
        'filière', 'licence', 'master', 'doctorat', 'biotechnologie', 'laboratoire',
        'bibliothèque', 'portail', 'membres', 'directeur', 'adjoint', 'statut',
        'syst', 'geomatics', 'applied', 'équipe', 'recherche', 'formation',
        'programme', 'campus', 'événements', 'qualité', 'full stack', 'offre',
        'ufr', 'lyon', 'fsbm', 'fsac', 'univh2c', 'www', 'ma', 'com', 'org',
        'enseignant', 'professeur', 'docteur', 'pr', 'prof', 'dr', 'mme', 'mlle',
        'responsable', 'coordonnateur', 'chef', 'département', 'department',
        'avis', 'concours', 'recrutement', 'poste', 'vacant', 'vice-doyen',
        'doyen', 'equipe', 'georesources', 'applied', 'campus', 'portail',
        'representant', 'conseil', 'commission', 'grade', 'email', 'contact'
    }

    def normalize_name(name):
        if not name:
            return None
        name = fix_text_encoding(name)
        name = re.sub(r'^(Pr\.|Prof\.|Professeur|Dr\.|M\.|Mme|Mlle)\s+', '', name, flags=re.I)
        name = re.sub(r'\s+', ' ', name).strip()
        if name.isupper():
            name = name.title()
        return name

    def is_valid_name(name):
        if not name or len(name) < 4 or len(name) > 60:
            return False
        words = name.split()
        if len(words) < 2:
            return False
        for w in words:
            if not re.match(r"^[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ\-']*$", w) and not w.isupper():
                return False
            if len(w) < 2:
                return False
        name_lower = name.lower()
        if any(forbidden in name_lower for forbidden in FORBIDDEN_WORDS):
            return False
        if re.search(r'[0-9@#$%^&*()_+=<>?/{}|]', name):
            return False
        return True

    def add_faculty(full_name, title="", department=""):
        full_name = normalize_name(full_name)
        if not full_name:
            return
        # Clé de dédoublonnage intra-page : nom normalisé en minuscules
        key = full_name.lower()
        if key in seen_names:
            return
        if not is_valid_name(full_name):
            return
        seen_names.add(key)
        record_id = hashlib.md5((full_name + university + source_url).encode()).hexdigest()
        faculty_list.append({
            "record_id": record_id,
            "full_name": full_name,
            "title": title,
            "department": department,
            "faculty": "",
            "university": university,
            "email": "",
            "research_areas": [],
            "source_system": "web_scraping",
            "source_url": source_url,
            "crawl_timestamp": datetime.now().isoformat(),
            "content_hash": hashlib.md5(html_content.encode()).hexdigest(),
            "is_deleted": False,
            "extraction_method": "html_parsing"
        })

    # --- Méthodes d'extraction (inchangées) ---

    # 1. Listes ordonnées
    for ol in soup.find_all('ol'):
        for li in ol.find_all('li'):
            text = li.get_text(strip=True)
            if re.match(r'^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ\s\-]+$', text) and len(text.split()) >= 2:
                add_faculty(text, "Professeur", "")

    # 2. Balises avec titre
    for elem in soup.find_all(['figcaption', 'span', 'div', 'p', 'td']):
        text = elem.get_text(strip=True)
        m = re.search(r'(?:Pr\.|Prof\.|Professeur|Dr\.|M\.|Mme|Mlle)\s+([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})', text)
        if m:
            title = "Enseignant"
            if re.search(r'Pr\.|Prof\.|Professeur', text):
                title = "Professeur"
            elif re.search(r'Dr\.', text):
                title = "Docteur"
            dept = ""
            dept_match = re.search(r'(?:département|department|chef)\s+de\s+([A-Za-zÀ-ÖØ-Þ\s]+?)(?:\.|,|$)', text, re.I)
            if dept_match:
                dept = dept_match.group(1).strip()
            add_faculty(m.group(1).strip(), title, dept)

    # 3. Tableaux (amélioré)
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue

        # Détection des en-têtes (première ligne)
        header_cells = rows[0].find_all(['th', 'td'])
        headers = [fix_text_encoding(c.get_text(strip=True)).lower() for c in header_cells]

        # Chercher les indices des colonnes utiles
        name_col = -1
        coord_col = -1
        email_col = -1
        for i, h in enumerate(headers):
            if any(k in h for k in ['nom', 'prenom', 'noms', 'prénoms', 'etudiant', 'membre']):
                name_col = i
            if any(k in h for k in ['coordon', 'responsable', 'chef', 'prof']):
                coord_col = i
            if 'email' in h:
                email_col = i

        # Si une colonne "nom" est trouvée, on l'utilise en priorité
        if name_col >= 0:
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) > name_col:
                    candidate = fix_text_encoding(cells[name_col].get_text(strip=True))
                    # Nettoyer les grades
                    candidate = re.sub(r'\b(PES|PH|PESA|MCF|Professeur|Maitre|Docteur|Pr\.|Prof\.|Dr\.)\b', '', candidate, flags=re.I)
                    candidate = re.sub(r'\s+', ' ', candidate).strip()
                    if candidate and len(candidate.split()) >= 2:
                        add_faculty(candidate, "Enseignant", "")
        else:
            # Fallback : si pas de colonne nom, on scanne toutes les cellules de chaque ligne
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                for cell in cells:
                    candidate = fix_text_encoding(cell.get_text(strip=True))
                    # On ignore les cellules qui contiennent des emails ou des mots-clés interdits
                    if '@' in candidate or len(candidate) < 4:
                        continue
                    # Nettoyer les grades
                    candidate = re.sub(r'\b(PES|PH|PESA|MCF|Professeur|Maitre|Docteur|Pr\.|Prof\.|Dr\.)\b', '', candidate, flags=re.I)
                    candidate = re.sub(r'\s+', ' ', candidate).strip()
                    if candidate and len(candidate.split()) >= 2:
                        add_faculty(candidate, "Enseignant", "")

        # Si on a une colonne "coordonnateur", on extrait aussi les noms de cette colonne
        if coord_col >= 0 and coord_col != name_col:
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) > coord_col:
                    coord_text = fix_text_encoding(cells[coord_col].get_text(strip=True))
                    m = re.search(r'(?:Pr\.|Prof\.|Dr\.|M\.|Mme)\s+([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})', coord_text)
                    if m:
                        add_faculty(m.group(1).strip(), "Professeur", "")

    # 4. Fallback regex (si moins de 5 résultats)
    if len(faculty_list) < 5:
        text = soup.get_text(' ', strip=True)
        pattern = r'(?:Pr\.|Prof\.|Professeur|Dr\.|M\.|Mme|Mlle)\s+([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})'
        for match in re.finditer(pattern, text):
            candidate = match.group(1).strip()
            if is_valid_name(candidate):
                add_faculty(candidate, "Enseignant", "")

    return faculty_list

# ———————————— Programs (inchangé) ————————————
def extract_programs_from_html(html_content, source_url, domain):
    if not html_content:
        return []
    html_content = clean_html_noise(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')
    programs = []
    seen_programs = set()

    university = domain

    FORBIDDEN_PROGRAM_WORDS = {
        'avis', 'concours', 'recrutement', 'poste', 'vacant', 'vice-doyen', 'doyen',
        'laboratoire', 'équipe', 'equipe', 'responsable', 'professeur', 'assistant',
        'chercheur', 'examen', 'inscription', 'fiche', 'session', 'semestre', 'liste',
        'convoqués', 'entretien', 'oral', 'spécialité', 'télécharger', 'plus de détails',
        'avertissement', 'information', 'actualité', 'carrière', 'emploi', 'stage',
        'contrat', 'offre', 'partenariat', 'cbx group', 'leader', 'devops', 'big data',
        'kubernetes', 'certification', 'mastere', 'contact', 'mail', 'phone', 'adresse',
        'participation', 'développement', 'biography', 'événements', 'events', 
        'mot du doyen', 'gouvernance', 'organigramme', 'mot du directeur'
    }

    LEVEL_KEYWORDS = {
        'doctorat': 'Doctorat',
        'phd': 'Doctorat',
        'master': 'Master',
        'mastère': 'Master',
        'licence professionnelle': 'Licence Professionnelle',
        'licence pro': 'Licence Professionnelle',
        'lpu': 'Licence Professionnelle',
        'licence': 'Licence',
        'license': 'Licence',
        'filière': 'Filière',
        'parcours': 'Filière'
    }

    ACADEMIC_KEYWORDS = [
        'sciences', 'mathématiques', 'math', 'physique', 'chimie', 'biologie',
        'génie', 'ingénierie', 'informatique', 'management', 'gestion', 'économie',
        'droit', 'langues', 'lettres', 'histoire', 'géographie', 'géologie',
        'environnement', 'agroalimentaire', 'industrie', 'électronique', 'automatique',
        'robotique', 'mécanique', 'thermique', 'énergétique', 'pétrole', 'géophysique',
        'géomatique', 'systèmes', 'réseaux', 'télécommunications', 'signal', 'imagerie',
        'matériaux', 'procédés', 'analyse', 'biotechnologie', 'agronomie',
        'biodiversité', 'écologie', 'biomédical', 'pharmacie', 'optique', 'photonique',
        'radioprotection', 'dosimétrie', 'cybersécurité', 'cryptographie', 'data'
    ]

    def get_level(text):
        text_lower = text.lower()
        for key, level in LEVEL_KEYWORDS.items():
            if key in text_lower:
                return level
        return "Formation"

    def normalize_program_name(name):
        if not name:
            return None
        name = fix_text_encoding(name)
        name = re.sub(r'\s+', ' ', name).strip()
        name = re.sub(r'^[\s🔹🔸📚🎓👤]+', '', name)
        name = re.sub(r'^(Filière|Formation|Programme|Diplôme|Master|Licence|Parcours|Cycle)\s+', '', name, flags=re.I)
        return name

    def is_valid_program(name):
        if not name or len(name) < 5 or len(name) > 100:
            return False
            
        words = name.split()
        if len(words) > 12:
            return False

        if name.endswith('.') or name.endswith('!') or name.endswith('?'):
            return False

        name_lower = name.lower()
        if any(forbidden in name_lower for forbidden in FORBIDDEN_PROGRAM_WORDS):
            return False
            
        if re.search(r'@|\.com|\.ma|\.fr|http|www|\b20\d{2}\b', name_lower):
            return False
            
        if re.match(r'^(offre|liste|contact|accueil|présentation|menu|short biography)$', name_lower):
            return False

        if re.search(r'\b(Pr|Prof|Dr|M|Mme|Mlle)\b\.?', name, re.I):
            return False

        if re.match(r'^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ\s\-]+$', name) and len(words) >= 2:
            if not any(kw in name_lower for kw in ACADEMIC_KEYWORDS):
                return False
                
        return True

    def add_program(name, level, responsible="", department=""):
        name = normalize_program_name(name)
        if not name or name in seen_programs:
            return
        if level == "Formation":
            name_lower = name.lower()
            if not any(kw in name_lower for kw in ACADEMIC_KEYWORDS):
                return
        if not is_valid_program(name):
            return
        seen_programs.add(name)
        program_id = hashlib.md5((name + university).encode()).hexdigest()
        programs.append({
            "program_id": program_id,
            "program_name": name,
            "level": level,
            "department": department,
            "faculty": university,
            "responsible": responsible,
            "source_url": source_url,
            "crawl_timestamp": datetime.now().isoformat()
        })

    # Extracteurs (inchangés)
    # 1. Headings
    for heading in soup.find_all(['h2', 'h3', 'h4', 'h5']):
        text = heading.get_text(strip=True)
        level = get_level(text)
        if level != "Formation":
            clean_name = re.sub(r'^(Licences?|Masters?|Doctorats?|Filières?|Formations?|Parcours?)\s*', '', text, flags=re.I).strip()
            if clean_name and len(clean_name) > 5:
                add_program(clean_name, level)
        else:
            parent_section = heading.find_parent(['section', 'div', 'article'])
            if parent_section:
                parent_text = parent_section.get_text(strip=True)
                level_parent = get_level(parent_text)
                if level_parent != "Formation":
                    add_program(text, level_parent)

    # 2. Tableaux
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(['th', 'td'])]
        name_idx = -1
        level_idx = -1
        resp_idx = -1
        for i, h in enumerate(headers):
            if any(k in h for k in ['intitulé', 'formation', 'programme', 'diplôme', 'diplome', 'titre', 'nom']):
                name_idx = i
            if any(k in h for k in ['niveau', 'type', 'grade']):
                level_idx = i
            if any(k in h for k in ['responsable', 'coordonnateur', 'chef']):
                resp_idx = i
        if name_idx >= 0:
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) > name_idx:
                    name = cells[name_idx].get_text(strip=True)
                    if not name or len(name) < 4:
                        continue
                    level = "Formation"
                    if level_idx >= 0 and len(cells) > level_idx:
                        level_text = cells[level_idx].get_text(strip=True)
                        level = get_level(level_text)
                    responsible = ""
                    if resp_idx >= 0 and len(cells) > resp_idx:
                        resp_text = cells[resp_idx].get_text(strip=True)
                        m = re.search(r'(?:Pr\.|Prof\.|Dr\.|M\.|Mme)\s+([A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3})', resp_text)
                        if m:
                            responsible = m.group(1).strip()
                    if level == "Formation" and re.match(r'^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ\s\-]+$', name) and len(name.split()) >= 2:
                        if not any(kw in name.lower() for kw in ACADEMIC_KEYWORDS):
                            continue
                    add_program(name, level, responsible)

    # 3. Liens PDF
    for a in soup.find_all('a', href=re.compile(r'\.pdf', re.I)):
        href = a.get('href', '').lower()
        if 'flyer' in href or 'brochure' in href or 'plaquette' in href:
            name = a.get_text(strip=True)
            if name and len(name) > 5:
                parent = a.find_parent(['details', 'section', 'div', 'li'])
                level = "Formation"
                if parent:
                    parent_text = parent.get_text(strip=True)
                    level = get_level(parent_text)
                add_program(name, level)

    # 4. Listes <ul>/<ol>
    for ul in soup.find_all(['ul', 'ol']):
        for li in ul.find_all('li'):
            a = li.find('a')
            if a:
                href = a.get('href', '')
                if any(k in href for k in ['formation', 'programme', 'filiere', 'licence', 'master']):
                    name = a.get_text(strip=True)
                    if name and len(name) > 5:
                        level = get_level(li.get_text(strip=True))
                        add_program(name, level)
            else:
                text = li.get_text(strip=True)
                if text and len(text) > 5 and not re.search(r'(Pr\.|Prof\.|Dr\.)', text):
                    if not re.match(r'^[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+(?:\s+[A-ZÀ-ÖØ-Þ][a-zà-öø-ÿ]+){1,3}$', text):
                        level = get_level(text)
                        add_program(text, level)

    # 5. Div/Article avec classes
    for card in soup.find_all(['div', 'article', 'section'], class_=re.compile(r'formation|program|diplome|licence|master', re.I)):
        title_elem = card.find(['h2', 'h3', 'h4', 'strong', 'b'])
        if title_elem:
            name = title_elem.get_text(strip=True)
            if name and len(name) > 5:
                level = get_level(card.get_text(strip=True))
                add_program(name, level)
        else:
            text_elem = card.find(['p', 'span'])
            if text_elem:
                name = text_elem.get_text(strip=True)
                if name and len(name) > 5:
                    level = get_level(card.get_text(strip=True))
                    add_program(name, level)

    # 6. Listes de définitions <dl>
    for dl in soup.find_all('dl'):
        dt = dl.find('dt')
        while dt:
            name = dt.get_text(strip=True)
            dd = dt.find_next_sibling('dd')
            if dd:
                level = get_level(name + ' ' + dd.get_text(strip=True))
                responsible = ""
                dept = ""
                add_program(name, level, responsible, dept)
            dt = dd.find_next_sibling('dt') if dd else None

    return programs

# ============================================================
# 5. Extraction combinée pour un fichier
# ============================================================
def extract_records_for_file(key, content, domain):
    # Réparation de l'encodage sur l'intégralité du contenu HTML
    content = fix_text_encoding(content)
    source_url = f"s3a://{RAW_BUCKET}/{key}"
    faculty = extract_faculty_from_html(content, source_url, domain)
    programs = extract_programs_from_html(content, source_url, domain)
    now = datetime.now(timezone.utc).isoformat()
    for f in faculty:
        f["crawl_timestamp"] = now
        f["content_hash"] = hashlib.md5(content.encode()).hexdigest()
    for p in programs:
        p["crawl_timestamp"] = now
    return faculty, programs

# ============================================================
# 6. Filtrage des fichiers par mots-clés (élargi)
# ============================================================
FACULTY_KEYWORDS = [
    'department', 'departement', 'dept', 'departements',
    'faculty', 'professor', 'professeur', 'professors', 'professeurs',
    'personnel', 'enseignant', 'enseignants', 'staff', 'teacher',
    'research', 'recherche', 'laboratory', 'laboratoire', 'labo',
    'equipe', 'team', 'chercheur', 'researcher',
    'membre', 'membres', 'conseil', 'representant', 'representants',
    'doyen', 'vice-doyen', 'chef', 'departement', 'commission',
    'grade', 'biologie', 'chimie', 'physique', 'mathematiques', 'informatique',
    'geologie', 'electronique', 'mecanique'
]

PROGRAM_KEYWORDS = [
    'formation', 'formations', 'program', 'programs', 'programme', 'programmes',
    'filiere', 'filieres', 'filière', 'filières',
    'licence', 'licences', 'master', 'masters', 'doctorat', 'doctorats',
    'diplome', 'diplomes', 'diplôme', 'diplômes',
    'course', 'courses', 'curriculum', 'enseignement', 'enseignements',
    'parcours', 'specialite', 'spécialité', 'specialites',
    'offre', 'offres', 'education', 'pedagogie', 'pédagogie',
    'formation-licence', 'formation-master', 'formation-doctorat'
]

ALL_RELEVANT_KEYWORDS = FACULTY_KEYWORDS + PROGRAM_KEYWORDS

def list_relevant_html_keys():
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY,
                      use_ssl=False, config=Config(signature_version='s3v4'))
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=RAW_BUCKET, Prefix='')
    relevant = []
    irrelevant = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.html') or re.search(r'%20|\s|:', key):
                continue
            filename = key.split('/')[-1].lower().replace('.html', '')
            if any(kw.lower() in filename for kw in ALL_RELEVANT_KEYWORDS):
                relevant.append(key)
            else:
                irrelevant.append(key)

    print("📊 STATISTIQUES:")
    print(f"   Fichiers pertinents trouvés: {len(relevant)}")
    print(f"   Fichiers ignorés (bruit): {len(irrelevant)}")
    if relevant + irrelevant:
        print(f"   Taux de réduction: {len(irrelevant)/(len(relevant)+len(irrelevant))*100:.1f}%")
    if irrelevant:
        print("\n   Exemples de fichiers ignorés:")
        for k in irrelevant[:5]:
            print(f"      ❌ {k}")
    return relevant

# ============================================================
# 7. Lecture et extraction en mapPartitions
# ============================================================
def process_partition(keys_iter):
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY,
                      use_ssl=False, config=Config(signature_version='s3v4'))
    for key in keys_iter:
        try:
            resp = s3.get_object(Bucket=RAW_BUCKET, Key=key)
            raw = resp['Body'].read()
            if HAS_CHARDET:
                enc = chardet.detect(raw)['encoding'] or 'utf-8'
            else:
                enc = 'utf-8'
            content = raw.decode(enc, errors='replace')
            del raw
            domain = key.split('/')[0] if '/' in key else key
            faculty, programs = extract_records_for_file(key, content, domain)
            del content
            for f in faculty:
                yield ('faculty', f['record_id'], f)
            for p in programs:
                yield ('program', p['program_id'], p)
        except Exception as e:
            print(f"⚠️ Erreur traitement {key}: {e}")
            continue

# ============================================================
# 8. Dédoublonnage
# ============================================================
def better_record(a, b):
    if a["crawl_timestamp"] >= b["crawl_timestamp"]:
        return a
    return b

# ============================================================
# 9. Schémas
# ============================================================
faculty_schema = StructType([
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
    StructField("is_deleted", BooleanType()),
    StructField("extraction_method", StringType()),
])

program_schema = StructType([
    StructField("program_id", StringType()),
    StructField("program_name", StringType()),
    StructField("level", StringType()),
    StructField("department", StringType()),
    StructField("faculty", StringType()),
    StructField("responsible", StringType()),
    StructField("source_url", StringType()),
    StructField("crawl_timestamp", StringType()),
])

# ============================================================
# 10. Programme principal
# ============================================================
print("📖 Récupération et filtrage intelligent des fichiers HTML...")
valid_keys = list_relevant_html_keys()
print(f"\n🎯 {len(valid_keys)} fichiers HTML pertinents sélectionnés.")
if not valid_keys:
    print("Aucun fichier valide. Arrêt.")
    spark.stop()
    raise SystemExit(0)

n_partitions = max(1, min(len(valid_keys) // 20 + 1, sc.defaultParallelism * 2))
keys_rdd = sc.parallelize(valid_keys, n_partitions)
print(f"🔎 Extraction sur {n_partitions} partitions...")
records_rdd = keys_rdd.mapPartitions(process_partition)
records_rdd.persist()

# Dédoublonnage FACULTY avec clé normalisée (nom en minuscules + université)
faculty_rdd = (records_rdd.filter(lambda t: t[0] == 'faculty')
               .map(lambda t: ((t[2]['full_name'].lower(), t[2]['university']), t[2]))
               .reduceByKey(better_record)
               .map(lambda kv: kv[1]))

# Dédoublonnage PROGRAMS (inchangé, avec record_id)
program_rdd = (records_rdd.filter(lambda t: t[0] == 'program')
               .map(lambda t: (t[1], t[2]))
               .reduceByKey(better_record)
               .map(lambda kv: kv[1]))

faculty_cols = [f.name for f in faculty_schema.fields]
program_cols = [f.name for f in program_schema.fields]

df_faculty = spark.createDataFrame(
    faculty_rdd.map(lambda r: Row(**{c: r.get(c) for c in faculty_cols})),
    schema=faculty_schema
)
df_programs = spark.createDataFrame(
    program_rdd.map(lambda r: Row(**{c: r.get(c) for c in program_cols})),
    schema=program_schema
)

# ============================================================
# 11. Écriture Hudi
# ============================================================
def write_hudi_with_sync(df, table_name, key_field):
    df.persist()
    count = df.count()
    if count == 0:
        print(f"⚠️ Aucune donnée pour {table_name}, écriture ignorée.")
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
        "hoodie.memory.merge.max.size": "104857600",
        "hoodie.memory.compaction.max.size": "104857600",
        "hoodie.parquet.max.file.size": "67108864",
        "hoodie.parquet.small.file.limit": "20971520",
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
    print(f"✅ Table {table_name} écrite avec {count} lignes et synchronisée dans Hive.")
    df.unpersist()

print("💾 Écriture des tables Hudi avec sync Hive (mode HMS)...")
write_hudi_with_sync(df_faculty, "faculty_profiles", "record_id")
write_hudi_with_sync(df_programs, "programs", "program_id")

records_rdd.unpersist()

print("🎉 Transformation HTML terminée. Tables disponibles dans Hive (base default).")
spark.stop()