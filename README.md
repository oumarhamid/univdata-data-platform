# UnivData — Plateforme de données universitaire

Pipeline complet de collecte, transformation et exposition des données publiques des universités marocaines : sites de facultés, publications scientifiques (OpenAlex, Crossref) et documents PDF, jusqu'à un dashboard BI et un moteur de recherche.

```
Sources (Web / API / PDF)
        │
        ▼
   MinIO (raw zone)
        │
        ▼
  Spark ETL (Hudi upserts)
        │
        ▼
 Apache Hudi (curated zone)
        │
        ▼
  Hive Metastore (catalogue SQL)
       ╱          ╲
      ▼             ▼
PostgreSQL      Elasticsearch
   │                 │
   ▼                 ▼
Metabase          Kibana

Orchestration : Apache Airflow (6 DAGs)
```

## Sommaire

- [Objectif](#objectif)
- [Architecture détaillée](#architecture-détaillée)
- [Stack technique](#stack-technique)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Services et ports](#services-et-ports)
- [Les 6 DAGs Airflow](#les-6-dags-airflow)
- [Lancer le pipeline complet](#lancer-le-pipeline-complet)
- [Tables curées (Hudi)](#tables-curées-hudi)
- [Vérifier chaque étape](#vérifier-chaque-étape)
- [Relancer sans dupliquer les données](#relancer-sans-dupliquer-les-données)
- [Dashboard Metabase](#dashboard-metabase)
- [Recherche Elasticsearch](#recherche-elasticsearch)
- [Runbook — problèmes connus](#runbook--problèmes-connus)
- [Démo](#démo)

## Objectif

Centraliser automatiquement des données universitaires dispersées sur des sites hétérogènes, sans format standard, pour permettre une recherche et une analyse croisées entre facultés.

**Sources couvertes :**
- Sites web : `fsac.univh2c.ma`, `www.fsbm.ma`, `www.fsr.ac.ma`, `www.fstm.ac.ma`
- API OpenAlex (publications scientifiques, max 5 000/run)
- API Crossref (métadonnées bibliographiques, max 2 000/run)
- Documents PDF collectés lors du scraping

**Volumes typiques observés :** ~718 profils enseignants, ~196 formations, ~2 134 publications, ~10 documents PDF par run complet.

## Architecture détaillée

### Ingestion (DAG `scraping_pipeline`, hebdomadaire)

- **BFS Scraper** (Playwright + requests) : profondeur 3, max 250 pages/domaine → écrit dans `raw-web-html` et `raw-documents`
- **openalex_ingest.py** : pagination par curseur → `raw-json/openalex/`
- **crossref_ingest.py** : pagination par curseur → `raw-json/crossref/`

### Raw zone (MinIO)

```
raw-web-html/{domain}/{YYYY}/{MM}/{DD}/depth={N}/{safe_filename}.html
raw-documents/{domain}/{YYYY}/{MM}/{DD}/files/{md5(url)}.pdf
raw-images/{domain}/{YYYY}/{MM}/{DD}/images/{md5(url)}.{ext}
raw-json/openalex/year=Y/month=M/day=D/works_page_NNNN.json
raw-json/crossref/year=Y/month=M/day=D/works_page_NNNN.json
raw-logs/errors/
```

### Transformation (Spark ETL → Hudi, mode COPY_ON_WRITE)

| DAG | Job Spark | Rôle |
|---|---|---|
| `html_to_hudi_pipeline` | `etl_html_to_hudi.py` | Extraction enseignants + formations, correction Mojibake, déduplication RDD |
| `api_to_hudi_pipeline` | `etl_apis_to_hudi.py` | Fusion OpenAlex + Crossref par DOI |
| `pdf_to_hudi_pipeline_optimized` | `etl_pdf_to_hudi.py` | Extraction pypdf, score qualité, classification type, détection `needs_ocr` |

### Serving

| DAG | Job | Rôle |
|---|---|---|
| `export_to_postgres_pipeline` | `export_to_postgres.py` | Export JDBC en mode OVERWRITE vers PostgreSQL (DB `metabase`) |
| `index_elasticsearch_pipeline` | `index_elasticsearch.py` | DELETE + réindexation complète de `university_docs` |

Metabase lit PostgreSQL ; Kibana/Elasticsearch exposent la recherche full-text.

> ⚠️ **Il n'y a pas de dépendances automatiques entre DAGs** — l'ordre d'exécution doit être respecté manuellement (voir [Lancer le pipeline complet](#lancer-le-pipeline-complet)).

## Stack technique

- **Orchestration** : Apache Airflow (LocalExecutor, socket Docker monté)
- **Stockage objet** : MinIO
- **Traitement** : Apache Spark (master + 1 worker, 2 cores / 3G RAM)
- **Lakehouse** : Apache Hudi 0.15.0 (bundle Spark 3, COPY_ON_WRITE)
- **Catalogue** : Hive Metastore (thrift://hive-metastore:9083)
- **Base de métadonnées** : PostgreSQL 13 (DB `airflow`, `hive_metastore`, `metabase`)
- **BI** : Metabase (14 questions SQL prédéfinies)
- **Recherche** : Elasticsearch 8.10.2 + Kibana

## Prérequis

- Docker et Docker Compose v2
- ~8-12 Go de RAM disponibles
- Ports libres : `3000, 5601, 7077, 8080, 8081, 8082, 9000, 9001, 9083, 9200, 9300`
- Python + `requests` sur la machine hôte (pour le script de création du dashboard Metabase)

## Installation

```bash
git clone <url-du-repo>
cd <nom-du-repo>

docker compose up -d --build
docker compose ps   # vérifier que tous les services sont "healthy"
```

Pour tout réinitialiser (⚠️ supprime les données) :

```bash
docker compose down -v && docker compose up -d
```

## Services et ports

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8082 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API (S3) | http://localhost:9000 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Hive Metastore | thrift://localhost:9083 | — |
| Metabase | http://localhost:3000 | admin@example.com / admin123@ |
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |

## Les 6 DAGs Airflow

1. `scraping_pipeline` — scraping web + ingestion APIs (@weekly)
2. `html_to_hudi_pipeline` — ETL HTML → Hudi
3. `api_to_hudi_pipeline` — ETL API → Hudi
4. `pdf_to_hudi_pipeline_optimized` — ETL PDF → Hudi
5. `export_to_postgres_pipeline` — export vers PostgreSQL
6. `index_elasticsearch_pipeline` — indexation Elasticsearch

## Lancer le pipeline complet

### Ordre obligatoire

```
1. scraping_pipeline
       ↓
2. html_to_hudi_pipeline  ┐
   api_to_hudi_pipeline   ├  (peuvent tourner en parallèle)
   pdf_to_hudi_pipeline   ┘
       ↓
3. export_to_postgres_pipeline
       ↓
4. index_elasticsearch_pipeline
       ↓
5. python create_metabase_dashboard.py   (manuel, sur l'hôte)
```

### Via l'UI Airflow

Aller sur http://localhost:8082, activer chaque DAG puis cliquer ▶ (Trigger DAG), dans l'ordre ci-dessus.

### Via la CLI

```bash
docker exec univdata-airflow-scheduler airflow dags trigger scraping_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger html_to_hudi_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger api_to_hudi_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger pdf_to_hudi_pipeline_optimized
docker exec univdata-airflow-scheduler airflow dags trigger export_to_postgres_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger index_elasticsearch_pipeline
```

### Commandes `spark-submit` exactes (hors Airflow)

Voir le détail complet (mémoire, timeouts, retries, variables d'environnement PDF) dans `pipeline_run_instructions.md`. Résumé des timeouts/retries Airflow :

| Étape | Timeout | Retries |
|---|---|---|
| ETL HTML → Hudi | 180 min | 2 (délai 3 min) |
| ETL API → Hudi | 60 min | 1 (délai 2 min) |
| ETL PDF → Hudi | 30 min | 1 (délai 2 min) |
| Export PostgreSQL | 180 min | 2 (délai 3 min) |
| Indexation Elasticsearch | 30 min | 2 (délai 5 min) |

### Créer le dashboard Metabase

```bash
pip install requests
python create_metabase_dashboard.py
```

> Avant de lancer le script, ajouter manuellement la base dans Metabase :
> Admin → Databases → Add database → PostgreSQL — Host `postgres`, Port `5432`, DB `metabase`, User `airflow`, Password `airflow`, **nom exact `Metabase PostgreSQL`**.

## Tables curées (Hudi)

Toutes en `curated-zone/{table}/`, format Hudi COPY_ON_WRITE, synchronisées avec Hive.

| Table | Clé (record_id) | Champs clés |
|---|---|---|
| `faculty_profiles` | MD5(full_name + university + source_url) | full_name, title, department, university, source_url |
| `programs` | MD5(program_name + university) | program_name, level, department, faculty, responsible |
| `publications` | DOI normalisé | doi, title, publication_date, type, cited_by_count, author_names, source_system |
| `documents_catalog` | MD5(contenu binaire PDF) | file_name, document_type, academic_year, needs_ocr, extraction_quality_score, extraction_status |

Détail complet des schémas dans `architecture.md`.

## Vérifier chaque étape

**Raw (MinIO) :**
```bash
docker exec univdata-minio-setup mc ls local/raw-web-html --recursive | wc -l
docker exec univdata-minio-setup mc ls local/raw-documents --recursive | wc -l
docker exec univdata-minio-setup mc ls local/raw-json --recursive | wc -l
```

**Tables Hive (HTML/API/PDF) :**
```bash
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "
  SELECT 'faculty_profiles', COUNT(*) FROM faculty_profiles
  UNION ALL SELECT 'programs', COUNT(*) FROM programs
  UNION ALL SELECT 'publications', COUNT(*) FROM publications
  UNION ALL SELECT 'documents_catalog', COUNT(*) FROM documents_catalog;
  "
```

**Export PostgreSQL :**
```bash
docker exec univdata-postgres psql -U airflow -d metabase -c "
  SELECT 'metabase_faculty_profiles', COUNT(*) FROM metabase_faculty_profiles
  UNION ALL SELECT 'metabase_programs', COUNT(*) FROM metabase_programs
  UNION ALL SELECT 'metabase_publications', COUNT(*) FROM metabase_publications
  UNION ALL SELECT 'metabase_documents_catalog', COUNT(*) FROM metabase_documents_catalog;
"
```

**Elasticsearch :**
```bash
curl -s http://localhost:9200/university_docs/_count | python -m json.tool
curl -s "http://localhost:9200/university_docs/_search?q=*&size=1" | python -m json.tool
```

**Logs Airflow :**
```bash
docker exec univdata-airflow-scheduler airflow tasks logs html_to_hudi_pipeline etl_html_to_hudi_via_docker
docker logs univdata-airflow-scheduler --tail 50 -f
```

## Relancer sans dupliquer les données

- **Hudi (upserts)** : idempotent par clé — relancer un ETL ne crée pas de doublons. Exception : si un job HTML plante en cours d'écriture en mode `append`, vider la table avant de relancer :
  ```bash
  docker exec univdata-minio-setup mc rm local/curated-zone/faculty_profiles --recursive --force
  ```
- **PDF** : déduplication via `source_path` dans `documents_catalog` — un PDF déjà catalogué n'est pas retraité.
- **Export PostgreSQL** : mode `overwrite` — toujours sûr à relancer.
- **Elasticsearch** : DELETE + recréation complète de l'index — toujours sûr à relancer.

## Dashboard Metabase

14 questions SQL prédéfinies, notamment :
- Total publications, enseignants, formations, documents PDF
- Publications par source (Crossref vs OpenAlex)
- Formations par niveau (Licence/Master/Filière)
- Score moyen d'extraction des PDF et taux nécessitant un OCR

## Recherche Elasticsearch

Index unique `university_docs` regroupant toutes les entités curées.

```bash
# Recherche par nom d'enseignant
curl -s "http://localhost:9200/university_docs/_search" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match": {"full_name": "Ahmed"}}, "size": 5}' | python -m json.tool

# Recherche multi-champs sur les formations
curl -s "http://localhost:9200/university_docs/_search" \
  -H "Content-Type: application/json" \
  -d '{"query": {"multi_match": {"query": "master informatique", "fields": ["program_name","level","department"]}}, "size": 5}' | python -m json.tool
```

## Runbook — problèmes connus

| Problème | Cause | Solution |
|---|---|---|
| `Table or view not found: faculty_profiles` | Hive Metastore vide (première exécution) | Lancer d'abord l'ETL HTML ou API ; Hudi crée la table via `hoodie.datasource.hive_sync.enable=true` |
| `SchemaCompatibilityException` | Colonne ajoutée au job mais ancien schéma Hudi existant | Vérifier `hoodie.datasource.write.reconcile.schema=true` ; sinon supprimer/recréer la table |
| `OutOfMemoryError: GC overhead limit exceeded` | Mémoire Spark insuffisante | Réduire `--driver-memory`, `--executor-memory`, `spark.sql.shuffle.partitions` ; pour PDF réduire `PDF_MAX_FILES`/`PDF_MAX_PAGES_PER_PDF` |
| Metabase : `column "document_name" does not exist` | Erreur dans `create_metabase_dashboard.py` (ligne 167) : colonne réelle = `file_name` | Corriger la requête dans le script, ou éditer la question manuellement dans Metabase |
| Timeout indexation Elasticsearch | ES pas encore prêt / GC JVM | Le job retente 30x/2s automatiquement ; sinon `docker compose restart elasticsearch` et relancer le DAG |
| `sudo: command not found` dans les tâches Airflow | `sudo` absent du conteneur | Vérifier que `/var/run/docker.sock` est monté ; sinon retirer `sudo` des commandes `docker exec` dans les DAGs |

**Monitoring rapide :**
```bash
docker compose ps
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"
docker logs univdata-spark-master --tail 100
```

## Démo

Script de démo complet (15 minutes, minutage détaillé) disponible dans `demo_script.md` :

1. **00:00–02:00** — Présentation du problème
2. **02:00–04:00** — Architecture et orchestration (Airflow)
3. **04:00–06:00** — Données brutes (MinIO)
4. **06:00–09:00** — Transformation Spark → Hive
5. **09:00–12:00** — Dashboard Metabase
6. **12:00–14:00** — Recherche Elasticsearch
7. **14:00–15:00** — Conclusion et perspectives

**Commandes de secours en cas de blocage pendant la démo :**
```bash
docker compose restart spark-master
docker compose restart elasticsearch
docker compose restart airflow-webserver
```

## Documentation complémentaire

- `architecture.md` — diagramme Mermaid complet, schémas de tables, infrastructure Docker détaillée
- `pipeline_run_instructions.md` — commandes `spark-submit` exactes de chaque DAG
- `runbook.md` — procédures run/monitor/recover complètes
- `demo_script.md` — script de démonstration minuté
