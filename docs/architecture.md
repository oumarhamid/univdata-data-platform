# Architecture — University DATA

## Diagramme Mermaid du flux complet

```mermaid
flowchart TD
    subgraph SOURCES["Sources"]
        WEB1["fsac.univh2c.ma"]
        WEB2["www.fsbm.ma"]
        WEB3["www.fsr.ac.ma"]
        WEB4["www.fstm.ac.ma"]
        OA["API OpenAlex\n(max 5 000 pubs/run)"]
        CR["API Crossref\n(max 2 000 pubs/run)"]
    end

    subgraph INGESTION["Ingestion (DAG 01 — scraping_pipeline, @weekly)"]
        BFS["BFS Scraper\nPlaywright + requests\ndepth=3, max=250 pages/domaine"]
        OAI["openalex_ingest.py\nPagination curseur"]
        CRI["crossref_ingest.py\nPagination curseur"]
    end

    subgraph MINIO_RAW["MinIO — Raw Zone"]
        BHTML["raw-web-html\n{domain}/{YYYY}/{MM}/{DD}/depth={N}/page.html"]
        BDOCS["raw-documents\n{domain}/{YYYY}/{MM}/{DD}/files/{md5}.pdf"]
        BJSON["raw-json\nopenalex/year=Y/month=M/day=D/works_page_NNNN.json\ncrossref/year=Y/month=M/day=D/works_page_NNNN.json"]
    end

    subgraph ETL["Transformation Spark ETL"]
        E1["DAG 02 — html_to_hudi_pipeline\netl_html_to_hudi.py\nExtraction enseignants + formations\nCorrection Mojibake, dédup RDD"]
        E2["DAG 03 — api_to_hudi_pipeline\netl_apis_to_hudi.py\nFusion OpenAlex+Crossref par DOI\nLecture partition la plus récente"]
        E3["DAG 04 — pdf_to_hudi_pipeline_optimized\netl_pdf_to_hudi.py\nExtraction pypdf, score qualité,\nclassification type, needs_ocr"]
    end

    subgraph MINIO_CUR["MinIO — Curated Zone (Apache Hudi COPY_ON_WRITE)"]
        T1["curated-zone/faculty_profiles/\nclé: MD5(nom+univ+url)"]
        T2["curated-zone/programs/\nclé: MD5(nom+univ)"]
        T3["curated-zone/publications/\nclé: DOI normalisé"]
        T4["curated-zone/documents_catalog/\nclé: MD5(contenu PDF)"]
    end

    HMS["Hive Metastore\nthrift://hive-metastore:9083\nDB: default\nTables: faculty_profiles, programs,\npublications, documents_catalog"]

    subgraph SERVING["Couche Serving"]
        EXP["DAG export_to_postgres_pipeline\nexport_to_postgres.py\nmode OVERWRITE JDBC"]
        PG["PostgreSQL — DB metabase\nmetabase_faculty_profiles\nmetabase_programs\nmetabase_publications\nmetabase_documents_catalog"]
        MB["Metabase :3000\n14 questions SQL\nTableau de bord"]
        IDX["DAG index_elasticsearch_pipeline\nindex_elasticsearch.py\nOverwrite complet (DELETE + index)"]
        ES["Elasticsearch :9200\nIndex: university_docs\nToutes les tables mélangées"]
        KB["Kibana :5601\nRecherche plein-texte"]
    end

    ORCH["Apache Airflow :8082\nLocalExecutor\nDocker socket monté\n6 DAGs"]

    WEB1 & WEB2 & WEB3 & WEB4 --> BFS --> BHTML
    BFS --> BDOCS
    OA --> OAI --> BJSON
    CR --> CRI --> BJSON

    BHTML --> E1 --> T1 & T2
    BJSON --> E2 --> T3
    BDOCS --> E3 --> T4

    T1 & T2 & T3 & T4 <--> HMS
    T1 & T2 & T3 & T4 --> EXP --> PG --> MB
    T1 & T2 & T3 & T4 --> IDX --> ES --> KB

    ORCH -.->|déclenche| E1 & E2 & E3 & EXP & IDX
```

---

## Flux MinIO — détail des chemins

```
MinIO
├── raw-web-html/
│   └── {domain}/
│       └── {YYYY}/{MM}/{DD}/
│           └── depth={N}/
│               └── {safe_filename}.html
│
├── raw-documents/
│   └── {domain}/
│       └── {YYYY}/{MM}/{DD}/
│           └── files/
│               └── {md5(url)}.pdf   ← nom = hash de l'URL source
│
├── raw-images/
│   └── {domain}/{YYYY}/{MM}/{DD}/images/{md5(url)}.{ext}
│
├── raw-json/
│   ├── openalex/year=YYYY/month=MM/day=DD/works_page_NNNN.json
│   └── crossref/year=YYYY/month=MM/day=DD/works_page_NNNN.json
│
├── raw-logs/
│   └── errors/
│
└── curated-zone/
    ├── faculty_profiles/
    │   └── .hoodie/  ← métadonnées Hudi (commits, timeline)
    ├── programs/
    ├── publications/
    └── documents_catalog/
```

---

## Schémas des tables Hudi

### `faculty_profiles`
| Champ | Type | Source |
|---|---|---|
| record_id | String | MD5(full_name + university + source_url) |
| full_name | String | Extraction HTML |
| title | String | Civilité déduite (Pr. → Professeur, Dr. → Docteur) |
| department | String | Contexte DOM autour du nom |
| university | String | Nom de domaine (ex: fsac.univh2c.ma) |
| source_system | String | Toujours "web_scraping" |
| source_url | String | s3a://raw-web-html/{key} |
| crawl_timestamp | String | ISO-8601 UTC |
| content_hash | String | MD5 du HTML nettoyé |
| is_deleted | Boolean | Toujours false |
| extraction_method | String | "html_parsing" |

### `programs`
| Champ | Type | Source |
|---|---|---|
| program_id | String | MD5(program_name + university) |
| program_name | String | Extraction HTML (6 stratégies) |
| level | String | Doctorat / Master / Licence / Filière |
| department | String | Contexte DOM |
| faculty | String | Domaine/université |
| responsible | String | Responsable si dans tableau HTML |
| source_url | String | s3a://raw-web-html/{key} |
| crawl_timestamp | String | ISO-8601 |

### `publications`
| Champ | Type | Source |
|---|---|---|
| work_id | String | URI DOI normalisé (clé primaire) |
| doi | String | DOI au format 10.xxxx/xxxx |
| title | String | Titre HTML-strippé |
| publication_date | Date | Date de publication |
| type | String | journal-article, conference-paper… |
| cited_by_count | Integer | Crossref uniquement |
| author_names | Array[String] | Liste des noms d'auteurs |
| author_orcids | Array[String] | ORCID des auteurs |
| author_institutions | Array[String] | Institutions |
| publisher | String | Éditeur (Crossref) |
| source_system | String | "openalex" ou "crossref" |
| crawl_timestamp | Timestamp | Horodatage Spark |

### `documents_catalog`
| Champ | Type | Source |
|---|---|---|
| record_id | String | MD5 du contenu binaire PDF |
| source_path | String | s3a://raw-documents/{key} |
| file_name | String | Nom du fichier |
| document_type | String | schedule / results / academic_program / announcement / administrative / other |
| program_name | String | Formation détectée |
| academic_year | String | Ex: 2025/2026 |
| semester | String | Ex: S1; S3 |
| student_count | Integer | Nb numéros Apogée (si student_list) |
| needs_ocr | Boolean | Vrai si PDF scanné non extractible |
| extraction_quality_score | Double | Score 0→1 |
| extraction_status | String | success / partial_success / failed |
| crawl_timestamp | String | ISO-8601 UTC |

---

## Infrastructure Docker

```
docker network: data-platform-net (bridge)

univdata-postgres          postgres:13
  ├── DB airflow           → Airflow scheduler/webserver
  ├── DB hive_metastore    → Hive Metastore JDO store
  └── DB metabase          → Metabase + tables exportées

univdata-minio             minio/minio:latest       :9000 :9001
univdata-minio-setup       minio/mc:latest          (one-shot, crée les buckets)
univdata-hive-metastore    build ./hive             :9083
univdata-spark-master      build ./spark            :7077 :8080
univdata-spark-worker      build ./spark            :8081  (2 cores, 3g RAM)
univdata-elasticsearch     build ./elasticsearch    :9200 :9300
univdata-kibana            kibana:8.10.2            :5601
univdata-airflow-init      build ./airflow          (one-shot, init DB)
univdata-airflow-webserver build ./airflow          :8082
univdata-airflow-scheduler build ./airflow
univdata-metabase          build ./metabase         :3000
```
