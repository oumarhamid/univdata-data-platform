# University DATA

Pipeline de données universitaires marocaines.
Il collecte, nettoie, stocke et expose les données de quatre facultés : enseignants, formations, publications scientifiques, documents PDF.

---

## Ce que fait le projet

1. **Scraping** — crawl BFS des sites web de FSAC, FSBM, FSR, FSTM + appels aux APIs OpenAlex et Crossref.
2. **Stockage brut** — tout est déposé dans MinIO (équivalent S3 local).
3. **ETL Spark** — trois jobs transforment les données brutes en tables structurées.
4. **Stockage curated** — les tables sont écrites en format Apache Hudi sur MinIO, synchronisées avec Hive.
5. **Export** — les tables Hudi sont copiées dans PostgreSQL pour Metabase.
6. **Indexation** — les mêmes tables sont indexées dans Elasticsearch.
7. **Orchestration** — Apache Airflow gère l'ordre et les retries.

---

## Stack technique

| Composant | Rôle | Port |
|---|---|---|
| **MinIO** | Lac de données (stockage brut + curated) | 9000 / 9001 |
| **Apache Spark 3.5** | Moteur de transformation ETL | 8080 / 7077 |
| **Apache Hudi 0.15** | Format de table transactionnelle sur MinIO | — |
| **Hive Metastore** | Catalogue de tables (schémas, emplacements) | 9083 |
| **PostgreSQL 13** | Base pour Airflow, Hive et Metabase | — |
| **Elasticsearch 8** | Recherche plein-texte | 9200 |
| **Kibana 8.10** | Interface de visualisation Elasticsearch | 5601 |
| **Apache Airflow** | Orchestration des DAGs | 8082 |
| **Metabase** | Tableau de bord analytique | 3000 |

---

## Prérequis

- Docker Desktop (ou Docker Engine + Compose v2)
- Au moins **8 Go de RAM** alloués à Docker
- Accès internet (pour télécharger les jars Hudi et les données des APIs)
- Python 3.10+ sur la machine hôte (pour `create_metabase_dashboard.py`)

---

## Installation et démarrage

### 1. Cloner le projet

```bash
git clone <url-du-repo>
cd univdata
```

### 2. Copier le fichier d'environnement

```bash
cp .env.exemple .env
```

Les valeurs par défaut fonctionnent pour un usage local :
- MinIO : `minioadmin` / `minioadmin`
- Airflow : `admin` / `admin`
- Metabase : `admin@example.com` / `admin123@`

### 3. Démarrer tous les services

```bash
docker compose up -d
```

Le démarrage complet prend **3 à 5 minutes**. Les buckets MinIO sont créés automatiquement par le service `minio-setup`.

### 4. Vérifier que tout est up

```bash
docker compose ps
```

Tous les services doivent être en état `Up` ou `running (healthy)`.

---

## Accès aux interfaces

| Interface | URL | Login |
|---|---|---|
| Airflow | http://localhost:8082 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:8080 | — |
| Metabase | http://localhost:3000 | admin@example.com / admin123@ |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |

---

## Lancer le pipeline

Les DAGs Airflow sont tous **paused** à la création. Il faut les activer et déclencher manuellement (sauf `scraping_pipeline` qui tourne toutes les semaines).

Ordre d'exécution :

```
scraping_pipeline        → données brutes dans MinIO
    ↓
html_to_hudi_pipeline    ┐
api_to_hudi_pipeline     ├ ETL → tables Hudi dans curated-zone
pdf_to_hudi_pipeline     ┘
    ↓
export_to_postgres_pipeline  → tables dans PostgreSQL
    ↓
index_elasticsearch_pipeline → index university_docs dans ES
```

Voir [pipeline_run_instructions.md](pipeline_run_instructions.md) pour les commandes exactes.

---

## Structure du projet

```
univdata/
├── airflow/
│   └── dags/
│       ├── 01_scraping_dag.py
│       ├── 02_html_to_hudi_dag.py
│       ├── 03_api_to_hudi_dag.py
│       ├── 04_pdf_to_hudi_dag.py
│       ├── export_to_postgres_dag.py
│       ├── index_elasticsearch_dag.py
│       └── utils/
│           ├── bfs_scraper_universal.py
│           ├── openalex_ingest.py
│           ├── crossref_ingest.py
│           └── minio_client.py
├── configs/
│   ├── fsac.univh2c.ma.json
│   ├── www.fsbm.ma.json
│   ├── www.fsr.ac.ma.json
│   └── www.fstm.ac.ma.json
├── spark/
│   ├── jobs/
│   │   ├── etl_html_to_hudi.py
│   │   ├── etl_apis_to_hudi.py
│   │   ├── etl_pdf_to_hudi.py
│   │   ├── export_to_postgres.py
│   │   └── index_elasticsearch.py
│   └── conf/
│       └── spark-defaults.conf
├── create_metabase_dashboard.py
├── docker-compose.yml
└── .env
```

---

## Arrêter le projet

```bash
# Arrêter sans supprimer les données
docker compose stop

# Arrêter et supprimer les volumes (données perdues)
docker compose down -v
```
