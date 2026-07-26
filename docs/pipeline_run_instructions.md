# Pipeline Run Instructions

Commandes exactes pour lancer chaque étape du pipeline, tirées des DAGs Airflow.

> **Prérequis** : `docker compose up -d` lancé, tous les services en bonne santé.

---

## Étape 0 — Vérifier que tout est opérationnel

```bash
# Statut de tous les conteneurs
docker compose ps

# Tester MinIO
curl http://localhost:9000/minio/health/live

# Tester Elasticsearch
curl http://localhost:9200

# Tester Hive Metastore (depuis le conteneur Spark)
docker exec univdata-spark-master bash -c "curl -s telnet://hive-metastore:9083 || echo 'Hive OK'"
```

---

## Étape 1 — Scraping + Ingestion APIs (DAG 01)

**Via Airflow** (recommandé) :
1. Aller sur http://localhost:8082
2. Activer le DAG `scraping_pipeline`
3. Cliquer **Trigger DAG**

**Via Python directement** (hors Airflow) :
```bash
# Depuis le conteneur airflow-webserver
docker exec -it univdata-airflow-webserver bash

# Scraping d'un domaine
python -c "
from dags.utils.bfs_scraper_universal import bfs_scrape_universal
bfs_scrape_universal('/opt/airflow/configs/fsac.univh2c.ma.json')
"

# Ingestion OpenAlex (5000 publications max)
python -c "
from dags.utils.openalex_ingest import ingest_openalex
ingest_openalex(max_works=5000)
"

# Ingestion Crossref (2000 publications max)
python -c "
from dags.utils.crossref_ingest import ingest_crossref
ingest_crossref(max_works=2000)
"
```

**Vérifier le résultat** :
```bash
# Lister les objets dans raw-web-html (via MinIO Console ou mc)
docker exec univdata-minio-setup mc ls local/raw-web-html --recursive | head -20

# Lister les JSON OpenAlex
docker exec univdata-minio-setup mc ls local/raw-json/openalex/
```

---

## Étape 2a — ETL HTML → Hudi (DAG 02)

**Commande exacte tirée du DAG** `02_html_to_hudi_dag.py` :

```bash
sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1536m \
  --executor-cores 2 \
  --conf spark.cores.max=2 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8 \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.network.timeout=300s \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
  /opt/spark/jobs/etl_html_to_hudi.py
```

Timeout Airflow : **180 minutes**. Retries : 2 (délai 3 min).

**Vérifier le résultat** :
```bash
# Compter les lignes dans Hive
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "SELECT COUNT(*) FROM faculty_profiles; SELECT COUNT(*) FROM programs;"
```

---

## Étape 2b — ETL API → Hudi (DAG 03)

**Commande exacte tirée du DAG** `03_api_to_hudi_dag.py` :

```bash
sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=4 \
  --conf spark.memory.fraction=0.8 \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
  /opt/spark/jobs/etl_apis_to_hudi.py
```

Timeout Airflow : **60 minutes**. Retries : 1 (délai 2 min).

**Vérifier le résultat** :
```bash
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "SELECT COUNT(*), source_system FROM publications GROUP BY source_system;"
```

---

## Étape 2c — ETL PDF → Hudi (DAG 04)

**Commande exacte tirée du DAG** `04_pdf_to_hudi_dag.py` :

```bash
sudo docker exec -u root univdata-spark-master bash -lc \
  'export PDF_MAX_FILES=5 PDF_MAX_PAGES_PER_PDF=2 PDF_MAX_FILE_SIZE_MB=6 PDF_MAX_TEXT_CHARS=8000 && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 512m \
    --executor-memory 1024m \
    --conf spark.executor.cores=1 \
    --conf spark.cores.max=1 \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.default.parallelism=2 \
    --conf spark.memory.fraction=0.4 \
    --conf spark.memory.storageFraction=0.3 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
    /opt/spark/jobs/etl_pdf_to_hudi.py'
```

Variables d'environnement configurables :
| Variable | Valeur DAG | Effet |
|---|---|---|
| `PDF_MAX_FILES` | 5 | Nombre de PDF traités par run |
| `PDF_MAX_PAGES_PER_PDF` | 2 | Pages extraites par PDF |
| `PDF_MAX_FILE_SIZE_MB` | 6 | Taille max du fichier |
| `PDF_MAX_TEXT_CHARS` | 8000 | Caractères max après nettoyage |

Timeout Airflow : **30 minutes**. Retries : 1 (délai 2 min).

**Vérifier le résultat** :
```bash
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "SELECT file_name, document_type, needs_ocr, extraction_quality_score FROM documents_catalog ORDER BY extraction_quality_score DESC LIMIT 10;"
```

---

## Étape 3 — Export vers PostgreSQL (DAG export)

**Commande exacte tirée du DAG** `export_to_postgres_dag.py` :

```bash
sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1536m \
  --executor-cores 2 \
  --conf spark.cores.max=2 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8 \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.network.timeout=300s \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0,org.postgresql:postgresql:42.5.1 \
  /opt/spark/jobs/export_to_postgres.py
```

Timeout Airflow : **180 minutes**. Retries : 2 (délai 3 min).

Tables créées dans PostgreSQL (DB `metabase`, mode OVERWRITE) :
- `metabase_faculty_profiles`
- `metabase_programs`
- `metabase_publications`
- `metabase_documents_catalog`

**Vérifier le résultat** :
```bash
docker exec univdata-postgres psql -U airflow -d metabase \
  -c "SELECT table_name, (SELECT COUNT(*) FROM information_schema.columns WHERE table_name=t.table_name) FROM information_schema.tables t WHERE table_schema='public';"
```

---

## Étape 4 — Indexation Elasticsearch (DAG index)

**Commande exacte tirée du DAG** `index_elasticsearch_dag.py` :

```bash
sudo docker exec -u root univdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 1024m \
  --executor-cores 1 \
  --conf spark.cores.max=1 \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=4 \
  --conf spark.memory.fraction=0.5 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatTimeout=360s \
  --conf spark.storage.blockManagerSlaveTimeout=300s \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
  /opt/spark/jobs/index_elasticsearch.py
```

Timeout Airflow : **30 minutes**. Retries : 2 (délai 5 min).

Le job supprime l'index `university_docs` avant de le recréer (`curl -X DELETE`).

**Vérifier le résultat** :
```bash
# Compter les documents dans l'index
curl -s http://localhost:9200/university_docs/_count | python -m json.tool

# Recherche test
curl -s "http://localhost:9200/university_docs/_search?q=professeur&size=3" | python -m json.tool
```

---

## Étape 5 — Créer le tableau de bord Metabase

**Script Python** `create_metabase_dashboard.py` (sur la machine hôte) :

```bash
# Prérequis : requests installé
pip install requests

# Lancer le script (Metabase doit être accessible sur localhost:3000)
python create_metabase_dashboard.py
```

Le script :
1. Attend que Metabase soit prêt
2. Se connecte avec `admin@example.com` / `admin123@`
3. Archive toutes les questions existantes (pour éviter les doublons)
4. Crée 14 nouvelles questions SQL

> **Important** : La base PostgreSQL doit d'abord être ajoutée manuellement dans Metabase :  
> Admin → Databases → Add database → PostgreSQL  
> Host: `postgres`, Port: `5432`, Database: `metabase`, User: `airflow`, Password: `airflow`  
> Nom exact attendu par le script : **`Metabase PostgreSQL`**
