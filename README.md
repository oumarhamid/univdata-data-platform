# University Data Platform

Plateforme de données universitaire : ingestion multi-sources, stockage brut, transformation, lakehouse curé, exposition SQL, BI et recherche full-text.

```
Sources (API / Web / Fichiers)
        │
        ▼
    MinIO (raw zone)
        │
        ▼
   Spark (ETL / normalisation)
        │
        ▼
  Apache Hudi (curated zone)
        │
        ▼
   Hive Metastore (catalogue SQL)
       ╱        ╲
      ▼           ▼
  Metabase   Elasticsearch
  (BI)        (recherche)

  Orchestration : Apache Airflow
```

## Sommaire

- [Objectif](#objectif)
- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Structure du projet](#structure-du-projet)
- [Prérequis](#prérequis)
- [Installation et démarrage](#installation-et-démarrage)
- [Services et ports](#services-et-ports)
- [Pipelines Airflow](#pipelines-airflow)
- [Runbook (run / monitor / recover)](#runbook-run--monitor--recover)
- [Tables curées (Hudi)](#tables-curées-hudi)
- [Dashboard Metabase](#dashboard-metabase)
- [Recherche Elasticsearch](#recherche-elasticsearch)
- [Tests](#tests)
- [Dépannage](#dépannage)

## Objectif

Ce projet implémente un pipeline de données de bout en bout pour un environnement universitaire :

- Ingestion depuis 3 types de sources : API (OpenAlex/Crossref), sites web statiques (facultés), fichiers (CSV/JSON).
- Stockage brut traçable dans MinIO (S3-compatible).
- Transformation et normalisation avec Spark.
- Production de tables curées Apache Hudi (`faculty_profiles`, `course_catalog`, etc.).
- Exposition SQL via Hive Metastore.
- Restitution via un dashboard Metabase et un moteur de recherche Elasticsearch.
- Orchestration et exécution planifiée via Airflow (DAGs, logs, retries).

## Architecture

| Couche | Technologie | Rôle |
|---|---|---|
| Orchestration | Apache Airflow | Planification des DAGs, logs, reprise sur erreur |
| Ingestion | Requests, BeautifulSoup, APIs | Collecte des données externes/internes |
| Stockage brut | MinIO | Object storage S3-compatible pour la donnée non transformée |
| Traitement | Apache Spark | ETL, normalisation, déduplication, enrichissement |
| Lakehouse | Apache Hudi | Tables analytiques versionnées avec upserts |
| Catalogue SQL | Hive Metastore | Accès SQL aux tables Hudi |
| BI | Metabase | Dashboards et exploration |
| Recherche | Elasticsearch (+ Kibana) | Recherche full-text, filtres, observabilité |
| Métadonnées PostgreSQL | PostgreSQL | Backend Airflow + Hive Metastore |

Chaque objet brut stocké dans MinIO conserve ses métadonnées (source, URL, timestamp, checksum, version du connecteur) afin de garantir la traçabilité entre les couches raw → curated → serving.

## Stack technique

- **Orchestration** : Apache Airflow (LocalExecutor)
- **Stockage objet** : MinIO
- **Traitement distribué** : Apache Spark (master + worker)
- **Lakehouse** : Apache Hudi 0.15.0 (bundle Spark 3)
- **Catalogue** : Hive Metastore (backend PostgreSQL)
- **BI** : Metabase
- **Recherche** : Elasticsearch 8.x + Kibana
- **Base de métadonnées** : PostgreSQL 13
- **Conteneurisation** : Docker / Docker Compose

## Structure du projet

```
.
├── airflow/
│   ├── dags/            # DAGs de scraping et d'ingestion
│   ├── data/             # Données locales utilisées par les DAGs
│   ├── logs/             # Logs d'exécution Airflow
│   └── plugins/
├── configs/               # Fichiers de configuration partagés
├── elasticsearch/
│   └── config/
├── hive/
│   ├── conf/              # core-site.xml, etc.
│   ├── lib/               # hudi-spark3-bundle_2.12-0.15.0.jar
│   └── scripts/
├── metabase/
├── minio/
│   └── data/              # Données MinIO persistées
├── postgres/
│   └── init/              # Scripts d'initialisation PostgreSQL
├── scripts/                # Scripts utilitaires (setup buckets, etc.)
├── spark/
│   ├── conf/              # spark-defaults.conf
│   └── jobs/               # Jobs Spark (ETL)
├── tests/
├── docker-compose.yml
└── README.md
```

## Prérequis

- Docker et Docker Compose v2
- 8 Go de RAM disponibles minimum (recommandé 12+ Go pour Spark + Elasticsearch + Airflow)
- Ports 3000, 5601, 7077, 8080, 8081, 8082, 9000, 9001, 9083, 9200, 9300 libres sur la machine hôte

## Installation et démarrage

1. **Cloner le dépôt et se placer à la racine :**

   ```bash
   git clone <url-du-repo>
   cd <nom-du-repo>
   ```

2. **Démarrer l'ensemble de la stack :**

   ```bash
   docker compose up -d --build
   ```

3. **Vérifier l'état des services :**

   ```bash
   docker compose ps
   ```

4. **Initialiser les buckets MinIO** (fait automatiquement par le service `minio-setup`, dépendant du healthcheck de `minio`). Vérifier les logs si besoin :

   ```bash
   docker compose logs minio-setup
   ```

5. **Créer l'utilisateur Airflow** (fait automatiquement par `airflow-init`, exécuté une seule fois avant le webserver et le scheduler).

6. **Accéder aux interfaces** (voir la table des ports ci-dessous).

7. **Arrêter la stack :**

   ```bash
   docker compose down
   ```

   Pour tout réinitialiser (⚠️ supprime les volumes/données) :

   ```bash
   docker compose down -v
   ```

## Services et ports

| Service | URL locale | Identifiants par défaut |
|---|---|---|
| Airflow Webserver | http://localhost:8082 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API (S3) | http://localhost:9000 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Hive Metastore (Thrift) | thrift://localhost:9083 | — |
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |
| Metabase | http://localhost:3000 | à créer au premier lancement |

## Pipelines Airflow

Le DAG principal (`scraping_pipeline`) orchestre les tâches suivantes :

- `ingest_openalex` : ingestion via l'API OpenAlex
- `ingest_crossref` : ingestion via l'API Crossref
- `scrape_*` (ex : `scrape_fsac.univh2c.ma`, `scrape_www.fsbm.ma`, `scrape_www.fsr.ac.ma`, `scrape_www.fstm.ac.ma`) : scraping des sites de facultés
- `ingest_local_csv_if_exists` : ingestion d'un fichier CSV local s'il est présent

Chaque tâche écrit ses données brutes dans MinIO avec les métadonnées de traçabilité (source, URL, timestamp, checksum).

Pour déclencher une exécution manuelle :

```bash
docker compose exec airflow-webserver airflow dags trigger scraping_pipeline
```

Pour consulter les logs d'une tâche :

```bash
docker compose exec airflow-webserver airflow tasks logs scraping_pipeline <task_id> <run_id>
```

## Runbook (run / monitor / recover)

**Lancer un run complet :**

```bash
docker compose up -d
docker compose exec airflow-webserver airflow dags trigger scraping_pipeline
```

**Monitorer :**

- Statut des DAGs : UI Airflow (http://localhost:8082) → onglet DAGs
- Statut des jobs Spark : UI Spark Master (http://localhost:8080)
- Santé des buckets MinIO : Console MinIO (http://localhost:9001)
- État des index : `curl http://localhost:9200/_cat/indices?v`

**Récupérer après un échec :**

1. Identifier la tâche en échec dans l'UI Airflow ou via `airflow tasks list scraping_pipeline`.
2. Consulter les logs de la tâche pour identifier la cause (ex : connectivité source, schéma inattendu).
3. Corriger la source du problème (ex : mapping de colonnes, timeout HTTP).
4. Relancer uniquement la tâche en échec (rerun sûr, idempotent grâce aux upserts Hudi) :

   ```bash
   docker compose exec airflow-webserver airflow tasks clear scraping_pipeline -t <task_id> -s <date_debut> -e <date_fin>
   ```

5. Vérifier que les données curées ont bien été mises à jour dans Hive/Metabase.

**Réinitialisation complète en cas de blocage majeur :**

```bash
docker compose down
docker compose up -d --build
```

## Tables curées (Hudi)

| Table | Description |
|---|---|
| `faculty_profiles` | Profils des enseignants-chercheurs (nom, faculté, publications, contact) |
| `course_catalog` | Catalogue des cours (intitulé, faculté, niveau, description) |

Champs communs recommandés : `record_id`, `source_system`, `source_url`, `content_hash`, `crawl_timestamp`, `business_timestamp`, `is_deleted`, `language`, `normalized_text`.

Accès SQL via Spark Thrift Server (`univdata-spark-thrift`, port du Hive Metastore : 9083) ou directement via Hive Metastore depuis Metabase/Spark SQL.

## Dashboard Metabase

Un dashboard unique regroupant 5 à 7 KPIs, par exemple :

- Nombre de profils enseignants ingérés par faculté
- Nombre de cours par niveau/faculté
- Taux de complétude des métadonnées
- Volume de données brutes ingérées par jour
- Nombre de doublons détectés/nettoyés

## Recherche Elasticsearch

Un index principal exposant une recherche par mots-clés sur les entités curées (profils, cours), avec filtres par faculté, langue et type de document.

Vérifier la santé du cluster :

```bash
curl http://localhost:9200/_cluster/health?pretty
```

## Tests

Le dossier `tests/` contient les tests de validation du pipeline (ingestion, transformation, cohérence des tables curées). Lancer via :

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/<test_job>.py
```

_(Adapter la commande selon le framework de test utilisé — pytest, spark-testing-base, etc.)_

## Dépannage

- **Aucune donnée dans Grafana/Metabase** : vérifier que le fichier de configuration source (ex : `prometheus.yml` ou la connexion Hive) n'est pas vide et que les services dépendants (`postgres`, `minio`) sont bien `healthy` (`docker compose ps`).
- **Airflow scheduler ne démarre pas** : vérifier que `airflow-init` s'est bien terminé (`service_completed_successfully`) et consulter `docker compose logs airflow-scheduler`.
- **Erreur de connexion Hive Metastore depuis Spark** : vérifier que `hive-metastore` est démarré avant `spark-thrift` et que `core-site.xml` pointe bien vers MinIO (`spark.hadoop.fs.s3a.endpoint`).
- **Bucket MinIO manquant** : relancer manuellement `docker compose run --rm minio-setup`.