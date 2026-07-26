# Runbook — University DATA

Procédures pour lancer, surveiller et récupérer le pipeline.

---

## 1. Lancer le pipeline complet

### Ordre obligatoire

Il n'y a pas de dépendances automatiques entre DAGs. L'ordre doit être respecté manuellement.

```
1. scraping_pipeline          (données brutes)
       ↓
2. html_to_hudi_pipeline   ┐
   api_to_hudi_pipeline    ├  (peuvent tourner en parallèle)
   pdf_to_hudi_pipeline    ┘
       ↓
3. export_to_postgres_pipeline
       ↓
4. index_elasticsearch_pipeline
       ↓
5. python create_metabase_dashboard.py   (manuel, sur l'hôte)
```

### Via l'interface Airflow

1. Aller sur http://localhost:8082
2. Activer le DAG voulu (toggle bleu)
3. Cliquer l'icône ▶ (Trigger DAG)
4. Attendre que le run passe au vert

### Via la ligne de commande (CLI Airflow)

```bash
# Déclencher un DAG manuellement
docker exec univdata-airflow-scheduler airflow dags trigger scraping_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger html_to_hudi_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger api_to_hudi_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger pdf_to_hudi_pipeline_optimized
docker exec univdata-airflow-scheduler airflow dags trigger export_to_postgres_pipeline
docker exec univdata-airflow-scheduler airflow dags trigger index_elasticsearch_pipeline

# Vérifier l'état d'un run
docker exec univdata-airflow-scheduler airflow dags list-runs -d scraping_pipeline
```

---

## 2. Vérifier que chaque étape a réussi

### Scraping — vérifier MinIO

```bash
# HTML crawlés
docker exec univdata-minio-setup mc ls local/raw-web-html --recursive | wc -l

# PDF téléchargés
docker exec univdata-minio-setup mc ls local/raw-documents --recursive | wc -l

# JSON APIs
docker exec univdata-minio-setup mc ls local/raw-json --recursive | wc -l
```

### ETL HTML — vérifier les tables Hive

```bash
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "
  SELECT 'faculty_profiles', COUNT(*) FROM faculty_profiles
  UNION ALL
  SELECT 'programs', COUNT(*) FROM programs;
  "
```

Résultat attendu : au moins quelques centaines de lignes dans chaque table.

### ETL API — vérifier les publications

```bash
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "
  SELECT source_system, COUNT(*) as nb
  FROM publications
  GROUP BY source_system;
  "
```

Résultat attendu : lignes pour `openalex` et `crossref`.

### ETL PDF — vérifier le catalogue

```bash
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "
  SELECT document_type, needs_ocr, COUNT(*) as nb
  FROM documents_catalog
  GROUP BY document_type, needs_ocr
  ORDER BY nb DESC;
  "
```

### Export PostgreSQL — vérifier les tables

```bash
docker exec univdata-postgres psql -U airflow -d metabase -c "
  SELECT
    'metabase_faculty_profiles' as t, COUNT(*) FROM metabase_faculty_profiles
  UNION ALL
    SELECT 'metabase_programs', COUNT(*) FROM metabase_programs
  UNION ALL
    SELECT 'metabase_publications', COUNT(*) FROM metabase_publications
  UNION ALL
    SELECT 'metabase_documents_catalog', COUNT(*) FROM metabase_documents_catalog;
"
```

### Elasticsearch — vérifier l'index

```bash
# Santé de l'index
curl -s http://localhost:9200/university_docs/_stats | python -m json.tool | grep '"count"' | head -5

# Recherche simple
curl -s "http://localhost:9200/university_docs/_search?q=*&size=1" | python -m json.tool
```

### Logs Airflow — suivre un run en cours

```bash
# Voir les logs d'un task
docker exec univdata-airflow-scheduler airflow tasks logs html_to_hudi_pipeline etl_html_to_hudi_via_docker

# Logs en direct du scheduler
docker logs univdata-airflow-scheduler --tail 50 -f
```

---

## 3. Relancer après un échec sans dupliquer les données

### Hudi — les upserts sont idempotents

Les jobs ETL utilisent le mode `upsert` Hudi. Si un job échoue à mi-parcours et qu'on le relance, Hudi remplace les enregistrements existants par leur clé. Il n'y a **pas de doublon**.

Exception : si le job HTML plante après avoir écrit une partie des données, la prochaine exécution en `append` rajoutera des lignes. Dans ce cas, vider la table Hudi avant de relancer :

```bash
# Supprimer et recréer une table Hudi (exemple faculty_profiles)
docker exec univdata-minio-setup mc rm local/curated-zone/faculty_profiles --recursive --force
# La table sera recréée au prochain run ETL
```

### PDF — déduplication par source_path

Le job PDF vérifie les PDF déjà traités via la table Hive `documents_catalog` (champ `source_path`). Un PDF déjà en base ne sera pas retraité même si le job est relancé.

```bash
# Voir les PDF déjà catalogués
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "SELECT source_path FROM documents_catalog LIMIT 20;"
```

### Export PostgreSQL — mode overwrite

L'export est en mode `overwrite`. Relancer l'export **écrase** les tables PostgreSQL proprement. Pas de doublon possible.

### Elasticsearch — DELETE + recréation

L'indexation supprime l'index `university_docs` avant de le recréer. Relancer l'indexation est toujours sûr.

---

## 4. Problèmes connus et solutions

### Problème : Hive table n'existe pas (première exécution)

```
Error: Table or view not found: faculty_profiles
```

**Cause** : Le Hive Metastore est vide. La table est créée lors du premier upsert Hudi.

**Solution** : Lancer d'abord l'ETL HTML ou API. Hudi crée la table automatiquement grâce à `hoodie.datasource.hive_sync.enable=true`.

---

### Problème : Évolution de schéma Hudi bloquée

```
org.apache.hudi.exception.SchemaCompatibilityException: Failed to merge incompatible schemas
```

**Cause** : Une nouvelle colonne a été ajoutée au job ETL, mais la table Hudi existante a un ancien schéma.

**Solution** : Ajouter `hoodie.datasource.write.reconcile.schema=true` dans les options Hudi du job (déjà présent dans `etl_pdf_to_hudi.py`). Si l'erreur persiste, supprimer et recréer la table :

```bash
docker exec univdata-minio-setup mc rm local/curated-zone/<nom_table> --recursive --force
```

---

### Problème : Spark OOM (Out of Memory)

```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

**Cause** : Trop peu de mémoire allouée. Le worker a 3 Go maximum.

**Solution** : Réduire les paramètres dans la commande `spark-submit` :
```bash
--driver-memory 512m
--executor-memory 512m
--conf spark.sql.shuffle.partitions=4
--conf spark.cores.max=1
```

Et pour le PDF ETL, réduire les variables d'environnement :
```bash
PDF_MAX_FILES=2 PDF_MAX_PAGES_PER_PDF=1
```

---

### Problème : Metabase — question "Catalogue des documents PDF" échoue

```
ERROR: column "document_name" does not exist
```

**Cause** : La requête dans `create_metabase_dashboard.py` (ligne 167) sélectionne `document_name`, mais la colonne réelle s'appelle `file_name`.

**Solution temporaire** : Dans Metabase, modifier la question manuellement pour remplacer `document_name` par `file_name`.

**Solution durable** : Corriger la requête dans `create_metabase_dashboard.py` ligne 167 :
```python
# Avant
"          file_name,\n          document_name,\n"
# Après
"          file_name,\n"
```

---

### Problème : Elasticsearch — timeout lors de l'indexation

```
org.elasticsearch.ElasticsearchStatusException: ... 30000ms
```

**Cause** : Elasticsearch met du temps à démarrer ou la JVM est en GC.

**Solution** : Le job attend 30 essais avec 2 secondes entre chaque (`wait_for_elasticsearch`). Si ça échoue quand même :
```bash
# Redémarrer Elasticsearch
docker compose restart elasticsearch
# Attendre 60 secondes puis relancer le DAG
```

---

### Problème : `sudo docker exec` refusé (permission denied)

```
sudo: command not found
```

**Cause** : Les commandes dans les DAGs utilisent `sudo docker exec`. Si Airflow n'a pas sudo, le `BashOperator` échoue.

**Solution** : Vérifier que le socket Docker est bien monté :
```yaml
# docker-compose.yml — airflow-webserver et airflow-scheduler
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

Si le problème persiste, utiliser `docker exec` sans `sudo` dans les DAGs :
```python
spark_submit_cmd = "docker exec -u root univdata-spark-master ..."
```

---


## 5. Monitoring rapide

```bash
# État de tous les services
docker compose ps

# Mémoire utilisée par conteneur
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"

# Logs d'un service
docker logs univdata-spark-master --tail 100
docker logs univdata-airflow-scheduler --tail 100
docker logs univdata-elasticsearch --tail 50
```
