# Demo Script — University DATA (15 minutes)

Déroulé pour une démo complète, étape par étape.
Tout ce qui est montré vient du système en production.

---

## Avant la démo (préparer)

```bash
# S'assurer que tout tourne
docker compose ps

# Le pipeline doit avoir tourné au moins une fois
# Données attendues : ~718 enseignants, ~196 formations, ~2134 publications, ~10 PDF
```

Ouvrir ces onglets dans le navigateur :
1. **Airflow** — http://localhost:8082
2. **MinIO Console** — http://localhost:9001
3. **Spark UI** — http://localhost:8080
4. **Metabase** — http://localhost:3000
5. **Kibana / Elasticsearch** — http://localhost:9200

---

## [00:00 — 02:00] — Présentation du problème

> **Ce qu'on montre :** Rien de technique encore. On explique le contexte.

- Les universités marocaines publient leurs données sur des sites séparés, sans format standard.
- Impossible de faire une recherche croisée entre facultés.
- Ce projet centralise tout automatiquement.

**Sources couvertes** :
- fsac.univh2c.ma, www.fsbm.ma, www.fsr.ac.ma, www.fstm.ac.ma
- API OpenAlex (publications scientifiques mondiales)
- API Crossref (métadonnées bibliographiques)

---

## [02:00 — 04:00] — Architecture et orchestration

> **Ce qu'on montre :** Interface Airflow

1. Ouvrir http://localhost:8082
2. Montrer la liste des 5 DAGs :
   - `scraping_pipeline` — tourne chaque semaine automatiquement
   - `html_to_hudi_pipeline`, `api_to_hudi_pipeline`, `pdf_to_hudi_pipeline_optimized` — déclenchés manuellement après le scraping
   - `export_to_postgres_pipeline`, `index_elasticsearch_pipeline` — déclenchés après l'ETL
3. Cliquer sur `scraping_pipeline` → montrer le graphe de tâches :
   - APIs et sites web en parallèle
   - Convergence vers `all_ingestion_done`
4. Cliquer sur un run passé → montrer les logs d'une tâche

**Message clé** : "Airflow gère l'ordre, les retries, et les timeouts. On ne lance rien à la main."

---

## [04:00 — 06:00] — Les données brutes dans MinIO

> **Ce qu'on montre :** MinIO Console

1. Ouvrir http://localhost:9001 (minioadmin / minioadmin)
2. Parcourir les buckets :
   - `raw-web-html` → ouvrir un dossier domaine → montrer la structure `{domain}/{YYYY}/{MM}/{DD}/depth={N}/`
   - Ouvrir un fichier HTML → montrer les métadonnées S3 (source_url, depth, content_hash)
   - `raw-documents` → montrer un PDF téléchargé
   - `raw-json` → ouvrir `openalex/year=.../works_page_0001.json` → montrer la structure JSON brute
3. Montrer `curated-zone` → les dossiers Hudi avec fichiers Parquet et `.hoodie/`

**Message clé** : "MinIO est le point de vérité. Tout est là, rien n'est perdu."

---

## [06:00 — 09:00] — Transformation Spark en direct (ou résultat)

> **Ce qu'on montre :** Spark UI + résultat Hive

**Option A — Montrer un job en cours** (si on peut en lancer un) :
```bash
# Lancer l'ETL PDF (le plus court)
sudo docker exec -u root univdata-spark-master bash -lc \
  'export PDF_MAX_FILES=2 PDF_MAX_PAGES_PER_PDF=1 && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 512m --executor-memory 512m \
    --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0 \
    /opt/spark/jobs/etl_pdf_to_hudi.py'
```

Pendant que ça tourne, ouvrir http://localhost:8080 → montrer le job actif.

**Option B — Montrer les résultats déjà calculés** :
```bash
# Résumé de ce que Spark a extrait
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "
  SELECT 'enseignants' as type, COUNT(*) as total FROM faculty_profiles
  UNION ALL SELECT 'formations', COUNT(*) FROM programs
  UNION ALL SELECT 'publications', COUNT(*) FROM publications
  UNION ALL SELECT 'documents PDF', COUNT(*) FROM documents_catalog;
  "
```

Montrer aussi le score qualité des PDF :
```bash
docker exec -u root univdata-spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "
  SELECT file_name, document_type, needs_ocr, ROUND(extraction_quality_score, 2) as score
  FROM documents_catalog ORDER BY score DESC;
  "
```

**Message clé** : "Spark lit les HTML et PDF bruts, corrige les erreurs d'encodage, et écrit des tables propres dans Hudi — un format qui supporte les mises à jour sans réécrire tout le fichier."

---

## [09:00 — 12:00] — Tableau de bord Metabase

> **Ce qu'on montre :** Metabase

1. Ouvrir http://localhost:3000
2. Aller dans **Our analytics** → ouvrir le tableau de bord
3. Parcourir les indicateurs :
   - **2 134 publications** — "collectées en un seul run, fusion de deux APIs"
   - **718 enseignants** — "extraits des pages HTML avec un algorithme de parsing"
   - **196 formations** — "cataloguées depuis 4 sites différents"
   - **10 PDF traités** — "score moyen 0.43 → 90% nécessitent OCR"
4. Montrer le graphe **Publications par source** → Crossref 57.9% vs OpenAlex 42.1%
5. Montrer le graphe **Formations par niveau** → répartition Licence/Master/Filière
6. Cliquer sur une question SQL → montrer la requête SQL sous-jacente

Exemple de requête montrée :
```sql
SELECT ROUND(AVG(extraction_quality_score)::numeric, 2) AS score_moyen
FROM metabase_documents_catalog
```

**Message clé** : "Metabase lit directement PostgreSQL. Les questions sont des SQL standards. N'importe qui peut en créer."

---

## [12:00 — 14:00] — Recherche Elasticsearch

> **Ce qu'on montre :** Terminal ou Kibana

**Via curl** (plus parlant en démo) :

```bash
# Rechercher un enseignant par nom
curl -s "http://localhost:9200/university_docs/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": { "match": { "full_name": "Ahmed" } },
    "size": 5
  }' | python -m json.tool

# Chercher une formation par mot-clé
curl -s "http://localhost:9200/university_docs/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": { "multi_match": {
      "query": "master informatique",
      "fields": ["program_name", "level", "department"]
    }},
    "size": 5
  }' | python -m json.tool

# Compter les documents dans l'index
curl -s "http://localhost:9200/university_docs/_count" | python -m json.tool
```

**Via Kibana** (si configuré) :
1. Ouvrir http://localhost:5601
2. Dev Tools → saisir la requête de recherche

**Message clé** : "Elasticsearch permet de chercher sur toutes les données en même temps — un nom d'enseignant, une formation, une publication — sans SQL."

---

## [14:00 — 15:00] — Conclusion et perspectives

> **Ce qu'on dit :**

- **Ce qu'on a construit** : un pipeline complet, de la source brute jusqu'à la visualisation, entièrement automatisé.
- **Ce qui marche** : scraping, APIs, ETL, export, indexation, dashboard.
- **Ce qui peut s'améliorer** :
  - Ajouter un moteur OCR pour les 90% de PDF scannés
  - Étendre à d'autres universités marocaines
  - Rendre le filtre de dates OpenAlex dynamique (fenêtre glissante)
  - Ajouter des dépendances cross-DAG dans Airflow

---

## Commandes de secours (si quelque chose ne marche pas)

```bash
# Redémarrer un service qui bloque
docker compose restart spark-master
docker compose restart elasticsearch
docker compose restart airflow-webserver

# Vérifier les logs d'un service
docker logs univdata-spark-master --tail 100
docker logs univdata-elasticsearch --tail 50

# Réinitialiser entièrement (supprime les données)
docker compose down -v && docker compose up -d
```
