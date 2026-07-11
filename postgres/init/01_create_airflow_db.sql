-- Création des bases manquantes
CREATE DATABASE hive_metastore;
CREATE DATABASE metabase;

-- Attribution des droits à l'utilisateur 'airflow' (qui est créé par défaut par le conteneur)
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO airflow;
GRANT ALL PRIVILEGES ON DATABASE metabase TO airflow;