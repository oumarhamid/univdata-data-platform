-- Création de la base pour Hive Metastore
CREATE DATABASE IF NOT EXISTS hive_metastore;

GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO airflow;