#!/bin/bash
set -e

# Attendre PostgreSQL
until pg_isready -h postgres -p 5432 -U airflow; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Définir le répertoire de configuration
export HIVE_CONF_DIR=/opt/hive/conf

# Vérifier si le schéma existe déjà
if schematool -dbType postgres -info > /dev/null 2>&1; then
  echo "Schéma Hive Metastore déjà initialisé."
else
  echo "Initialisation du schéma Hive Metastore..."
  schematool -dbType postgres -initSchema
fi

# Démarrer le service
exec /opt/hive/bin/hive --service metastore -p 9083