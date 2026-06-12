#!/bin/bash
set -e

# Variables d'environnement (issues de docker-compose ou défauts)
DB_HOST=${DB_HOST:-postgres}
DB_PORT=${DB_PORT:-5432}
DB_USER=${POSTGRES_USER:-airflow}
DB_PASS=${POSTGRES_PASSWORD:-airflow}
DB_NAME=${HIVE_METASTORE_DB:-hive_metastore}

echo "Waiting for PostgreSQL at $DB_HOST:$DB_PORT..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  sleep 2
done

echo "Creating database if not exists..."
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -tc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 || \
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -c "CREATE DATABASE $DB_NAME"

echo "Using DB_USER: $DB_USER"

# Générer hive-site.xml dynamiquement
cat > /opt/hive/conf/hive-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://$DB_HOST:$DB_PORT/$DB_NAME</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>$DB_USER</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>$DB_PASS</value>
  </property>
  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.autoCreateTables</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://0.0.0.0:9083</value>
  </property>
</configuration>
EOF

echo "Initializing Hive schema (PostgreSQL)..."
/opt/hive/bin/schematool -dbType postgres -initSchema

echo "Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore