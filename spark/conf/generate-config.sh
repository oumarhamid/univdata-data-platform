#!/bin/bash
# Remplacer les variables d'environnement dans le template
envsubst < /opt/bitnami/spark/conf/spark-defaults.conf.template > /opt/bitnami/spark/conf/spark-defaults.conf
# Lancer la commande originale
exec "$@"