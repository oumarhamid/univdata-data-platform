#!/bin/sh
sleep 5
mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc mb local/${MINIO_BUCKET_RAW_HTML}    --ignore-existing
mc mb local/${MINIO_BUCKET_RAW_DOCS}    --ignore-existing
mc mb local/${MINIO_BUCKET_RAW_IMAGES}  --ignore-existing
mc mb local/${MINIO_BUCKET_RAW_JSON}    --ignore-existing
mc mb local/${MINIO_BUCKET_RAW_LOGS}    --ignore-existing
mc mb local/curated-zone                --ignore-existing
echo "Buckets créés"