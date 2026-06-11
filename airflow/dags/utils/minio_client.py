from minio import Minio
from minio.error import S3Error
import os
import hashlib
import io

# Récupération des variables d'environnement (avec fallback)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

_client = None

def get_client():
    global _client
    if _client is None:
        _client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
    return _client

def ensure_bucket(bucket_name):
    client = get_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

def upload_bytes(bucket_name, object_path, data, metadata=None):
    """Upload des bytes vers MinIO avec métadonnées"""
    ensure_bucket(bucket_name)
    client = get_client()
    if isinstance(data, bytes):
        length = len(data)
        data_stream = io.BytesIO(data)
    else:
        # Si c'est un fichier-like
        data_stream = data
        length = -1  # unknown, mais put_object le gère
    try:
        client.put_object(
            bucket_name, object_path, data_stream, length,
            metadata=metadata or {}
        )
        print(f"Uploaded {object_path} to {bucket_name}")
    except S3Error as e:
        print(f"Error uploading {object_path}: {e}")

def upload_file(bucket_name, object_path, file_path, metadata=None):
    """Upload d'un fichier local"""
    with open(file_path, 'rb') as f:
        data = f.read()
    upload_bytes(bucket_name, object_path, data, metadata)

def compute_hash(content):
    return hashlib.md5(content).hexdigest()