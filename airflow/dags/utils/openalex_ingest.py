import requests
import json
import os
from datetime import datetime
from .minio_client import upload_bytes, compute_hash

# On peut utiliser une clé API si nécessaire (OpenAlex n'en nécessite pas, mais pour d'autres si)
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", None)

def ingest_openalex(bucket_raw_json="raw-json", **context):
    url = "https://api.openalex.org/works"
    params = {
        "filter": "institutions.country_code:MA",
        "per-page": 200
    }
    headers = {}
    if OPENALEX_API_KEY:
        headers['Authorization'] = f'Bearer {OPENALEX_API_KEY}'
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    content = json.dumps(data, indent=2).encode('utf-8')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_path = f"openalex/works_{timestamp}.json"
    
    metadata = {
        "source_system": "openalex",
        "crawl_timestamp": datetime.now().isoformat(),
        "content_hash": compute_hash(content),
        "record_count": len(data.get('results', []))
    }
    upload_bytes(bucket_raw_json, object_path, content, metadata)
    return f"Ingested {len(data.get('results', []))} works"