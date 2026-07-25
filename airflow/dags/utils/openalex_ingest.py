import requests
import json
import os
from datetime import datetime
from .minio_client import upload_bytes, compute_hash

def ingest_openalex(bucket_raw_json="raw-json", max_works=5000, **context):
    """
    Ingère les données OpenAlex par pagination continue (curseur) sur une plage de dates fixe.
    Structure identique à celle de crossref_ingest.py.
    """
    base_url = "https://api.openalex.org/works"
    per_page = 50                     # nombre d'œuvres par page (max 200)
    select_fields = "id,title,publication_date,authorships,institutions,concepts,doi"

    # Plage de dates (vous pouvez modifier ces dates comme dans Crossref)
    filter_query = "from_publication_date:2026-06-01,to_publication_date:2026-06-30"

    # Date d'extraction (utilisée pour le partitionnement MinIO)
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    cursor = "*"
    total_works = 0
    page_counter = 1

    params = {
        "filter": filter_query,
        "per-page": per_page,
        "cursor": cursor,
        "select": select_fields
    }

    # Clé API OpenAlex (optionnelle)
    api_key = os.getenv("OPENALEX_API_KEY")
    headers = {}
    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'

    while True:
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        results = data.get('results', [])

        if not results:
            break

        # Partitionnement (identique à Crossref)
        object_path = f"openalex/year={year}/month={month:02d}/day={day:02d}/works_page_{page_counter:04d}.json"

        metadata = {
            "source_system": "openalex",
            "crawl_timestamp": now.isoformat(),
            "filter": filter_query,
            "page": page_counter,
            "content_hash": compute_hash(json.dumps(data, indent=2).encode())
        }
        upload_bytes(bucket_raw_json, object_path, json.dumps(data, indent=2).encode(), metadata)

        total_works += len(results)
        print(f"[OpenAlex] Page {page_counter}: {len(results)} œuvres → total {total_works}/{max_works}")

        if total_works >= max_works:
            break

        # Récupération du prochain curseur (comme Crossref)
        next_cursor = data.get('meta', {}).get('next_cursor')
        if not next_cursor:
            break

        params['cursor'] = next_cursor
        page_counter += 1

    print(f"✅ OpenAlex terminé : {total_works} œuvres ingérées (limite {max_works}).")
    return f"OpenAlex ingested {total_works} works"