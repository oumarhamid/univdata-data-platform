import requests
import json
import os
from datetime import datetime
from .minio_client import upload_bytes, compute_hash

def ingest_orcid(bucket_raw_json="raw-json", max_records=1000, **context):
    """
    Ingère les profils ORCID (recherche publique).
    Partitionnement year/month/day. Nécessite un token ORCID_API_TOKEN (optionnel).
    """
    base_url = "https://pub.orcid.org/v3.0/search/"
    token = os.getenv("ORCID_API_TOKEN")
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        print("⚠️ ORCID_API_TOKEN non défini, l'API publique a des limites faibles.")

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    params = {
        "q": "creation-date:[2026-01-01 TO 2026-12-31]",   # à adapter
        "rows": 100,   # max 200
        "start": 0
    }
    total_records = 0
    page_counter = 1

    while True:
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ ORCID error {response.status_code}: {response.text}")
            break
        data = response.json()
        results = data.get('result', [])
        if not results:
            break

        object_path = f"orcid/year={year}/month={month:02d}/day={day:02d}/profiles_page_{page_counter:04d}.json"

        metadata = {
            "source_system": "orcid",
            "crawl_timestamp": now.isoformat(),
            "page": page_counter,
            "content_hash": compute_hash(json.dumps(data, indent=2).encode())
        }
        upload_bytes(bucket_raw_json, object_path, json.dumps(data, indent=2).encode(), metadata)

        total_records += len(results)
        print(f"[ORCID] Page {page_counter}: {len(results)} profils → total {total_records}/{max_records}")

        if total_records >= max_records:
            break

        params['start'] = page_counter * params['rows']
        page_counter += 1

    print(f"✅ ORCID terminé : {total_records} profils ingérés.")
    return f"ORCID ingested {total_records} profiles"