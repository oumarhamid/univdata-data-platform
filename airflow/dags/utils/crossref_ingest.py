import requests
import json
import os
from datetime import datetime
from .minio_client import upload_bytes, compute_hash

def ingest_crossref(bucket_raw_json="raw-json", max_works=2000, **context):
    """
    Ingère les métadonnées des publications depuis Crossref.
    Pagination par curseur (rows + cursor). Partitionnement year/month/day.
    """
    base_url = "https://api.crossref.org/works"
    rows = 100                     # max 1000 mais 100 est raisonnable
    cursor = "*"
    total_works = 0
    page_counter = 1
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    params = {
        "rows": rows,
        "cursor": cursor,
        # Filtre optionnel : publications de 2026 (à ajuster)
        "filter": "from-pub-date:2026-01-01,until-pub-date:2026-12-31"
    }

    while True:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        items = data.get('message', {}).get('items', [])
        if not items:
            break

        object_path = f"crossref/year={year}/month={month:02d}/day={day:02d}/works_page_{page_counter:04d}.json"

        metadata = {
            "source_system": "crossref",
            "crawl_timestamp": now.isoformat(),
            "page": page_counter,
            "content_hash": compute_hash(json.dumps(data, indent=2).encode())
        }
        upload_bytes(bucket_raw_json, object_path, json.dumps(data, indent=2).encode(), metadata)

        total_works += len(items)
        print(f"[Crossref] Page {page_counter}: {len(items)} œuvres → total {total_works}/{max_works}")

        if total_works >= max_works:
            break

        next_cursor = data.get('message', {}).get('next-cursor')
        if not next_cursor:
            break
        params['cursor'] = next_cursor
        page_counter += 1

    print(f"✅ Crossref terminé : {total_works} œuvres ingérées.")
    return f"Crossref ingested {total_works} works"