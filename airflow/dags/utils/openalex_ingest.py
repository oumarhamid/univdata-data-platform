import requests
import json
import os
from datetime import datetime, timedelta
from .minio_client import upload_bytes, compute_hash

def ingest_openalex(bucket_raw_json="raw-json", max_works=5000, **context):
    """
    Ingère les données OpenAlex par plages hebdomadaires et curseur.
    Partitionnement par date (year/month/day) pour éviter les fichiers trop gros.
    Limite le nombre total d'œuvres récupérées à `max_works`.
    """
    base_url = "https://api.openalex.org/works"
    per_page = 50                     # Moins de résultats par page → fichiers plus petits
    select_fields = "id,title,publication_date,authorships,institutions,concepts,doi"
    
    # Période à récupérer (exemple : juin 2026)
    start_date = datetime(2026, 6, 1)
    end_date   = datetime(2026, 6, 30)
    
    total_works = 0
    all_done = False
    
    # Découpage par semaine
    current_start = start_date
    while current_start <= end_date and not all_done:
        current_end = min(current_start + timedelta(days=6), end_date)
        filter_query = f"from_publication_date:{current_start.date().isoformat()},to_publication_date:{current_end.date().isoformat()}"
        
        params = {
            "filter": filter_query,
            "per-page": per_page,
            "cursor": "*",
            "select": select_fields
        }
        
        api_key = os.getenv("OPENALEX_API_KEY")
        headers = {}
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'
        
        page_counter = 1
        while True:
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            if not results:
                break
            
            # Sauvegarder cette page (même si on dépasse la limite, on sauvegarde partiellement)
            year = current_start.year
            month = current_start.month
            day = current_start.day
            object_path = f"openalex/year={year}/month={month:02d}/day={day:02d}/works_page_{page_counter:04d}.json"
            
            metadata = {
                "source_system": "openalex",
                "crawl_timestamp": datetime.now().isoformat(),
                "filter": filter_query,
                "page": page_counter,
                "content_hash": compute_hash(json.dumps(data, indent=2).encode())
            }
            upload_bytes(bucket_raw_json, object_path, json.dumps(data, indent=2).encode(), metadata)
            
            total_works += len(results)
            print(f"Page {page_counter} de la semaine {current_start.date()} : {len(results)} œuvres → total {total_works}/{max_works}")
            
            # Vérifier si la limite est atteinte ou dépassée
            if total_works >= max_works:
                all_done = True
                break
            
            # Passage à la page suivante
            if 'meta' in data and 'next_cursor' in data['meta'] and data['meta']['next_cursor']:
                params['cursor'] = data['meta']['next_cursor']
                page_counter += 1
            else:
                break
        
        current_start = current_end + timedelta(days=1)
    
    print(f"✅ Ingestion terminée pour la période {start_date.date()} → {end_date.date()} : {total_works} œuvres ingérées (limite {max_works}).")
    return f"Ingested {total_works} works (max {max_works})"