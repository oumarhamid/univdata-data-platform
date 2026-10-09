import json
import os
import time
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests

from .minio_client import compute_hash, upload_bytes


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _retry_after_seconds(response):
    """Retourne le délai Retry-After en secondes s'il est exploitable."""
    retry_after = response.headers.get("Retry-After")
    if not retry_after:
        return None

    try:
        return max(float(retry_after), 0.0)
    except ValueError:
        try:
            retry_date = parsedate_to_datetime(retry_after)
            if retry_date.tzinfo is None:
                return None

            now = datetime.now(retry_date.tzinfo)
            return max((retry_date - now).total_seconds(), 0.0)
        except (TypeError, ValueError, OverflowError):
            return None


def _get_with_retry(
    session,
    url,
    params,
    max_retries=5,
    timeout=(10, 60),
):
    """
    Effectue un appel HTTP robuste.

    Retry sur :
    - 429 Too Many Requests
    - erreurs serveur 5xx temporaires
    - erreurs réseau temporaires

    Respecte Retry-After lorsqu'il est fourni.
    """
    for attempt in range(max_retries + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt >= max_retries:
                raise

            delay = min(2 * (2 ** attempt), 60)
            print(
                f"[Crossref] Erreur réseau : {exc}. "
                f"Nouvelle tentative dans {delay:.0f}s "
                f"({attempt + 1}/{max_retries})."
            )
            time.sleep(delay)
            continue

        if response.status_code not in RETRYABLE_STATUS_CODES:
            response.raise_for_status()
            return response

        if attempt >= max_retries:
            response.raise_for_status()

        retry_after = _retry_after_seconds(response)
        delay = (
            retry_after
            if retry_after is not None
            else min(2 * (2 ** attempt), 60)
        )

        print(
            f"[Crossref] HTTP {response.status_code}. "
            f"Nouvelle tentative dans {delay:.1f}s "
            f"({attempt + 1}/{max_retries})."
        )
        time.sleep(delay)

    raise RuntimeError("Échec inattendu lors de l'appel Crossref.")


def ingest_crossref(bucket_raw_json="raw-json", max_works=2000, **context):
    """
    Ingère les métadonnées des publications depuis Crossref.

    Pagination par curseur (rows + cursor).
    Partitionnement year/month/day.
    Gestion du rate limiting et des erreurs HTTP temporaires.
    """
    base_url = "https://api.crossref.org/works"

    rows = 100
    cursor = "*"
    total_works = 0
    page_counter = 1

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    request_interval = float(
        os.getenv("CROSSREF_REQUEST_INTERVAL_SECONDS", "1.25")
    )
    max_retries = int(os.getenv("CROSSREF_MAX_RETRIES", "5"))

    mailto = os.getenv("CROSSREF_MAILTO", "").strip()
    user_agent = os.getenv(
        "CROSSREF_USER_AGENT",
        "UnivData-Data-Platform/1.0",
    ).strip()

    if mailto:
        user_agent = f"{user_agent} (mailto:{mailto})"

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }
    )

    params = {
        "rows": rows,
        "cursor": cursor,
        "filter": "from-pub-date:2026-01-01,until-pub-date:2026-12-31",
    }

    if mailto:
        params["mailto"] = mailto

    while True:
        response = _get_with_retry(
            session=session,
            url=base_url,
            params=params,
            max_retries=max_retries,
        )

        data = response.json()
        items = data.get("message", {}).get("items", [])

        if not items:
            break

        object_path = (
            f"crossref/year={year}/month={month:02d}/day={day:02d}/"
            f"works_page_{page_counter:04d}.json"
        )

        payload = json.dumps(data, indent=2).encode()

        metadata = {
            "source_system": "crossref",
            "crawl_timestamp": now.isoformat(),
            "page": page_counter,
            "content_hash": compute_hash(payload),
        }

        upload_bytes(
            bucket_raw_json,
            object_path,
            payload,
            metadata,
        )

        total_works += len(items)

        print(
            f"[Crossref] Page {page_counter}: "
            f"{len(items)} œuvres → total "
            f"{total_works}/{max_works}"
        )

        if total_works >= max_works:
            break

        next_cursor = data.get("message", {}).get("next-cursor")

        if not next_cursor:
            break

        params["cursor"] = next_cursor
        page_counter += 1

        if request_interval > 0:
            time.sleep(request_interval)

    session.close()

    print(
        f"✅ Crossref terminé : {total_works} œuvres ingérées."
    )

    return f"Crossref ingested {total_works} works"