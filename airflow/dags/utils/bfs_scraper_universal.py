
import json
import requests
import time
import hashlib
from urllib.parse import urljoin, urlparse, urldefrag, quote
from collections import deque
from datetime import datetime
from bs4 import BeautifulSoup
from .minio_client import upload_bytes, compute_hash

# Playwright optionnel
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)


def to_ascii_metadata(value: str) -> str:
    return quote(value, safe='/:?=&')


def save_scraping_error(logs_bucket, url, depth, error_message):
    """Enregistre une erreur de collecte sans masquer l'erreur initiale."""
    try:
        error_meta = {
            "source_url": to_ascii_metadata(url),
            "depth": depth,
            "error": to_ascii_metadata(error_message),
            "crawl_timestamp": datetime.now().isoformat(),
        }
        object_path = f"errors/{hashlib.md5(url.encode()).hexdigest()}.txt"
        upload_bytes(logs_bucket, object_path, error_message.encode('utf-8'), error_meta)
    except Exception as log_error:
        print(f"[WARN] Journal MinIO non enregistré pour {url}: {log_error}")


def is_internal(url, domain):
    netloc = urlparse(url).netloc.lower()
    domain_clean = domain.lower().replace('www.', '')
    netloc_clean = netloc.replace('www.', '')
    return netloc_clean == domain_clean


def get_file_extension(url, content_type):
    path = urlparse(url).path
    if '.' in path.split('/')[-1]:
        ext = path.split('.')[-1].split('?')[0].lower()
        if ext:
            return ext
    if '/' in content_type:
        return content_type.split('/')[-1].split(';')[0]
    return 'bin'


def extract_links_from_html(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    attrs = ['href', 'data-href', 'ng-href', ':href', 'v-bind:href', 'xlink:href', 'src', 'data-src']
    tags = soup.find_all(['a', 'link', 'area', 'base', 'img', 'script', 'iframe', 'source'])
    for tag in tags:
        for attr in attrs:
            if tag.has_attr(attr):
                url = tag[attr]
                if url and not url.startswith('#') and not url.startswith('javascript:'):
                    full_url = urljoin(base_url, url)
                    full_url, _ = urldefrag(full_url)
                    if full_url:
                        links.add(full_url)
    return links


def sanitize_filename(url: str) -> str:
    """Transforme une URL en nom de fichier valide (sans /, ?, =, etc.)"""
    # Supprime le protocole
    if url.startswith('https://'):
        url = url[8:]
    elif url.startswith('http://'):
        url = url[7:]
    # Remplacer tous les caractères non alphanumériques par _
    import re
    safe = re.sub(r'[^a-zA-Z0-9]', '_', url)
    # Limiter la longueur (optionnel)
    if len(safe) > 200:
        safe = safe[:200]
    return safe + '.html'


def fetch_with_playwright(url, user_agent, timeout=60000, wait_for_selector=None):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=timeout)
        if wait_for_selector:
            try:
                page.wait_for_selector(wait_for_selector, timeout=10000)
            except Exception as e:
                print(f"[WARN] Le sélecteur {wait_for_selector} n'est pas apparu pour {url}: {e}")
        html = page.content().encode('utf-8')
        browser.close()
        return html, 'text/html', 200


def fetch_with_requests(url, user_agent, timeout=30):
    resp = requests.get(url, headers={'User-Agent': user_agent}, timeout=timeout)
    resp.raise_for_status()
    return resp.content, resp.headers.get('Content-Type', '').lower(), resp.status_code


def bfs_scrape_universal(config_path, **context):
    config = load_config(config_path)
    domain = config['domain']
    start_url = config['start_url']
    max_depth = config.get('max_depth', 5)
    rate_limit = config.get('rate_limit_seconds', 1)
    user_agent = config.get('user_agent', 'UnivDataBot/1.0')
    buckets = config.get('buckets', {})
    html_bucket = buckets.get('html', 'raw-web-html')
    docs_bucket = buckets.get('documents', 'raw-documents')
    images_bucket = buckets.get('images', 'raw-images')
    logs_bucket = buckets.get('logs', 'raw-logs')
    file_extensions = set(config.get('file_extensions', []))
    image_extensions = set(config.get('image_extensions', []))
    max_pages = config.get('max_pages_per_domain', 1000)
    use_playwright = config.get('use_playwright', False)
    wait_for_selector = config.get('wait_for_selector', None)   # nouveau
    min_html_size = config.get('min_html_size', 5000)           # seuil en bytes

    visited_urls = set()
    queue = deque([(start_url, 0)])
    total_pages = 0

    seed_urls = config.get('seed_urls', [])
    for url in seed_urls:
        if url not in visited_urls:
            queue.append((url, 1))
            print(f"[DEBUG] Seed URL ajoutée (depth 1) : {url}")

    print(f"[DEBUG] Démarrage scraping pour {domain} (Playwright={use_playwright}) depuis {start_url}, profondeur max {max_depth}")

    session = requests.Session()
    session.headers.update({'User-Agent': user_agent})

    # Préfixe temporel pour le partitionnement (année/mois/jour)
    now = datetime.now()
    time_prefix = now.strftime("%Y/%m/%d")

    while queue and total_pages < max_pages:
        url, depth = queue.popleft()
        url, _ = urldefrag(url)
        if url in visited_urls or depth > max_depth:
            continue

        print(f"[DEBUG] Traitement de {url} (depth {depth})")
        visited_urls.add(url)
        total_pages += 1

        ext = get_file_extension(url, '')
        is_file = ext in file_extensions or ext in image_extensions

        if is_file:
            try:
                data, content_type, http_status = fetch_with_requests(url, user_agent, timeout=15)
                bucket = images_bucket if ext in image_extensions else docs_bucket
                # Pour les fichiers, on garde un hash dans le nom, mais on ajoute le partitionnement temporel
                object_name = f"{domain}/{time_prefix}/{'images' if ext in image_extensions else 'files'}/{hashlib.md5(url.encode()).hexdigest()}.{ext}"
            except Exception as e:
                print(f"[ERROR] Échec fichier {url}: {e}")
                save_scraping_error(logs_bucket, url, depth, str(e))
                continue
        else:
            # Page HTML
            success = False
            data = None
            content_type = None
            http_status = None
            if use_playwright and PLAYWRIGHT_AVAILABLE:
                try:
                    data, content_type, http_status = fetch_with_playwright(url, user_agent, timeout=60000, wait_for_selector=wait_for_selector)
                    success = True
                    print(f"[DEBUG] Playwright réussi pour {url}")
                except Exception as e:
                    print(f"[WARN] Playwright échoué pour {url}: {e}. Fallback vers requests.")
                    try:
                        data, content_type, http_status = fetch_with_requests(url, user_agent, timeout=30)
                        success = True
                        print(f"[DEBUG] Fallback requests réussi pour {url}")
                    except Exception as e2:
                        print(f"[ERROR] Fallback requests également échoué pour {url}: {e2}")
            else:
                try:
                    data, content_type, http_status = fetch_with_requests(url, user_agent, timeout=30)
                    success = True
                except Exception as e:
                    print(f"[ERROR] Échec requests pour {url}: {e}")

            if not success:
                save_scraping_error(logs_bucket, url, depth, "All methods failed")
                continue

            # Vérifier la taille minimale (ignorer les fichiers trop petits)
            if len(data) < min_html_size:
                print(f"[WARN] Fichier HTML trop petit ({len(data)} bytes) pour {url}, non sauvegardé")
                continue

            bucket = html_bucket
            # Nom de fichier unique basé sur l'URL complète (y compris paramètres)
            safe_filename = sanitize_filename(url)
            object_name = f"{domain}/{time_prefix}/depth={depth}/{safe_filename}"

        # Métadonnées et upload
        timestamp = datetime.now().isoformat()
        checksum = compute_hash(data)
        metadata = {
            "source_url": to_ascii_metadata(url),
            "depth": depth,
            "crawl_timestamp": timestamp,
            "content_hash": checksum,
            "domain": domain,
            "http_status": http_status,
            "content_type": content_type
        }
        upload_bytes(bucket, object_name, data, metadata)
        print(f"[DEBUG] Sauvegardé : {object_name}")

        # Extraction des liens (uniquement pour HTML)
        if not is_file and content_type and 'text/html' in content_type:
            html_str = data.decode('utf-8', errors='ignore')
            links_found = 0
            for link in extract_links_from_html(html_str, url):
                if not is_internal(link, domain):
                    continue
                if link not in visited_urls:
                    queue.append((link, depth + 1))
                    links_found += 1
                    print(f"[DEBUG] Nouveau lien ajouté (depth {depth+1}) : {link}")
            print(f"[DEBUG] {links_found} nouveaux liens internes trouvés dans {url}")

        time.sleep(rate_limit)

    print(f"[DEBUG] Scraping terminé pour {domain}, {len(visited_urls)} URLs traitées (limite {max_pages})")
    return f"Scraping terminé pour {domain}, {len(visited_urls)} URLs traitées (limite {max_pages})"