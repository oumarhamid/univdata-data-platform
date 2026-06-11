import json
import requests
import time
import hashlib
from urllib.parse import urljoin, urlparse, urldefrag, quote
from collections import deque
from datetime import datetime
from bs4 import BeautifulSoup
from .minio_client import upload_bytes, compute_hash
from playwright.sync_api import sync_playwright

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)

def to_ascii_metadata(value: str) -> str:
    """Convertit une chaîne en ASCII sûr pour les métadonnées MinIO."""
    return quote(value, safe='/:?=&')

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

def bfs_scrape_with_playwright(config_path, **context):
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
    file_extensions = config.get('file_extensions', [])
    image_extensions = config.get('image_extensions', [])
    max_pages = config.get('max_pages_per_domain', 1000)

    visited_urls = set()
    queue = deque([(start_url, 0)])
    total_pages = 0

    # Ajout des seed_urls si présentes
    seed_urls = config.get('seed_urls', [])
    for url in seed_urls:
        if url not in visited_urls:
            queue.append((url, 1))
            print(f"[DEBUG] Seed URL ajoutée (depth 1) : {url}")

    print(f"[DEBUG] Démarrage scraping avec Playwright pour {domain} depuis {start_url}, profondeur max {max_depth}")

    session = requests.Session()
    session.headers.update({'User-Agent': user_agent})

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()

        while queue and total_pages < max_pages:
            url, depth = queue.popleft()
            url, _ = urldefrag(url)
            if url in visited_urls:
                continue
            if depth > max_depth:
                continue

            print(f"[DEBUG] Traitement de {url} (depth {depth})")
            visited_urls.add(url)
            total_pages += 1

            ext_from_url = get_file_extension(url, '')
            is_file = ext_from_url in file_extensions or ext_from_url in image_extensions

            try:
                if is_file:
                    # Télécharger avec requests
                    resp = session.get(url, timeout=15)
                    resp.raise_for_status()
                    data_bytes = resp.content
                    content_type = resp.headers.get('Content-Type', '').lower()
                    timestamp = datetime.now().isoformat()
                    checksum = compute_hash(data_bytes)
                    metadata = {
                        "source_url": to_ascii_metadata(url),
                        "depth": depth,
                        "crawl_timestamp": timestamp,
                        "content_hash": checksum,
                        "domain": domain,
                        "http_status": resp.status_code,
                        "content_type": content_type
                    }
                    if ext_from_url in image_extensions:
                        object_name = f"{domain}/images/{hashlib.md5(url.encode()).hexdigest()}.{ext_from_url}"
                        upload_bytes(images_bucket, object_name, data_bytes, metadata)
                        print(f"[DEBUG] Image sauvegardée : {object_name}")
                    else:
                        object_name = f"{domain}/files/{hashlib.md5(url.encode()).hexdigest()}.{ext_from_url}"
                        upload_bytes(docs_bucket, object_name, data_bytes, metadata)
                        print(f"[DEBUG] Document sauvegardé : {object_name}")
                else:
                    # Page HTML : utiliser Playwright
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    data_bytes = page.content().encode('utf-8')
                    content_type = 'text/html'
                    timestamp = datetime.now().isoformat()
                    checksum = compute_hash(data_bytes)
                    metadata = {
                        "source_url": to_ascii_metadata(url),
                        "depth": depth,
                        "crawl_timestamp": timestamp,
                        "content_hash": checksum,
                        "domain": domain,
                        "http_status": 200,
                        "content_type": content_type
                    }
                    parsed = urlparse(url)
                    path_part = parsed.path if parsed.path else "index"
                    if path_part.endswith('/'):
                        path_part += "index"
                    object_name = f"{domain}/depth={depth}/{parsed.netloc}{path_part.replace('/', '_')}.html"
                    upload_bytes(html_bucket, object_name, data_bytes, metadata)
                    print(f"[DEBUG] HTML sauvegardé : {object_name}")

                    html_rendered = page.content()
                    links_found = 0
                    for link in extract_links_from_html(html_rendered, url):
                        if not is_internal(link, domain):
                            continue
                        if link not in visited_urls:
                            queue.append((link, depth + 1))
                            links_found += 1
                            print(f"[DEBUG] Nouveau lien ajouté (depth {depth+1}) : {link}")
                    print(f"[DEBUG] {links_found} nouveaux liens internes trouvés dans {url}")

                time.sleep(rate_limit)

            except Exception as e:
                error_msg = f"URL: {url}\nError: {str(e)}\nDepth: {depth}"
                print(f"[ERROR] {error_msg}")
                error_data = error_msg.encode()
                error_meta = {
                    "source_url": to_ascii_metadata(url),
                    "depth": depth,
                    "error": to_ascii_metadata(str(e)),
                    "crawl_timestamp": datetime.now().isoformat()
                }
                upload_bytes(logs_bucket, f"errors/{hashlib.md5(url.encode()).hexdigest()}.txt", error_data, error_meta)

        browser.close()

    print(f"[DEBUG] Scraping terminé pour {domain}, {len(visited_urls)} URLs traitées (limite {max_pages})")
    return f"Scraping terminé pour {domain}, {len(visited_urls)} URLs traitées (limite {max_pages})"