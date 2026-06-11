import json
import requests
import time
import hashlib
from urllib.parse import urljoin, urlparse, urldefrag
from collections import deque
from datetime import datetime
from bs4 import BeautifulSoup
from .minio_client import upload_bytes, compute_hash

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)

def is_internal(url, domain):
    """Vérifie si l'URL appartient au domaine donné, en ignorant www."""
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

def extract_links(soup, base_url):
    """Extrait toutes les URLs depuis les attributs de liens courants (href, data-href, ng-href, :href, src, etc.)"""
    links = set()
    # Attributs potentiels contenant une URL
    attrs = ['href', 'data-href', 'ng-href', ':href', 'v-bind:href', 'xlink:href', 'src', 'data-src']
    # Balises à inspecter
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

def bfs_scrape_from_config(config_path, **context):
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
    session = requests.Session()
    session.headers.update({'User-Agent': user_agent})
    total_pages = 0

    print(f"[DEBUG] Démarrage scraping pour {domain} depuis {start_url}, profondeur max {max_depth}")

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

        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            content_type = resp.headers.get('Content-Type', '').lower()
            data_bytes = resp.content
            timestamp = datetime.now().isoformat()
            checksum = compute_hash(data_bytes)
            metadata = {
                "source_url": url,
                "depth": depth,
                "crawl_timestamp": timestamp,
                "content_hash": checksum,
                "domain": domain,
                "http_status": resp.status_code,
                "content_type": content_type
            }

            is_html = 'text/html' in content_type
            ext = get_file_extension(url, content_type)

            if is_html:
                parsed = urlparse(url)
                path_part = parsed.path if parsed.path else "index"
                if path_part.endswith('/'):
                    path_part += "index"
                object_name = f"{domain}/depth={depth}/{parsed.netloc}{path_part.replace('/', '_')}.html"
                upload_bytes(html_bucket, object_name, data_bytes, metadata)
                print(f"[DEBUG] HTML sauvegardé : {object_name}")

                soup = BeautifulSoup(resp.text, 'html.parser')
                links_found = 0
                for link in extract_links(soup, url):
                    if not is_internal(link, domain):
                        print(f"[DEBUG] Lien externe ignoré : {link}")
                        continue
                    if link not in visited_urls:
                        queue.append((link, depth + 1))
                        links_found += 1
                        print(f"[DEBUG] Nouveau lien ajouté (depth {depth+1}) : {link}")
                print(f"[DEBUG] {links_found} nouveaux liens internes trouvés dans {url}")

            else:
                if ext in image_extensions:
                    object_name = f"{domain}/images/{hashlib.md5(url.encode()).hexdigest()}.{ext}"
                    upload_bytes(images_bucket, object_name, data_bytes, metadata)
                    print(f"[DEBUG] Image sauvegardée : {object_name}")
                elif ext in file_extensions:
                    object_name = f"{domain}/files/{hashlib.md5(url.encode()).hexdigest()}.{ext}"
                    upload_bytes(docs_bucket, object_name, data_bytes, metadata)
                    print(f"[DEBUG] Document sauvegardé : {object_name}")
                else:
                    object_name = f"{domain}/others/{hashlib.md5(url.encode()).hexdigest()}.bin"
                    upload_bytes(logs_bucket, object_name, data_bytes, metadata)
                    print(f"[DEBUG] Autre fichier sauvegardé : {object_name}")

            time.sleep(rate_limit)

        except Exception as e:
            error_msg = f"URL: {url}\nError: {str(e)}\nDepth: {depth}"
            print(f"[ERROR] {error_msg}")
            error_data = error_msg.encode()
            error_meta = {"source_url": url, "depth": depth, "error": str(e), "crawl_timestamp": datetime.now().isoformat()}
            upload_bytes(logs_bucket, f"errors/{hashlib.md5(url.encode()).hexdigest()}.txt", error_data, error_meta)

    print(f"[DEBUG] Scraping terminé pour {domain}, {len(visited_urls)} URLs traitées (limite {max_pages})")
    return f"Scraping terminé pour {domain}, {len(visited_urls)} URLs traitées (limite {max_pages})"