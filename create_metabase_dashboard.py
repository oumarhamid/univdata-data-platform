import requests
import time
import sys

METABASE_URL = "http://localhost:3000"
ADMIN_EMAIL = "admin@example.com"
ADMIN_PASSWORD = "admin123@"
PG_DATABASE_NAME = "Metabase PostgreSQL"   # Nom exact de la base

def wait_for_metabase():
    print("⏳ Attente du démarrage de Metabase...")
    while True:
        try:
            r = requests.get(f"{METABASE_URL}/api/health")
            if r.status_code == 200:
                print("✅ Metabase est prêt")
                return
        except:
            pass
        time.sleep(5)

def login():
    login_data = {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    resp = requests.post(f"{METABASE_URL}/api/session", json=login_data)
    if resp.status_code == 200:
        return resp.json()["id"]
    return None

def get_databases(token):
    headers = {"X-Metabase-Session": token}
    resp = requests.get(f"{METABASE_URL}/api/database", headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "data" in data:
            return data["data"]
    return []

def get_database_id_by_name(token, name):
    dbs = get_databases(token)
    for db in dbs:
        if isinstance(db, dict) and db.get("name") == name:
            return db["id"]
    return None

def get_existing_cards(token):
    """Liste TOUTES les questions existantes (non déjà archivées), y compris
    les doublons et les questions orphelines qui ne sont plus dans la liste
    `queries` actuelle (ex: anciennes questions supprimées du script mais
    jamais nettoyées dans Metabase)."""
    headers = {"X-Metabase-Session": token}
    resp = requests.get(f"{METABASE_URL}/api/card", headers=headers)
    cards = []
    if resp.status_code == 200:
        data = resp.json()
        items = data if isinstance(data, list) else data.get("data", [])
        for item in items:
            if isinstance(item, dict) and not item.get("archived"):
                cards.append({"id": item.get("id"), "name": item.get("name")})
    return cards


def archive_card(token, card_id):
    headers = {"X-Metabase-Session": token}
    requests.put(f"{METABASE_URL}/api/card/{card_id}", json={"archived": True}, headers=headers)


def archive_all_existing_cards(token):
    """Archive TOUTES les questions existantes avant de recréer le tableau
    de bord depuis zéro : plus de doublons accumulés d'un run cassé à
    l'autre, plus de questions orphelines référençant des colonnes qui
    n'existent plus dans le schéma actuel."""
    cards = get_existing_cards(token)
    if not cards:
        print("ℹ️  Aucune question existante à nettoyer.")
        return
    print(f"🧹 Nettoyage : archivage de {len(cards)} question(s) existante(s)...")
    for card in cards:
        archive_card(token, card["id"])
        print(f"   🗑️  Archivée : '{card['name']}' (id={card['id']})")
    print("✅ Nettoyage terminé.")


def create_question(token, db_id, name, query):
    headers = {"X-Metabase-Session": token}
    card_data = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": query},
            "database": db_id
        },
        "display": "table",
        "visualization_settings": {}
    }
    resp = requests.post(f"{METABASE_URL}/api/card", json=card_data, headers=headers)
    if resp.status_code == 200:
        print(f"✅ Question '{name}' créée")
        return resp.json()["id"]
    else:
        if "table not found" in resp.text.lower() or "does not exist" in resp.text.lower():
            print(f"⚠️  Table manquante pour '{name}', question ignorée.")
        else:
            print(f"❌ Erreur création question '{name}': {resp.text}")
        return None

def main():
    wait_for_metabase()
    token = login()
    if not token:
        print("❌ Échec de connexion. Vérifiez les identifiants admin.")
        sys.exit(1)

    db_id = get_database_id_by_name(token, PG_DATABASE_NAME)
    if not db_id:
        print(f"❌ Base PostgreSQL '{PG_DATABASE_NAME}' introuvable.")
        print("   Ajoutez-la manuellement (Admin → Databases) ou vérifiez le nom.")
        sys.exit(1)
    print(f"✅ Base PostgreSQL trouvée (id={db_id})")

    # Définir les requêtes pour les tables exportées vers PostgreSQL
    queries = {
        # Faculty
        "Total professeurs": "SELECT COUNT(*) as total FROM metabase_faculty_profiles",
        "Professeurs par université": "SELECT university, COUNT(*) as nb FROM metabase_faculty_profiles GROUP BY university ORDER BY nb DESC",
        "Professeurs par département": "SELECT department, COUNT(*) as nb FROM metabase_faculty_profiles WHERE department IS NOT NULL AND department != '' GROUP BY department ORDER BY nb DESC",

        # Publications
        "Total publications": "SELECT COUNT(*) as total FROM metabase_publications",
        "Publications par source": "SELECT source_system, COUNT(*) as nb FROM metabase_publications GROUP BY source_system ORDER BY nb DESC",
        "Publications par année": "SELECT EXTRACT(YEAR FROM publication_date) as annee, COUNT(*) as nb FROM metabase_publications WHERE publication_date IS NOT NULL GROUP BY annee ORDER BY annee DESC",

        # Programs
        "Total formations": "SELECT COUNT(*) as total FROM metabase_programs",
        "Formations par niveau": "SELECT level, COUNT(*) as nb FROM metabase_programs GROUP BY level ORDER BY nb DESC",

        # Documents PDF
        "Total documents PDF": "SELECT COUNT(*) as total FROM metabase_documents_catalog",

        "Documents PDF par type": """
        SELECT document_type, COUNT(*) as nb
        FROM metabase_documents_catalog
        GROUP BY document_type
        ORDER BY nb DESC
        """,

        "Statut OCR des PDF": """
        SELECT
          CASE
            WHEN needs_ocr = true THEN 'OCR nécessaire'
            ELSE 'Texte extrait'
          END AS statut_ocr,
          COUNT(*) AS nb
        FROM metabase_documents_catalog
        GROUP BY needs_ocr
        """,

        "Score moyen extraction PDF": """
        SELECT ROUND(AVG(extraction_quality_score)::numeric, 2) AS score_moyen
        FROM metabase_documents_catalog
        """,

        "Catalogue des documents PDF": """
        SELECT
          file_name,
          document_name,
          document_type,
          program_name,
          academic_year,
          semester,
          needs_ocr,
          extraction_quality_score
        FROM metabase_documents_catalog
        ORDER BY extraction_quality_score DESC
        """,

        # --- 3 nouveaux calculs ---

        "Professeurs par titre": """
        SELECT title, COUNT(*) as nb
        FROM metabase_faculty_profiles
        WHERE title IS NOT NULL AND title != ''
        GROUP BY title
        ORDER BY nb DESC
        """,

        "Taux de documents nécessitant OCR (%)": """
        SELECT
          ROUND(
            100.0 * SUM(CASE WHEN needs_ocr = true THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            1
          ) AS pct_needs_ocr
        FROM metabase_documents_catalog
        """,

        "Documents par formation détectée": """
        SELECT program_name, COUNT(*) as nb
        FROM metabase_documents_catalog
        WHERE program_name IS NOT NULL AND program_name != ''
        GROUP BY program_name
        ORDER BY nb DESC
        """,
    }

    created = 0
    archive_all_existing_cards(token)
    for name, q in queries.items():
        cid = create_question(token, db_id, name, q)
        if cid:
            created += 1

    print(f"\n🎉 {created} questions créées avec succès.")
    print("👉 Rendez-vous dans Metabase, ouvrez votre dashboard existant,")
    print("   puis ajoutez manuellement les nouvelles questions si nécessaire.")

if __name__ == "__main__":
    main()