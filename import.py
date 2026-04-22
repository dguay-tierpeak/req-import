import csv, json, urllib.request, os, time, zipfile, shutil

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://pfdfwvcqkmakgzhgswip.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBmZGZ3dmNxa21ha2d6aGdzd2lwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY3ODQyNTksImV4cCI6MjA5MjM2MDI1OX0.5Sc8mSof8C6M_6RtaBtU39mv0YsJGsJ9UmxFfLt7L3Q")

CKAN_API    = "https://www.donneesquebec.ca/recherche/api/3/action/package_show?id=registre-des-entreprises"
DOWNLOAD_DIR = "/tmp/req_data"
CSV_DIR      = DOWNLOAD_DIR

FORME_JURI = {
    "CIE": "Société par actions", "IND": "Entreprise individuelle",
    "APE": "OSBL", "SENC": "Société en nom collectif", "COP": "Coopérative",
    "SEC": "Société en commandite", "ASS": "Association", "AU": "Autre"
}
EMPLOYES = {
    "A": "1-5", "B": "6-10", "C": "11-25", "D": "26-49", "E": "50-99",
    "F": "100-249", "G": "250-499", "H": "500-749", "I": "750-999",
    "J": "1000-2499", "K": "2500-4999", "L": "5000+", "O": "Aucun", "N": ""
}

def download_data():
    print("Recherche du fichier ZIP sur Données Québec...")
    with urllib.request.urlopen(CKAN_API) as r:
        pkg = json.loads(r.read())

    zip_url = None
    for resource in pkg["result"]["resources"]:
        if resource.get("format", "").upper() == "ZIP":
            zip_url = resource["url"]
            break

    if not zip_url:
        raise Exception("Aucun fichier ZIP trouvé dans le dataset")

    print(f"Téléchargement depuis : {zip_url}")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    zip_path = f"{DOWNLOAD_DIR}/req.zip"

    urllib.request.urlretrieve(zip_url, zip_path)
    print("Téléchargement terminé. Extraction...")

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DOWNLOAD_DIR)

    os.remove(zip_path)
    print("Extraction terminée.")

def upsert_table(table, batch):
    data = json.dumps(batch).encode("utf-8")
    for attempt in range(5):
        try:
            req = urllib.request.Request(
                f"{SUPABASE_URL}/rest/v1/{table}",
                data=data,
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates"
                },
                method="POST"
            )
            urllib.request.urlopen(req)
            return
        except Exception as e:
            print(f"  Erreur (tentative {attempt+1}/5): {e}")
            time.sleep(3 * (attempt + 1))
    print("  Batch ignoré après 5 tentatives")

# --- Téléchargement ---
download_data()

# --- Import noms ---
print("Import des noms actifs...")
batch_noms, total_noms = [], 0
with open(f"{CSV_DIR}/Nom.csv", encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
        if row["STAT_NOM"] == "V" and row["NOM_ASSUJ"].strip():
            batch_noms.append({
                "neq": row["NEQ"],
                "nom": row["NOM_ASSUJ"].strip(),
                "type_nom": row["TYP_NOM_ASSUJ"],
            })
            if len(batch_noms) == 500:
                upsert_table("noms", batch_noms)
                total_noms += len(batch_noms)
                batch_noms = []
                if total_noms % 50000 == 0:
                    print(f"  {total_noms} noms importés...")
if batch_noms:
    upsert_table("noms", batch_noms)
    total_noms += len(batch_noms)
print(f"{total_noms} noms importés")

# --- Import entreprises ---
print("Import des entreprises actives...")
noms_principaux = {}
with open(f"{CSV_DIR}/Nom.csv", encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
        if row["STAT_NOM"] == "V" and row["TYP_NOM_ASSUJ"] == "N":
            noms_principaux[row["NEQ"]] = row["NOM_ASSUJ"].strip()

batch, total = [], 0
with open(f"{CSV_DIR}/Entreprise.csv", encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
        if row["COD_STAT_IMMAT"] != "IM":
            continue
        batch.append({
            "neq": row["NEQ"],
            "nom": noms_principaux.get(row["NEQ"], ""),
            "forme_juridique": FORME_JURI.get(row["COD_FORME_JURI"], row["COD_FORME_JURI"]),
            "nb_employes": EMPLOYES.get(row["COD_INTVAL_EMPLO_QUE"], ""),
            "code_activite": row["COD_ACT_ECON_CAE"],
            "activite_economique": row["DESC_ACT_ECON_ASSUJ"],
            "dat_immat": row["DAT_IMMAT"] or None,
            "adresse": row["ADR_DOMCL_LIGN1_ADR"],
            "ville_province": row["ADR_DOMCL_LIGN2_ADR"],
            "code_postal": row["ADR_DOMCL_LIGN4_ADR"],
        })
        if len(batch) == 500:
            upsert_table("entreprises", batch)
            total += len(batch)
            batch = []
            if total % 50000 == 0:
                print(f"  {total} importées...")

if batch:
    upsert_table("entreprises", batch)
    total += len(batch)

# --- Nettoyage ---
shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
print(f"Terminé — {total} entreprises importées dans Supabase")
