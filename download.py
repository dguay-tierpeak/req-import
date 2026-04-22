from playwright.sync_api import sync_playwright
import os, zipfile

DOWNLOAD_URL = "https://www.registreentreprises.gouv.qc.ca/RQAnonymeGR/GR/GR03/GR03A2_22A_PIU_RecupDonnPub_PC/FichierDonneesOuvertes.aspx"
DOWNLOAD_DIR = "/tmp/req_data"

def download_with_browser():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        print("Navigation vers la page de téléchargement...")
        page.goto(DOWNLOAD_URL, wait_until="networkidle", timeout=60000)

        # Cocher la case de consentement si présente
        try:
            checkboxes = page.locator("input[type='checkbox']").all()
            for checkbox in checkboxes:
                if not checkbox.is_checked():
                    checkbox.check()
                    print("Case de consentement cochée")
        except Exception as e:
            print(f"Pas de case à cocher : {e}")

        # Cliquer sur le bouton de téléchargement
        print("Clic sur le bouton de téléchargement...")
        with page.expect_download(timeout=120000) as download_info:
            try:
                page.locator("input[type='submit']").first.click()
            except:
                page.locator("button[type='submit']").first.click()

        download = download_info.value
        zip_path = f"{DOWNLOAD_DIR}/JeuDonnees.zip"
        download.save_as(zip_path)
        print(f"Fichier téléchargé : {zip_path}")

        browser.close()

    # Extraction
    print("Extraction du ZIP...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DOWNLOAD_DIR)
    os.remove(zip_path)
    print("Extraction terminée.")

download_with_browser()
