import asyncio, os, zipfile
from playwright.async_api import async_playwright

DOWNLOAD_URL = "https://www.registreentreprises.gouv.qc.ca/RQAnonymeGR/GR/GR03/GR03A2_22A_PIU_RecupDonnPub_PC/FichierDonneesOuvertes.aspx"
DOWNLOAD_DIR = "/tmp/req_data"

async def download_with_browser():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        print("Navigation vers la page de téléchargement...")
        await page.goto(DOWNLOAD_URL, wait_until="networkidle", timeout=60000)

        # Cocher la case de consentement si présente
        try:
            checkboxes = await page.locator("input[type='checkbox']").all()
            for checkbox in checkboxes:
                if not await checkbox.is_checked():
                    await checkbox.check()
                    print("Case de consentement cochée")
        except Exception as e:
            print(f"Pas de case à cocher : {e}")

        # Cliquer sur le bouton de téléchargement
        print("Clic sur le bouton de téléchargement...")
        try:
            async with page.expect_download(timeout=120000) as download_info:
                try:
                    await page.locator("input[type='submit']").first.click()
                except:
                    await page.locator("button[type='submit']").first.click()
            download = await download_info.value
            zip_path = f"{DOWNLOAD_DIR}/JeuDonnees.zip"
            await download.save_as(zip_path)
            print(f"Fichier téléchargé : {zip_path}")
        except Exception as e:
            # Prendre un screenshot pour déboguer si erreur
            await page.screenshot(path="debug.png")
            print(f"Erreur lors du téléchargement : {e}")
            raise

        await browser.close()

    # Extraction
    print("Extraction du ZIP...")
    with zipfile.ZipFile(f"{DOWNLOAD_DIR}/JeuDonnees.zip", "r") as z:
        z.extractall(DOWNLOAD_DIR)
    os.remove(f"{DOWNLOAD_DIR}/JeuDonnees.zip")
    print("Extraction terminée.")

asyncio.run(download_with_browser())
