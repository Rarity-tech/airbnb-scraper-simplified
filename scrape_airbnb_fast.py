"""
Scraper Airbnb - Extraction HTML directe
Reproduction exacte des conditions navigateur
"""
import asyncio, csv, re, datetime, os
from urllib.parse import urljoin
from playwright.async_api import async_playwright

# Configuration
SEARCH_URLS_FILE = "search_urls.txt"
OUTPUT_CSV = "airbnb_hosts_data.csv"
MAX_WORKERS = 4  # Contextes parallèles
MAX_LISTINGS_PER_PAGE = int(os.getenv("MAX_LISTINGS", "50"))

def now_iso():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

def read_search_urls():
    with open(SEARCH_URLS_FILE, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

async def create_browser_context(playwright):
    """Crée un contexte navigateur avec configuration anti-détection"""
    browser = await playwright.firefox.launch(
        headless=True,
        firefox_user_prefs={
            "dom.webdriver.enabled": False,
            "useAutomationExtension": False
        }
    )
    
    context = await browser.new_context(
        locale='fr-FR',
        timezone_id='America/New_York',
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        extra_http_headers={
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
    )
    
    return browser, context

async def collect_listing_urls(page, search_url, max_items):
    """Collecte les URLs d'annonces depuis une page de recherche"""
    await page.goto(search_url, wait_until='networkidle', timeout=60000)
    await asyncio.sleep(3)
    
    # Fermer popups si présents
    try:
        await page.click('button:has-text("Accepter")', timeout=2000)
    except:
        pass
    
    seen = set()
    for _ in range(5):  # Max 5 scrolls
        links = await page.locator('a[href*="/rooms/"]').all()
        for link in links:
            try:
                href = await link.get_attribute('href')
                if href and '/rooms/' in href and 'experiences' not in href:
                    full = urljoin(str(page.url), href.split('?')[0])
                    seen.add(full)
                    if len(seen) >= max_items:
                        break
            except:
                continue
        
        if len(seen) >= max_items:
            break
            
        await page.mouse.wheel(0, 2000)
        await asyncio.sleep(1)
    
    return list(seen)[:max_items]

async def extract_host_data(page, listing_url):
    """Extrait TOUTES les données depuis une page d'annonce"""
    data = {
        'listing_url': listing_url,
        'listing_title': '',
        'license_code': '',
        'host_url': '',
        'host_name': '',
        'host_rating': '',
        'host_years': '',
        'host_reviews_count': '',
        'scraped_at': now_iso()
    }
    
    try:
        # Navigation avec attente complète
        await page.goto(listing_url, wait_until='networkidle', timeout=60000)
        await asyncio.sleep(5)  # Attente sécurité pour redirections
        
        # Vérifier si page de connexion
        body_text = await page.locator('body').inner_text()
        if 'connexion' in body_text.lower() or 'sign in' in body_text.lower():
            print(f"    ⚠️ Page de connexion détectée, retry...")
            await page.reload(wait_until='networkidle')
            await asyncio.sleep(5)
        
        # Scroll pour charger section hôte
        await page.mouse.wheel(0, 2000)
        await asyncio.sleep(2)
        
        # 1. TITRE
        try:
            data['listing_title'] = await page.locator('h1').first.inner_text(timeout=3000)
        except:
            try:
                data['listing_title'] = await page.get_attribute('meta[property="og:title"]', 'content', timeout=3000)
            except:
                pass
        
        # 2. CODE LICENCE (dans le texte de description)
        try:
            # Chercher "Infos d'enregistrement" ou patterns de licence
            full_text = await page.locator('body').inner_text()
            
            # Patterns de licence Dubai
            patterns = [
                r'MAR-[A-Z]{3}-[A-Z0-9]{5,}',  # Ex: MAR-MAR-KMJWW
                r'\b\d{5,8}\b',  # Numéro simple
                r'[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, full_text)
                if match:
                    data['license_code'] = match.group(0)
                    break
        except:
            pass
        
        # 3. URL PROFIL HÔTE
        try:
            # Méthode 1: Lien "Accéder au profil"
            profile_link = await page.locator('a[href*="/users/profile/"]').first.get_attribute('href', timeout=5000)
            if profile_link:
                data['host_url'] = urljoin(listing_url, profile_link.split('?')[0])
        except:
            pass
        
        # 4. NOM HÔTE
        try:
            # Chercher heading avec nom d'hôte
            host_section = await page.locator('text=/Votre hôte|Rencontrez votre hôte/').first.locator('..').inner_text(timeout=3000)
            # Extraire le nom après "Votre hôte :"
            if 'Votre hôte' in host_section:
                name_match = re.search(r'Votre hôte\s*:\s*([^\n]+)', host_section)
                if name_match:
                    data['host_name'] = name_match.group(1).strip()
        except:
            try:
                # Alternative: heading niveau 2 avec nom
                data['host_name'] = await page.locator('h2:has-text("LLC"), h2:has-text("Vacation"), h2:has-text("Homes")').first.inner_text(timeout=3000)
            except:
                pass
        
        # 5. NOTE HÔTE + REVIEWS
        try:
            # Chercher pattern "156 commentaires Note moyenne : 4,86 sur 5"
            ratings_text = await page.locator('text=/commentaires.*Note moyenne/').first.inner_text(timeout=3000)
            
            # Extraire nombre de commentaires
            reviews_match = re.search(r'(\d+)\s+commentaires?', ratings_text)
            if reviews_match:
                data['host_reviews_count'] = reviews_match.group(1)
            
            # Extraire note
            rating_match = re.search(r'(\d+[,\.]\d+)\s+sur\s+5', ratings_text)
            if rating_match:
                data['host_rating'] = rating_match.group(1).replace(',', '.')
        except:
            pass
        
        # 6. ANNÉES D'ACTIVITÉ
        try:
            # Chercher "Hôte depuis X mois" ou "Hôte depuis X ans"
            years_text = await page.locator('text=/Hôte depuis/').first.inner_text(timeout=3000)
            
            if 'mois' in years_text:
                months_match = re.search(r'(\d+)\s+mois', years_text)
                if months_match:
                    data['host_years'] = f"0 (${months_match.group(1)} mois)"
            elif 'an' in years_text:
                years_match = re.search(r'(\d+)\s+ans?', years_text)
                if years_match:
                    data['host_years'] = years_match.group(1)
        except:
            pass
        
        print(f"    ✓ {data['listing_title'][:40]}...")
        print(f"      Licence: {data['license_code'] or 'N/A'}")
        print(f"      Hôte: {data['host_name'] or 'N/A'} ({data['host_rating'] or 'N/A'}★)")
        
    except Exception as e:
        print(f"    ❌ Erreur: {e}")
    
    return data

async def process_listing_batch(context, listing_urls, batch_id):
    """Traite un batch d'annonces dans un contexte"""
    page = await context.new_page()
    results = []
    
    for i, url in enumerate(listing_urls, 1):
        print(f"\n[Worker {batch_id}] [{i}/{len(listing_urls)}] {url}")
        data = await extract_host_data(page, url)
        results.append(data)
        await asyncio.sleep(2)  # Délai entre requêtes
    
    await page.close()
    return results

async def main():
    print("\n🚀 AIRBNB SCRAPER SIMPLIFIÉ")
    print("=" * 60)
    
    search_urls = read_search_urls()
    print(f"📋 {len(search_urls)} page(s) de recherche à traiter\n")
    
    all_results = []
    
    async with async_playwright() as p:
        # Créer contextes parallèles
        contexts = []
        browsers = []
        
        for i in range(MAX_WORKERS):
            browser, context = await create_browser_context(p)
            browsers.append(browser)
            contexts.append(context)
        
        print(f"🌐 {MAX_WORKERS} workers créés\n")
        
        # Pour chaque page de recherche
        for page_idx, search_url in enumerate(search_urls, 1):
            print(f"\n{'='*60}")
            print(f"📄 PAGE {page_idx}/{len(search_urls)}: {search_url}")
            print(f"{'='*60}")
            
            # Collecter URLs d'annonces
            temp_page = await contexts[0].new_page()
            listing_urls = await collect_listing_urls(temp_page, search_url, MAX_LISTINGS_PER_PAGE)
            await temp_page.close()
            
            print(f"✓ {len(listing_urls)} annonces trouvées")
            
            # Diviser en batches pour workers
            batch_size = (len(listing_urls) + MAX_WORKERS - 1) // MAX_WORKERS
            batches = [listing_urls[i:i+batch_size] for i in range(0, len(listing_urls), batch_size)]
            
            # Traiter en parallèle
            tasks = [process_listing_batch(contexts[i], batches[i], i+1) 
                    for i in range(len(batches))]
            
            batch_results = await asyncio.gather(*tasks)
            
            # Flatten results
            for batch in batch_results:
                all_results.extend(batch)
        
        # Fermer tout
        for browser in browsers:
            await browser.close()
    
    # Sauvegarder CSV
    if all_results:
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
            writer.writeheader()
            writer.writerows(all_results)
        
        print(f"\n{'='*60}")
        print(f"✅ {len(all_results)} hôtes sauvegardés dans {OUTPUT_CSV}")
        print(f"{'='*60}\n")
    else:
        print("\n⚠️ Aucune donnée collectée")

if __name__ == "__main__":
    asyncio.run(main())
