"""
Scraper Airbnb - Version CORRIGÉE
- URL hôte correcte (pas les commentateurs)
- Code licence robuste
- Multilingue (FR + EN)
"""
import asyncio, csv, re, datetime, os
from urllib.parse import urljoin
from playwright.async_api import async_playwright

# Configuration
SEARCH_URLS_FILE = "search_urls.txt"
OUTPUT_CSV = "airbnb_hosts_data.csv"
MAX_WORKERS = 4
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
    
    # Fermer popups
    try:
        await page.click('button:has-text("Accepter")', timeout=2000)
    except:
        try:
            await page.click('button:has-text("Accept")', timeout=2000)
        except:
            pass
    
    seen = set()
    for _ in range(5):
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

async def extract_license_code(page):
    """Extraction ROBUSTE du code licence - CORRIGÉE"""
    try:
        # Étape 1 : Cliquer sur "Lire la suite" / "Read more" / "Show more"
        clicked = False
        for button_text in ["Lire la suite", "Read more", "Show more", "Afficher plus"]:
            try:
                button = page.locator(f'button:has-text("{button_text}")').first
                await button.click(timeout=2000)
                await asyncio.sleep(1)
                clicked = True
                print(f"      ✓ Cliqué sur '{button_text}'")
                break
            except:
                continue
        
        # Étape 2 : Récupérer le texte complet
        full_text = await page.locator('body').inner_text()
        
        # Étape 3 : Chercher section "Infos d'enregistrement" (FR + EN)
        registration_keywords = [
            "Infos d'enregistrement", "Détails de l'enregistrement",
            "Registration details", "Registration information",
            "License", "Licence", "Permit", "Permis",
            "Enregistrement"
        ]
        
        # Si on trouve une section dédiée, limiter la recherche
        text_to_search = full_text
        for keyword in registration_keywords:
            if keyword in full_text:
                idx = full_text.find(keyword)
                text_to_search = full_text[idx:idx+1000]  # 1000 chars après le keyword
                break
        
        # Étape 4 : Patterns de licence (ordre de priorité)
        patterns = [
            r'[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{5,7}',  # Ex: BUS-MAG-42KDF, MAR-MAR-KMJWW
            r'[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}',  # Variante
            r'\b\d{6,8}\b',  # Numéro simple (6-8 chiffres)
            r'[A-Z]{2,3}-[A-Z]{2,4}-[A-Z0-9]{4,8}',  # Pattern générique
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_to_search)
            if match:
                code = match.group(0)
                # Vérifier que ce n'est pas un faux positif (pas une date, etc.)
                if not re.match(r'^\d{2}-\d{2}-\d{4}$', code):  # Pas une date
                    return code
        
        return ""
        
    except Exception as e:
        print(f"      ⚠️ Erreur extraction licence: {e}")
        return ""

async def extract_host_url(page, listing_url):
    """Extraction CORRECTE de l'URL hôte - CORRIGÉE"""
    try:
        # Scroll vers section hôte
        await page.mouse.wheel(0, 3000)
        await asyncio.sleep(2)
        
        # STRATÉGIE 1 : Chercher dans la section "Rencontrez votre hôte" / "Meet your host"
        section_keywords = [
            "Rencontrez votre hôte", "Meet your host",
            "Votre hôte", "Your host",
            "About the host", "À propos de l'hôte"
        ]
        
        for keyword in section_keywords:
            try:
                # Trouver la section
                section = page.locator(f'text=/{keyword}/i').first
                # Chercher un lien profile dans cette section ou ses parents
                host_link = section.locator('xpath=ancestor::*//a[contains(@href, "/users/profile/")]').first
                href = await host_link.get_attribute('href', timeout=3000)
                if href:
                    url = urljoin(listing_url, href.split('?')[0])
                    print(f"      ✓ URL hôte (section): {url}")
                    return url
            except:
                continue
        
        # STRATÉGIE 2 : Chercher "Accéder au profil" / "View profile"
        profile_texts = [
            "Accéder au profil", "View profile",
            "profil complet", "full profile"
        ]
        
        for text in profile_texts:
            try:
                link = page.locator(f'a:has-text("{text}")').first
                href = await link.get_attribute('href', timeout=3000)
                if href and '/users/profile/' in href:
                    url = urljoin(listing_url, href.split('?')[0])
                    print(f"      ✓ URL hôte (lien profil): {url}")
                    return url
            except:
                continue
        
        # STRATÉGIE 3 : Chercher dans heading niveau 2 ou 3 (nom hôte)
        try:
            # Chercher h2 ou h3 qui contient un nom d'hôte (souvent proche du lien)
            headings = await page.locator('h2, h3').all()
            for heading in headings:
                text = await heading.inner_text()
                # Si c'est un nom probable (pas trop long, pas de mots-clés annonce)
                if len(text) < 50 and not any(word in text.lower() for word in ['bedroom', 'chambre', 'studio', 'appartement']):
                    # Chercher lien profile proche
                    nearby_link = heading.locator('xpath=ancestor::*//a[contains(@href, "/users/profile/")]').first
                    href = await nearby_link.get_attribute('href', timeout=2000)
                    if href:
                        url = urljoin(listing_url, href.split('?')[0])
                        print(f"      ✓ URL hôte (heading): {url}")
                        return url
        except:
            pass
        
        # STRATÉGIE 4 : Dernier recours - chercher tous les liens, mais ÉVITER les commentateurs
        all_links = await page.locator('a[href*="/users/profile/"]').all()
        if all_links:
            # Prendre le dernier lien plutôt que le premier (les commentateurs sont souvent en bas)
            # En fait non, prendre le premier après la section titre
            for link in all_links[:3]:  # Tester les 3 premiers seulement
                href = await link.get_attribute('href')
                if href and '/users/profile/' in href:
                    url = urljoin(listing_url, href.split('?')[0])
                    print(f"      ⚠️ URL hôte (fallback): {url}")
                    return url
        
        print(f"      ❌ Aucune URL hôte trouvée")
        return ""
        
    except Exception as e:
        print(f"      ⚠️ Erreur extraction URL hôte: {e}")
        return ""

async def extract_host_data(page, listing_url):
    """Extrait TOUTES les données depuis une page d'annonce - VERSION CORRIGÉE"""
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
        await asyncio.sleep(5)
        
        # Vérifier page de connexion
        body_text = await page.locator('body').inner_text()
        if 'connexion' in body_text.lower() or 'sign in' in body_text.lower():
            print(f"    ⚠️ Page de connexion, retry...")
            await page.reload(wait_until='networkidle')
            await asyncio.sleep(5)
        
        # Scroll complet
        await page.mouse.wheel(0, 2000)
        await asyncio.sleep(1)
        await page.mouse.wheel(0, 2000)
        await asyncio.sleep(1)
        
        # 1. TITRE
        try:
            data['listing_title'] = await page.locator('h1').first.inner_text(timeout=3000)
        except:
            try:
                data['listing_title'] = await page.get_attribute('meta[property="og:title"]', 'content', timeout=3000)
            except:
                pass
        
        # 2. CODE LICENCE - FONCTION ROBUSTE
        data['license_code'] = await extract_license_code(page)
        
        # 3. URL PROFIL HÔTE - FONCTION CORRIGÉE
        data['host_url'] = await extract_host_url(page, listing_url)
        
        # 4. NOM HÔTE (multilingue)
        try:
            # Pattern FR
            host_text = await page.locator('text=/Votre hôte|Rencontrez votre hôte/i').first.locator('..').inner_text(timeout=3000)
            name_match = re.search(r'Votre hôte\s*[:\s]+([^\n]+)', host_text, re.IGNORECASE)
            if name_match:
                data['host_name'] = name_match.group(1).strip()
        except:
            try:
                # Pattern EN
                host_text = await page.locator('text=/Your host|Meet your host/i').first.locator('..').inner_text(timeout=3000)
                name_match = re.search(r'Your host\s*[:\s]+([^\n]+)', host_text, re.IGNORECASE)
                if name_match:
                    data['host_name'] = name_match.group(1).strip()
            except:
                try:
                    # Chercher h2 avec nom
                    data['host_name'] = await page.locator('h2').nth(2).inner_text(timeout=3000)
                except:
                    pass
        
        # 5. NOTE HÔTE + REVIEWS (multilingue)
        try:
            # Pattern FR
            ratings_text = await page.locator('text=/commentaires?.*[Nn]ote/i').first.inner_text(timeout=3000)
            reviews_match = re.search(r'(\d+)\s+commentaires?', ratings_text, re.IGNORECASE)
            if reviews_match:
                data['host_reviews_count'] = reviews_match.group(1)
            rating_match = re.search(r'(\d+[,\.]\d+)\s+sur\s+5', ratings_text, re.IGNORECASE)
            if rating_match:
                data['host_rating'] = rating_match.group(1).replace(',', '.')
        except:
            try:
                # Pattern EN
                ratings_text = await page.locator('text=/reviews?.*[Rr]ating/i').first.inner_text(timeout=3000)
                reviews_match = re.search(r'(\d+)\s+reviews?', ratings_text, re.IGNORECASE)
                if reviews_match:
                    data['host_reviews_count'] = reviews_match.group(1)
                rating_match = re.search(r'(\d+[,\.]\d+)\s+out of\s+5', ratings_text, re.IGNORECASE)
                if rating_match:
                    data['host_rating'] = rating_match.group(1).replace(',', '.')
            except:
                pass
        
        # 6. ANNÉES D'ACTIVITÉ (multilingue)
        try:
            # Pattern FR
            years_text = await page.locator('text=/Hôte depuis/i').first.inner_text(timeout=3000)
            if 'mois' in years_text:
                months_match = re.search(r'(\d+)\s+mois', years_text, re.IGNORECASE)
                if months_match:
                    data['host_years'] = f"0 (${months_match.group(1)} mois)"
            elif 'an' in years_text:
                years_match = re.search(r'(\d+)\s+ans?', years_text, re.IGNORECASE)
                if years_match:
                    data['host_years'] = years_match.group(1)
        except:
            try:
                # Pattern EN
                years_text = await page.locator('text=/Hosting since|Host since/i').first.inner_text(timeout=3000)
                if 'month' in years_text:
                    months_match = re.search(r'(\d+)\s+months?', years_text, re.IGNORECASE)
                    if months_match:
                        data['host_years'] = f"0 (${months_match.group(1)} months)"
                elif 'year' in years_text:
                    years_match = re.search(r'(\d+)\s+years?', years_text, re.IGNORECASE)
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
    """Traite un batch d'annonces"""
    page = await context.new_page()
    results = []
    
    for i, url in enumerate(listing_urls, 1):
        print(f"\n[Worker {batch_id}] [{i}/{len(listing_urls)}] {url}")
        data = await extract_host_data(page, url)
        results.append(data)
        await asyncio.sleep(2)
    
    await page.close()
    return results

async def main():
    print("\n🚀 AIRBNB SCRAPER CORRIGÉ")
    print("=" * 60)
    
    search_urls = read_search_urls()
    print(f"📋 {len(search_urls)} page(s) de recherche\n")
    
    all_results = []
    
    async with async_playwright() as p:
        contexts = []
        browsers = []
        
        for i in range(MAX_WORKERS):
            browser, context = await create_browser_context(p)
            browsers.append(browser)
            contexts.append(context)
        
        print(f"🌐 {MAX_WORKERS} workers\n")
        
        for page_idx, search_url in enumerate(search_urls, 1):
            print(f"\n{'='*60}")
            print(f"📄 PAGE {page_idx}/{len(search_urls)}")
            print(f"{'='*60}")
            
            temp_page = await contexts[0].new_page()
            listing_urls = await collect_listing_urls(temp_page, search_url, MAX_LISTINGS_PER_PAGE)
            await temp_page.close()
            
            print(f"✓ {len(listing_urls)} annonces")
            
            batch_size = (len(listing_urls) + MAX_WORKERS - 1) // MAX_WORKERS
            batches = [listing_urls[i:i+batch_size] for i in range(0, len(listing_urls), batch_size)]
            
            tasks = [process_listing_batch(contexts[i], batches[i], i+1) 
                    for i in range(len(batches))]
            
            batch_results = await asyncio.gather(*tasks)
            
            for batch in batch_results:
                all_results.extend(batch)
        
        for browser in browsers:
            await browser.close()
    
    if all_results:
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
            writer.writeheader()
            writer.writerows(all_results)
        
        print(f"\n{'='*60}")
        print(f"✅ {len(all_results)} hôtes → {OUTPUT_CSV}")
        print(f"{'='*60}\n")
    else:
        print("\n⚠️ Aucune donnée")

if __name__ == "__main__":
    asyncio.run(main())
