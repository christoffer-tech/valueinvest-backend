import sys
import logging
import urllib.parse
import re
import time
import random
import requests as std_requests # For Jina/Standard
from bs4 import BeautifulSoup

# --- 1. CONFIGURATION ---
JINA_API_KEY = "jina_18edc5ecbee44fceb94ea05a675f2fd5NYFCvhRikOR-aCOpgK0KCRywSnaq"

logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try curl_cffi for Direct Fetch
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded.")
except ImportError:
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found.")

# --- 2. SESSION FACTORY ---
def get_cffi_session():
    """Browser session for Direct Fetch"""
    # Rotate Chrome versions
    ver = random.choice(["120", "124", "119"])
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver}.0.0.0 Safari/537.36"
    
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1"
    }
    
    if SESSION_TYPE == "cffi":
        return cffi_requests.Session(impersonate="chrome120", headers=headers)
    else:
        s = std_requests.Session()
        s.headers.update(headers)
        return s

def resolve_name(ticker):
    t = ticker.upper().split('.')[0]
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "PNDORA": "Pandora A/S",
        "TSLA": "Tesla",
        "NVDA": "Nvidia",
        "AAPL": "Apple",
        "MSFT": "Microsoft"
    }
    return mapping.get(t, t)

# --- 3. SEARCH (DuckDuckGo Lite) ---
def search_duckduckgo(query):
    try:
        url = "https://html.duckduckgo.com/html/"
        data = {'q': query + " site:investing.com earnings call transcript"}
        # Use cffi for search to avoid blocks
        sess = get_cffi_session() 
        resp = sess.post(url, data=data, timeout=10)
        
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        links = []
        for a in soup.find_all('a', class_='result__a', href=True):
            links.append(a['href'])
        return links
    except: return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name}")
    
    links = search_duckduckgo(name)
    valid = []
    seen = set()
    for l in links:
        if l in seen: continue
        seen.add(l)
        if "investing.com" in l and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings" in l.lower():
                valid.append(l)
    
    logger.info(f"✅ Found {len(valid)} candidates.")
    return valid

# --- 4. TEXT CLEANER ---
def clean_text(soup):
    # Try generic containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.body
    if not body: return None
    
    for tag in body(["script", "style", "iframe", "button", "figure", "aside", "nav", "footer"]): tag.decompose()
    for div in body.find_all('div'):
        if any(c in str(div.get('class', [])) for c in ['related', 'ad', 'share', 'img']): div.decompose()
        
    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 30 and "Position:" not in txt: text_parts.append(txt)
    return "\n\n".join(text_parts)

# --- 5. FETCHING STRATEGIES ---

def fetch_google_cache(url):
    """STRATEGY 1: Google Web Cache (Text Only)"""
    try:
        logger.info(f"   💾 Trying Google Cache...")
        # strip=1 is VITAL - it removes JS/CSS/Images which causes 99% of blocks
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        
        sess = get_cffi_session()
        # Cache usually requires a simple desktop UA
        sess.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        
        resp = sess.get(cache_url, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            # In strip=1 mode, the whole body is the text usually
            text = clean_text(soup) or soup.get_text()
            if text and len(text) > 500: return text
        elif resp.status_code == 404:
            logger.info("      ↳ Cache Miss (Page too new or uncached)")
        else:
            logger.info(f"      ↳ Cache Failed ({resp.status_code})")
            
        return None
    except Exception as e: return None

def fetch_jina_proxy(url):
    """STRATEGY 2: Jina Reader API"""
    try:
        logger.info(f"   🤖 Trying Jina Reader...")
        jina_url = f"https://r.jina.ai/{url}"
        
        headers = {
            "Authorization": f"Bearer {JINA_API_KEY}",
            "X-Respond-With": "markdown",
            "X-No-Cache": "true",
            "X-With-Generated-Alt": "false"
        }
        
        resp = std_requests.get(jina_url, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            text = resp.text
            if "Access to this page has been denied" in text or len(text) < 200:
                logger.warning("      ↳ Jina Blocked.")
                return None
            return text
        else:
            logger.warning(f"      ↳ Jina Status: {resp.status_code}")
        return None
    except Exception as e: return None

def fetch_archive(url):
    """STRATEGY 3: Archive.today / Wayback"""
    try:
        logger.info(f"   🏛️ Trying Archive Mirrors...")
        # 1. Wayback Machine API
        wb_api = f"https://archive.org/wayback/available?url={url}"
        resp = std_requests.get(wb_api, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('archived_snapshots', {}).get('closest', {}):
                snap_url = data['archived_snapshots']['closest']['url']
                logger.info(f"      ↳ Found Wayback: {snap_url}")
                resp_snap = std_requests.get(snap_url, timeout=20)
                if resp_snap.status_code == 200:
                    text = clean_text(BeautifulSoup(resp_snap.content, 'html.parser'))
                    if text and len(text) > 500: return text
        
        # 2. Archive.today (Requires curl_cffi usually)
        if SESSION_TYPE == "cffi":
            sess = get_cffi_session()
            archive_url = f"https://archive.today/newest/{url}"
            resp = sess.get(archive_url, timeout=20)
            if resp.status_code == 200:
                text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
                if text and len(text) > 500: return text
                
        return None
    except: return None

def fetch_translate(url):
    """STRATEGY 4: Google Translate Proxy"""
    try:
        logger.info(f"   🛡️ Trying Translate Proxy...")
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        sess = get_cffi_session()
        resp = sess.get(proxy_url, timeout=20)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            iframe = soup.find('iframe', {'name': 'c'})
            if iframe and iframe.get('src'):
                resp = sess.get(iframe['src'], timeout=15)
                soup = BeautifulSoup(resp.content, 'html.parser')
            text = clean_text(soup)
            if text and len(text) > 500: return text
        return None
    except: return None

def fetch_direct(url):
    """STRATEGY 5: Direct Fetch"""
    try:
        logger.info(f"   ⚡ Trying Direct Fetch...")
        sess = get_cffi_session()
        resp = sess.get(url, timeout=20)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if text and len(text) > 500: return text
        else:
            logger.info(f"      ↳ Direct Failed: {resp.status_code}")
        return None
    except Exception as e: return None

# --- 6. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search
    candidates = get_candidates(ticker)
    if not candidates:
        return None, {"error": "No candidates found."}
    
    # 2. Fetch Loop
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        # 1. Google Cache (Best)
        text = fetch_google_cache(link)
        if text: return text, {"source": "Investing.com (Cache)", "url": link, "title": "Earnings Call", "symbol": ticker}

        # 2. Jina Reader (Reliable)
        text = fetch_jina_proxy(link)
        if text: return text, {"source": "Investing.com (Jina)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # 3. Archive (Backup)
        text = fetch_archive(link)
        if text: return text, {"source": "Investing.com (Archive)", "url": link, "title": "Earnings Call", "symbol": ticker}

        # 4. Translate Proxy (Fallback)
        text = fetch_translate(link)
        if text: return text, {"source": "Investing.com (Translate)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # 5. Direct (Last Resort)
        text = fetch_direct(link)
        if text: return text, {"source": "Investing.com (Direct)", "url": link, "title": "Earnings Call", "symbol": ticker}

    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
