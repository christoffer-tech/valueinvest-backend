import sys
import logging
import urllib.parse
import re
import time
import random
from bs4 import BeautifulSoup

# --- 1. CONFIGURATION ---
logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try to use curl_cffi (Best for bypassing Cloudflare)
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded.")
except ImportError:
    import requests as std_requests
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Using standard requests.")

# --- 2. SESSION FACTORY ---

def get_session(mobile=False):
    """
    Creates a browser session with randomized fingerprints.
    """
    if mobile:
        ua = f"Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 132)}.0.0.0 Mobile Safari/537.36"
    else:
        ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 132)}.0.0.0 Safari/537.36"

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }

    if SESSION_TYPE == "cffi":
        return cffi_requests.Session(impersonate="chrome120", headers=headers)
    else:
        s = std_requests.Session()
        s.headers.update(headers)
        return s

# --- 3. INTELLIGENT NAME RESOLUTION (From Colab) ---

def normalize_ticker(symbol):
    """
    Converts tickers like 'VWS.CO' -> 'VWS.CO' (keeps suffix) 
    but helps resolve the name correctly.
    """
    if not symbol or '.' not in symbol: return symbol
    return symbol

def resolve_name(ticker):
    """
    Maps tickers to company names. 
    Crucial for VWS.CO -> Vestas Wind Systems.
    """
    t = ticker.upper().split('.')[0]
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "PNDORA": "Pandora A/S",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "NVDA": "Nvidia",
        "GOOG": "Alphabet",
        "META": "Meta Platforms"
    }
    return mapping.get(t, t)

# --- 4. MULTI-ENGINE SEARCH ---

def search_duckduckgo(query):
    """Engine 1: DuckDuckGo Lite (HTML)"""
    try:
        url = "https://html.duckduckgo.com/html/"
        data = {'q': query + " site:investing.com earnings call transcript"}
        sess = get_session()
        resp = sess.post(url, data=data, timeout=10)
        
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        links = []
        for a in soup.find_all('a', class_='result__a', href=True):
            links.append(a['href'])
        return links
    except: return []

def search_ask(query):
    """Engine 2: Ask.com (Backup)"""
    try:
        url = "https://www.ask.com/web"
        params = {'q': query + " site:investing.com transcript"}
        sess = get_session()
        resp = sess.get(url, params=params, timeout=10)
        if resp.status_code != 200: return []
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if "investing.com" in a['href']: links.append(a['href'])
        return links
    except: return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name}")
    
    # 1. DuckDuckGo
    links = search_duckduckgo(name)
    if links: 
        logger.info(f"✅ DuckDuckGo found {len(links)} links")
        return filter_links(links)

    # 2. Ask.com
    logger.info("⚠️ DDG failed. Switching to Ask.com...")
    links = search_ask(name)
    if links:
        logger.info(f"✅ Ask.com found {len(links)} links")
        return filter_links(links)

    return []

def filter_links(raw_links):
    valid = []
    seen = set()
    for l in raw_links:
        if l in seen: continue
        seen.add(l)
        if "investing.com" in l and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings" in l.lower():
                valid.append(l)
    return valid

# --- 5. FETCHING (With Archive Fallback) ---

def clean_text(soup):
    # Try generic containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.body

    if not body: return None

    # Clean junk
    for tag in body(["script", "style", "iframe", "aside", "button", "figure", "span", "nav", "footer", "header"]): 
        tag.decompose()
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'share']):
            div.decompose()

    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 30 and "Position:" not in txt:
            text_parts.append(txt)
    return "\n\n".join(text_parts)

def fetch_archive_mirror(url):
    """
    STRATEGY: Archive.today Mirroring (From Colab Script)
    This bypasses the live site entirely by checking if it was archived.
    """
    mirrors = ["https://archive.is", "https://archive.li", "https://archive.ph", "https://archive.today"]
    
    for mirror in mirrors:
        try:
            target = f"{mirror}/newest/{url}"
            logger.info(f"   🏛️ Checking Mirror: {mirror}")
            
            sess = get_session()
            resp = sess.get(target, timeout=20)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                text = clean_text(soup)
                
                # Check if we got a "WIP" (Work in Progress) page
                if "working..." in soup.get_text().lower():
                    logger.info("      ↳ Archive is working... waiting 5s")
                    time.sleep(5)
                    resp = sess.get(resp.url, timeout=20)
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    text = clean_text(soup)

                if text and len(text) > 500:
                    return text
        except:
            continue
    return None

def fetch_direct(url):
    """Direct Fetch using curl_cffi"""
    try:
        logger.info(f"   ⚡ Trying Direct Fetch...")
        sess = get_session()
        resp = sess.get(url, timeout=20)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if text and len(text) > 500: return text
        return None
    except: return None

def fetch_google_cache(url):
    """Google Cache Fetch"""
    try:
        logger.info(f"   💾 Trying Google Cache...")
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        sess = get_session()
        sess.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        resp = sess.get(cache_url, timeout=15)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if text and len(text) > 500: return text
        return None
    except: return None

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
        
        # A. Direct Fetch (Fastest)
        text = fetch_direct(link)
        if text: return text, {"source": "Investing.com", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # B. Archive Mirror (The Colab Trick)
        text = fetch_archive_mirror(link)
        if text: return text, {"source": "Archive.today", "url": link, "title": "Earnings Call", "symbol": ticker}

        # C. Google Cache (Legacy)
        text = fetch_google_cache(link)
        if text: return text, {"source": "Google Cache", "url": link, "title": "Earnings Call", "symbol": ticker}
        
    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
