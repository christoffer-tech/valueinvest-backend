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
    """Generates a session with realistic headers."""
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

def resolve_name(ticker):
    t = ticker.upper().split('.')[0]
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "PNDORA": "Pandora A/S",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "MSFT": "Microsoft"
    }
    return mapping.get(t, t)

# --- 3. SEARCH (DuckDuckGo Lite) ---

def search_duckduckgo(query):
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

# --- 4. THE THREE-LAYER FETCHER ---

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

def fetch_via_translate(url):
    """Method A: Google Translate Proxy (Live Fetch)"""
    try:
        # Translate English -> English
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        logger.info(f"   🛡️ Trying Translate Proxy...")
        
        sess = get_session()
        resp = sess.get(proxy_url, timeout=15)
        
        if resp.status_code != 200: return None
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # If content is hidden in iframe
        iframe = soup.find('iframe', {'name': 'c'})
        if iframe and iframe.get('src'):
            resp = sess.get(iframe['src'], timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
        text = clean_text(soup)
        if text and len(text) > 500: return text
        return None
    except: return None

def fetch_wayback(url):
    """Method B: Wayback Machine (Archive)"""
    try:
        logger.info(f"   🏛️ Trying Wayback Machine...")
        api_url = f"https://archive.org/wayback/available?url={url}"
        sess = get_session()
        resp = sess.get(api_url, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get('archived_snapshots', {}).get('closest', {}):
                snap_url = data['archived_snapshots']['closest']['url']
                resp_snap = sess.get(snap_url, timeout=20)
                if resp_snap.status_code == 200:
                    soup = BeautifulSoup(resp_snap.content, 'html.parser')
                    text = clean_text(soup)
                    if text and len(text) > 500: return text
        return None
    except: return None

def fetch_google_cache(url):
    """Method C: Google Cache (Legacy)"""
    try:
        logger.info(f"   💾 Trying Google Cache...")
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        sess = get_session()
        resp = sess.get(cache_url, timeout=10)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if text and len(text) > 500: return text
        return None
    except: return None

# --- 5. MAIN ORCHESTRATOR ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search
    candidates = get_candidates(ticker)
    if not candidates:
        return None, {"error": "No candidates found."}
    
    # 2. Iterate Candidates
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        # Try Method A: Translate Proxy
        text = fetch_via_translate(link)
        if text: return text, {"source": "Investing.com (Proxy)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # Try Method B: Wayback Machine
        text = fetch_wayback(link)
        if text: return text, {"source": "Investing.com (Archive)", "url": link, "title": "Earnings Call", "symbol": ticker}

        # Try Method C: Google Cache
        text = fetch_google_cache(link)
        if text: return text, {"source": "Investing.com (Cache)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
