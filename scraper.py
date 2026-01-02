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

def get_session():
    """Generates a session with a random realistic browser fingerprint."""
    # Rotate Chrome versions to avoid static fingerprinting
    ver = random.choice(["120", "121", "122", "123", "124"])
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver}.0.0.0 Safari/537.36"

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }

    if SESSION_TYPE == "cffi":
        # curl_cffi impersonate handles the TLS handshake signature
        return cffi_requests.Session(impersonate="chrome120", headers=headers)
    else:
        s = std_requests.Session()
        s.headers.update(headers)
        return s

def resolve_name(ticker):
    """Normalize ticker and map to company name."""
    t = ticker.upper().split('.')[0]
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "PNDORA": "Pandora A/S",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "NVDA": "Nvidia"
    }
    return mapping.get(t, t)

# --- 3. MULTI-ENGINE SEARCH ---

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
    """Engine 2: Ask.com (Very resilient to Cloud IPs)"""
    try:
        # Ask.com is great because it has weaker bot protection than Google/Bing
        url = "https://www.ask.com/web"
        params = {'q': query + " site:investing.com transcript"}
        sess = get_session()
        resp = sess.get(url, params=params, timeout=10)
        
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        links = []
        for a in soup.find_all('a', href=True):
            # Ask.com results are usually clean URLs
            if "investing.com" in a['href']:
                links.append(a['href'])
        return links
    except: return []

def search_yahoo(query):
    """Engine 3: Yahoo (Fallback)"""
    try:
        url = "https://search.yahoo.com/search"
        params = {'p': query + " site:investing.com earnings call transcript"}
        sess = get_session()
        resp = sess.get(url, params=params, timeout=10)
        
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Clean Yahoo Redirects
            if "RU=" in href:
                try:
                    start = href.find("RU=") + 3
                    end = href.find("/RK=")
                    if end == -1: end = len(href)
                    href = urllib.parse.unquote(href[start:end])
                except: pass
            if "investing.com" in href:
                links.append(href)
        return links
    except: return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Orchestrating Search for: {name}")
    
    # 1. Try DuckDuckGo
    links = search_duckduckgo(name)
    if links:
        logger.info(f"✅ DuckDuckGo found {len(links)} links")
        return filter_links(links)

    # 2. Try Ask.com (Great fallback)
    logger.info("⚠️ DDG blocked. Switching to Ask.com...")
    links = search_ask(name)
    if links:
        logger.info(f"✅ Ask.com found {len(links)} links")
        return filter_links(links)

    # 3. Try Yahoo
    logger.info("⚠️ Ask.com blocked. Switching to Yahoo...")
    links = search_yahoo(name)
    if links:
        logger.info(f"✅ Yahoo found {len(links)} links")
        return filter_links(links)

    return []

def filter_links(raw_links):
    valid = []
    seen = set()
    for l in raw_links:
        if l in seen: continue
        seen.add(l)
        # Strict Transcript Filter
        if "investing.com" in l and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings" in l.lower():
                valid.append(l)
    return valid

# --- 4. FETCHING (Google Cache) ---

def clean_text(soup):
    # Try different content containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.body

    if not body: return None

    # Clean junk
    for tag in body(["script", "style", "iframe", "aside", "button", "figure", "span", "nav", "footer"]): tag.decompose()
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'share']):
            div.decompose()

    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 30 and "Position:" not in txt:
            text_parts.append(txt)
    return "\n\n".join(text_parts)

def fetch_google_cache(url):
    """Fetches text-only version from Google Cache."""
    try:
        # strip=1 is vital - removes all JS/CSS/Images
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        logger.info(f"🛡️ Google Cache: {cache_url}")
        
        sess = get_session()
        # Cache usually requires a simple desktop UA
        sess.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        
        resp = sess.get(cache_url, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            text = clean_text(soup)
            if not text: text = soup.get_text()
            return text
        return None
    except: return None

# --- 5. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search
    candidates = get_candidates(ticker)
    
    if not candidates:
        logger.error("❌ All search engines returned 0 results (IP Blocked).")
        return None, {"error": "All search engines blocked."}
    
    # 2. Fetch
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        text = fetch_google_cache(link)
        
        if text and len(text) > 500:
            return text, {
                "source": "Investing.com (Google Cache)",
                "url": link,
                "title": "Earnings Call",
                "symbol": ticker
            }
            
    return None, {"error": "Candidates found but download failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
