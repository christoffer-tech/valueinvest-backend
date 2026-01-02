import sys
import logging
import urllib.parse
import re
import time
import random
from bs4 import BeautifulSoup

# --- 1. CONFIGURATION & LOGGING ---
logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try to use curl_cffi for better TLS fingerprinting
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
    Creates a session with realistic browser headers.
    """
    if mobile:
        ua = f"Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 130)}.0.0.0 Mobile Safari/537.36"
    else:
        ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 130)}.0.0.0 Safari/537.36"

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1"
    }

    if SESSION_TYPE == "cffi":
        # Randomize impersonation to look less robotic
        ver = random.choice(["chrome110", "chrome120", "chrome124"])
        return cffi_requests.Session(impersonate=ver, headers=headers)
    else:
        s = std_requests.Session()
        s.headers.update(headers)
        return s

def resolve_name(ticker):
    """Resolves ticker to company name."""
    t = ticker.upper().split('.')[0]
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "PNDORA": "Pandora A/S",
        "TSLA": "Tesla",
        "NVDA": "Nvidia"
    }
    return mapping.get(t, t)

# --- 3. SEARCH ENGINES ---

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

def search_yahoo(query):
    try:
        url = "https://search.yahoo.com/search"
        params = {'p': query + " site:investing.com earnings call transcript", 'ei': 'UTF-8'}
        sess = get_session()
        resp = sess.get(url, params=params, timeout=10)
        if resp.status_code != 200: return []
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if "RU=" in href: # Decode Yahoo Redirects
                try:
                    start = href.find("RU=") + 3
                    end = href.find("/RK=")
                    if end == -1: end = len(href)
                    href = urllib.parse.unquote(href[start:end])
                except: pass
            if "investing.com" in href: links.append(href)
        return links
    except: return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name} (Ticker: {ticker})")
    
    # 1. DuckDuckGo (Primary)
    links = search_duckduckgo(name)
    if links: 
        logger.info(f"✅ DuckDuckGo found {len(links)} links")
        return filter_links(links)
        
    # 2. Yahoo (Backup)
    logger.info("⚠️ DDG failed. Switching to Yahoo...")
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
        if "investing.com" in l and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings" in l.lower():
                valid.append(l)
    return valid

# --- 4. FETCHING STRATEGIES ---

def clean_text(soup):
    """
    Robust text cleaner.
    Falls back to collecting ALL paragraphs if specific containers are missing.
    """
    # 1. Try standard containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.find('div', class_='article-content')

    # 2. If no container (common in Cache/Proxy), use Body
    if not body:
        body = soup.body

    if not body: return None

    # Remove junk
    for tag in body(["script", "style", "iframe", "aside", "button", "figure", "span", "nav", "footer", "header"]): 
        tag.decompose()
    
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'img', 'share']):
            div.decompose()

    # Extract text
    text_parts = []
    for p in body.find_all(['p', 'h2', 'div']): # div included for unformatted text
        txt = p.get_text().strip()
        # Quality filter
        if len(txt) > 40 and "Position:" not in txt and "confidential tip" not in txt and "Copyright" not in txt:
            text_parts.append(txt)
    
    # Heuristic: If we found very few paragraphs, it's probably a failed parse
    if len(text_parts) < 5: return None
            
    return "\n\n".join(text_parts)

def fetch_google_cache(url):
    """
    STRATEGY 1: Google Web Cache (Text Only).
    This is the most powerful unblocking method.
    """
    try:
        # 'strip=1' forces text-only mode (extremely fast, no JS)
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        logger.info(f"🛡️ Google Cache: {cache_url}")
        
        sess = get_session()
        resp = sess.get(cache_url, timeout=15)
        
        if resp.status_code == 404:
            logger.warning("   ↳ Cache miss (404).")
            return None
        if resp.status_code != 200: 
            return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Google Cache often puts content in a <pre> tag or raw body
        text = clean_text(soup)
        if not text:
            # Fallback: Just grab the whole text if structure is gone
            text = soup.get_text()
            
        return text
    except Exception as e:
        logger.error(f"Cache Error: {e}")
        return None

def fetch_via_google_translate(url):
    """
    STRATEGY 2: Google Translate Proxy.
    """
    try:
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        logger.info(f"🛡️ Translate Proxy: {proxy_url}")
        
        sess = get_session()
        resp = sess.get(proxy_url, timeout=20)
        
        if resp.status_code != 200: return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        text = clean_text(soup)
        
        if not text or len(text) < 500:
            iframe = soup.find('iframe', {'name': 'c'})
            if iframe and iframe.get('src'):
                logger.info("   ↳ Following Iframe...")
                resp2 = sess.get(iframe['src'], timeout=15)
                text = clean_text(BeautifulSoup(resp2.content, 'html.parser'))

        return text
    except Exception as e:
        return None

# --- 5. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search
    candidates = get_candidates(ticker)
    if not candidates:
        return None, {"error": "No links found via Search."}
    
    # 2. Fetch Loop
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        # A. Try Google Cache (Best)
        text = fetch_google_cache(link)
        if text and len(text) > 1000:
            return text, {"source": "Investing.com (Cache)", "url": link, "title": "Earnings Call", "symbol": ticker}
            
        # B. Try Translate Proxy (Backup)
        text = fetch_via_google_translate(link)
        if text and len(text) > 1000:
            return text, {"source": "Investing.com (Proxy)", "url": link, "title": "Earnings Call", "symbol": ticker}
            
    return None, {"error": "Found links but failed to fetch content (Blocked)."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS! Length: {len(t)}")
