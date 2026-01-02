import sys
import logging
import urllib.parse
import re
import time
import random
from bs4 import BeautifulSoup

# --- 1. SETUP ---
logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try importing curl_cffi, fallback to requests
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
    if mobile:
        ua = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    else:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
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
        "PNDORA": "Pandora A/S"
    }
    return mapping.get(t, t)

# --- 3. SEARCH (DuckDuckGo Lite) ---
def search_ddg(query):
    try:
        full_query = f"{query} site:investing.com earnings call transcript"
        logger.info(f"🔎 Searching DDG for: {full_query}")
        
        url = "https://html.duckduckgo.com/html/"
        sess = get_session(mobile=False)
        resp = sess.post(url, data={'q': full_query}, timeout=15)
        
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', class_='result__a', href=True):
            if "investing.com" in a['href']:
                links.append(a['href'])
        return links
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return []

# --- 4. FETCHING STRATEGIES ---

def clean_text(soup):
    """Robust text cleaner"""
    # Try generic article containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', class_='article-content') or \
           soup.find('div', id='article-content')

    if not body: return None

    # Remove junk tags
    for tag in body(["script", "style", "iframe", "aside", "button", "figure"]): 
        tag.decompose()
    
    # Remove junk divs (ads, related articles)
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'img']):
            div.decompose()

    # Extract clean text
    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 20 and "Position:" not in txt and "confidential tip" not in txt:
            text_parts.append(txt)
            
    return "\n\n".join(text_parts)

def fetch_via_google_translate(url):
    """
    MAGIC BULLET: Uses Google Translate as a proxy to fetch the page.
    This bypasses almost all IP blocks because the request comes from Google.
    """
    try:
        # We ask Google to translate English -> English (no change, just proxying)
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        logger.info(f"🛡️ Engaging Google Translate Proxy: {proxy_url}")
        
        sess = get_session(mobile=False)
        resp = sess.get(proxy_url, timeout=20)
        
        if resp.status_code != 200:
            return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # The translated content is usually inside an iframe or a specific container
        # Google Translate structure is complex, but the original text is often embedded
        
        # 1. Try to find the main content within the translate wrapper
        # Often inside a generic div or just the body
        text = clean_text(soup)
        
        # 2. If clean_text failed (because class names changed in proxy), try brute force p-tag extraction
        if not text or len(text) < 500:
            paragraphs = [p.get_text().strip() for p in soup.find_all('p') if len(p.get_text()) > 50]
            text = "\n\n".join(paragraphs)
            
        return text
        
    except Exception as e:
        logger.error(f"Translate Proxy Error: {e}")
        return None

def fetch_direct(url):
    """Direct fetch with mobile retry."""
    try:
        logger.info(f"📥 Direct Fetch: {url}")
        sess = get_session(mobile=False)
        resp = sess.get(url, timeout=10)
        
        if resp.status_code == 403:
            logger.warning("⚠️ 403 Blocked. Retrying Mobile...")
            time.sleep(1)
            sess = get_session(mobile=True)
            resp = sess.get(url, timeout=10)
            
        if resp.status_code != 200: return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        return clean_text(soup)
    except Exception as e:
        return None

# --- 5. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    name = resolve_name(ticker)
    
    # 1. Search
    links = search_ddg(name)
    candidates = [l for l in links if "investing.com" in l and ("transcript" in l.lower() or "earnings" in l.lower())]
    
    if not candidates:
        logger.error("❌ No links found.")
        return None, {"error": "No links found"}

    logger.info(f"✅ Found {len(candidates)} candidates.")
    
    # 2. Iterate candidates
    for link in candidates[:3]:
        # A. Try Direct
        text = fetch_direct(link)
        if text and len(text) > 500:
            return text, {"source": "Investing.com", "url": link, "title": "Earnings Call", "symbol": ticker}
            
        # B. Try Google Translate Proxy (The Fix)
        text = fetch_via_google_translate(link)
        if text and len(text) > 500:
            return text, {"source": "Investing.com (Via Proxy)", "url": link, "title": "Earnings Call", "symbol": ticker}
            
    logger.error("❌ All fetch methods failed.")
    return None, {"error": "Server blocked by Investing.com"}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
