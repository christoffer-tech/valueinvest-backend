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
        "NVDA": "Nvidia"
    }
    if "PNDORA" in t: return "Pandora A/S"
    return mapping.get(t, t)

# --- 3. SEARCH (DuckDuckGo) ---
def search_ddg(query, site_filter=None):
    try:
        if site_filter:
            full_query = f"{query} site:{site_filter} earnings call transcript"
        else:
            full_query = f"{query} earnings call transcript"
            
        logger.info(f"🔎 Searching DDG for: {full_query}")
        url = "https://html.duckduckgo.com/html/"
        sess = get_session(mobile=False)
        resp = sess.post(url, data={'q': full_query}, timeout=15)
        
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', class_='result__a', href=True):
            links.append(a['href'])
        return links
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return []

# --- 4. FETCHING STRATEGIES ---

def clean_text(soup):
    """Standard cleaner for transcript text"""
    # Try different content containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', class_='article-body') or \
           soup.find('div', class_='article-content') or \
           soup.find('div', id='article-content')

    if not body: return None

    # Remove junk
    for tag in body(["script", "style", "iframe", "aside", "button"]): tag.decompose()
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad']):
            div.decompose()

    # Extract text
    text_parts = []
    for p in body.find_all(['p', 'h2', 'li']):
        txt = p.get_text().strip()
        if len(txt) > 20 and "Position:" not in txt and "confidential tip" not in txt:
            text_parts.append(txt)
            
    return "\n\n".join(text_parts)

def fetch_via_google_cache(url):
    """
    NUCLEAR OPTION: Fetches the page from Google Cache to bypass blocking.
    """
    try:
        # Construct Cache URL (strip=1 gets text-only version, faster & safer)
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        logger.info(f"🛡️ Attempting Google Cache Bypass: {cache_url}")
        
        sess = get_session(mobile=False)
        # Google Cache requires a clean User-Agent usually
        sess.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        
        resp = sess.get(cache_url, timeout=15)
        
        if resp.status_code == 404:
            logger.warning("Cache miss (404). Page not cached by Google.")
            return None, None
        if resp.status_code != 200:
            logger.warning(f"Cache fetch failed: {resp.status_code}")
            return None, None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Google Cache puts the original content inside a <pre> or generic div
        # We try to parse it normally as the structure is usually preserved
        text = clean_text(soup)
        if not text:
            # Fallback for text-only cache view
            text = soup.get_text()
            
        return text, "Google Cache"
        
    except Exception as e:
        logger.error(f"Cache Error: {e}")
        return None, None

def fetch_direct(url):
    """Standard direct fetch with mobile fallback."""
    try:
        logger.info(f"📥 Fetching Direct: {url}")
        sess = get_session(mobile=False)
        resp = sess.get(url, timeout=10)
        
        if resp.status_code == 403:
            logger.warning("⚠️ 403 Blocked (Desktop). Retrying Mobile...")
            time.sleep(1)
            sess = get_session(mobile=True)
            resp = sess.get(url, timeout=10)
            
        if resp.status_code != 200:
            return None, f"Status {resp.status_code}"
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        text = clean_text(soup)
        title = soup.title.string.strip() if soup.title else "Unknown"
        
        return text, title
    except Exception as e:
        return None, str(e)

# --- 5. MAIN ORCHESTRATOR ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    name = resolve_name(ticker)
    
    # --- PHASE 1: INVESTING.COM ---
    logger.info(f"--- Phase 1: Searching Investing.com ---")
    links = search_ddg(name, site_filter="investing.com")
    
    # Filter Investing.com links
    candidates = [l for l in links if "investing.com" in l and ("transcript" in l or "earnings" in l)]
    
    if candidates:
        logger.info(f"✅ Found {len(candidates)} Investing.com candidates.")
        for link in candidates[:3]: # Try top 3
            # A. Try Direct
            text, meta = fetch_direct(link)
            if text and len(text) > 500:
                return text, {"source": "Investing.com", "url": link, "title": meta, "symbol": ticker}
            
            # B. Try Google Cache (If direct failed)
            logger.info("Direct failed. Engaging Google Cache...")
            text, source = fetch_via_google_cache(link)
            if text and len(text) > 500:
                return text, {"source": "Investing.com (Cached)", "url": link, "title": "Cached Transcript", "symbol": ticker}
                
    else:
        logger.warning("❌ No Investing.com links found.")

    # --- PHASE 2: MOTLEY FOOL FALLBACK ---
    # If Investing.com is 100% dead/blocked, try Fool.com
    logger.info(f"--- Phase 2: Motley Fool Fallback ---")
    fool_links = search_ddg(name, site_filter="fool.com")
    
    if fool_links:
        logger.info(f"✅ Found {len(fool_links)} Motley Fool candidates.")
        for link in fool_links[:3]:
            text, meta = fetch_direct(link)
            if text and len(text) > 500:
                 return text, {"source": "Motley Fool", "url": link, "title": meta, "symbol": ticker}
                 
    return None, {"error": "All sources failed (Blocked or Not Found)"}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {m['url']}")
