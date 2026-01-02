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

# Use standard requests for Jina/Proxies (they don't need TLS spoofing)
import requests as std_requests

# Try curl_cffi for Direct Fetch fallback
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded.")
except ImportError:
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found.")

# --- 2. HELPERS ---

def get_cffi_session():
    """Browser session for Search & Direct Fetch"""
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 132)}.0.0.0 Safari/537.36"
    headers = {"User-Agent": ua, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    
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
        "NVDA": "Nvidia"
    }
    return mapping.get(t, t)

# --- 3. SEARCH (DuckDuckGo Lite) ---

def search_duckduckgo(query):
    try:
        url = "https://html.duckduckgo.com/html/"
        data = {'q': query + " site:investing.com earnings call transcript"}
        sess = get_cffi_session() # Use cffi for search to avoid blocks
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

# --- 4. FETCHING STRATEGIES ---

def clean_text(soup):
    # Standard cleaner
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.body
    if not body: return None
    
    for tag in body(["script", "style", "iframe", "button", "figure", "aside"]): tag.decompose()
    for div in body.find_all('div'):
        if any(c in str(div.get('class', [])) for c in ['related', 'ad', 'share']): div.decompose()
        
    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 30 and "Position:" not in txt: text_parts.append(txt)
    return "\n\n".join(text_parts)

def fetch_jina_proxy(url):
    """
    STRATEGY 1: Jina Reader API (The 'Magic' Proxy)
    Uses Jina's servers to fetch and render the page into Markdown.
    """
    try:
        logger.info(f"   🤖 Engaging Jina Reader Proxy...")
        # r.jina.ai converts any URL to clean markdown/text
        jina_url = f"https://r.jina.ai/{url}"
        
        # Jina needs standard requests, no TLS spoofing needed
        resp = std_requests.get(jina_url, timeout=25)
        
        if resp.status_code == 200:
            text = resp.text
            # Check if Jina failed to get content
            if "Access to this page has been denied" in text or len(text) < 200:
                logger.warning("      ↳ Jina was blocked.")
                return None
                
            logger.info("      ↳ Jina Success!")
            return text
        return None
    except Exception as e:
        logger.warning(f"      ↳ Jina Error: {e}")
        return None

def fetch_translate_proxy(url):
    """STRATEGY 2: Google Translate Proxy"""
    try:
        logger.info(f"   🛡️ Engaging Translate Proxy...")
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        sess = get_cffi_session()
        resp = sess.get(proxy_url, timeout=20)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            # Extract from iframe if present
            iframe = soup.find('iframe', {'name': 'c'})
            if iframe and iframe.get('src'):
                resp = sess.get(iframe['src'], timeout=15)
                soup = BeautifulSoup(resp.content, 'html.parser')
            
            text = clean_text(soup)
            if text and len(text) > 500: return text
        return None
    except: return None

def fetch_direct_fallback(url):
    """STRATEGY 3: Direct Fetch (Last Resort)"""
    try:
        logger.info(f"   ⚡ Trying Direct Fetch...")
        sess = get_cffi_session()
        resp = sess.get(url, timeout=15)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if text and len(text) > 500: return text
        return None
    except: return None

# --- 5. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search
    candidates = get_candidates(ticker)
    if not candidates:
        return None, {"error": "No candidates found."}
    
    # 2. Fetch Loop
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        # A. Jina Reader (Best for Cloud IPs)
        text = fetch_jina_proxy(link)
        if text: return text, {"source": "Investing.com (Jina AI)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # B. Google Translate (Backup)
        text = fetch_translate_proxy(link)
        if text: return text, {"source": "Investing.com (Translate)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # C. Direct (Fallback)
        text = fetch_direct_fallback(link)
        if text: return text, {"source": "Investing.com (Direct)", "url": link, "title": "Earnings Call", "symbol": ticker}

    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
