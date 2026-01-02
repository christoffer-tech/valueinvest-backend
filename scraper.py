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
    logger.info("✅ curl_cffi loaded (Best for bypassing Cloudflare)")
except ImportError:
    import requests as std_requests
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Using standard requests.")

# --- 2. SESSION FACTORY ---

def get_session(mobile=False):
    """
    Creates a new session with rotated headers to evade IP blocks.
    """
    if mobile:
        ua = f"Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110, 120)}.0.0.0 Mobile Safari/537.36"
    else:
        ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110, 120)}.0.0.0 Safari/537.36"

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
    """
    Smart name resolver. 
    1. Removes suffixes (.CO, .DE).
    2. Maps known difficult tickers.
    3. Returns clean name.
    """
    clean_ticker = ticker.upper().split('.')[0]
    
    # Map for known difficult tickers
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
    
    return mapping.get(clean_ticker, clean_ticker)

# --- 3. MULTI-ENGINE SEARCH ORCHESTRATOR ---

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

def search_brave(query):
    """Engine 2: Brave Search (Cloud Friendly)"""
    try:
        url = "https://search.brave.com/search"
        params = {'q': query + " site:investing.com earnings call transcript", 'source': 'web'}
        sess = get_session()
        resp = sess.get(url, params=params, timeout=10)
        if resp.status_code != 200: return []
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if "investing.com" in a['href']: links.append(a['href'])
        return links
    except: return []

def search_yahoo(query):
    """Engine 3: Yahoo Search (Backup)"""
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
            # Decode Yahoo Redirects
            if "RU=" in href:
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
    """
    Rotates through search engines until links are found.
    """
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name} (Ticker: {ticker})")
    
    # 1. Try DuckDuckGo
    links = search_duckduckgo(name)
    if links: 
        logger.info(f"✅ DuckDuckGo found {len(links)} links")
        return filter_links(links)
        
    # 2. Try Brave
    logger.info("⚠️ DDG failed. Switching to Brave...")
    links = search_brave(name)
    if links:
        logger.info(f"✅ Brave found {len(links)} links")
        return filter_links(links)

    # 3. Try Yahoo
    logger.info("⚠️ Brave failed. Switching to Yahoo...")
    links = search_yahoo(name)
    if links:
        logger.info(f"✅ Yahoo found {len(links)} links")
        return filter_links(links)
        
    return []

def filter_links(raw_links):
    """Strictly ensures links are transcripts."""
    valid = []
    seen = set()
    for l in raw_links:
        if l in seen: continue
        seen.add(l)
        
        if "investing.com" in l and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings" in l.lower():
                valid.append(l)
    return valid

# --- 4. FETCHING (Google Translate Proxy) ---

def clean_text(soup):
    # Locate content container
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.find('div', class_='article-content')

    if not body: return None

    # Remove junk
    for tag in body(["script", "style", "iframe", "aside", "button", "figure", "span"]): 
        if tag.name == "span" and "img" in str(tag): tag.decompose()
        elif tag.name != "span": tag.decompose()
    
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'img', 'share']):
            div.decompose()

    # Extract clean paragraphs
    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 30 and "Position:" not in txt and "confidential tip" not in txt:
            text_parts.append(txt)
            
    return "\n\n".join(text_parts)

def fetch_via_google_translate(url):
    """
    Proxies the request through Google Translate to bypass 403 blocks.
    """
    try:
        # Translate English -> English
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        logger.info(f"🛡️ Proxy Fetch: {url}")
        
        sess = get_session(mobile=False)
        resp = sess.get(proxy_url, timeout=20)
        
        if resp.status_code != 200: return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        text = clean_text(soup)
        
        # If proxy wrapper hides content in iframe
        if not text or len(text) < 500:
            iframe = soup.find('iframe', {'name': 'c'})
            if iframe and iframe.get('src'):
                logger.info("   ↳ Following Proxy Iframe...")
                resp2 = sess.get(iframe['src'], timeout=15)
                text = clean_text(BeautifulSoup(resp2.content, 'html.parser'))

        return text
    except Exception as e:
        logger.error(f"Proxy Error: {e}")
        return None

def fetch_direct(url):
    """Direct fetch attempt (low success rate on cloud, but fast)."""
    try:
        sess = get_session(mobile=False)
        resp = sess.get(url, timeout=10)
        if resp.status_code != 200: return None
        return clean_text(BeautifulSoup(resp.content, 'html.parser'))
    except: return None

# --- 5. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Find Candidates (Multi-Engine)
    candidates = get_candidates(ticker)
    
    if not candidates:
        logger.error("❌ No links found on any search engine.")
        return None, {"error": "Search failed on DDG, Brave, and Yahoo."}
    
    # 2. Fetch Content (Try top 3)
    for link in candidates[:3]:
        
        # A. Try Proxy (Most Reliable)
        text = fetch_via_google_translate(link)
        
        # B. Try Direct (Fallback)
        if not text:
            text = fetch_direct(link)
            
        if text and len(text) > 1000:
            logger.info("✅ Transcript Scraped Successfully")
            return text, {
                "source": "Investing.com",
                "url": link,
                "title": "Earnings Call Transcript",
                "symbol": ticker
            }
            
    return None, {"error": "Found links but failed to fetch content (Blocked)."}

if __name__ == "__main__":
    # Test
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS! Length: {len(t)}")
