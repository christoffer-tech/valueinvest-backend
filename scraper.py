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
    logger.info("✅ curl_cffi loaded (Cloudflare Bypass Ready).")
except ImportError:
    import requests as std_requests
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Using standard requests (High Block Risk).")

# --- 2. SESSION FACTORY ---

def get_session(mobile=False):
    """
    Creates a browser session with randomized fingerprints.
    """
    if mobile:
        # Modern Android User-Agent
        ua = f"Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 132)}.0.0.0 Mobile Safari/537.36"
        plat = "Android"
    else:
        # Modern Desktop User-Agent
        ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 132)}.0.0.0 Safari/537.36"
        plat = "Windows"

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": f'"Chromium";v="{random.randint(120, 132)}", "Google Chrome";v="{random.randint(120, 132)}", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?1" if mobile else "?0",
        "Sec-Ch-Ua-Platform": f'"{plat}"'
    }

    if SESSION_TYPE == "cffi":
        # Impersonate a real browser TLS handshake
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
        "MSFT": "Microsoft",
        "NVDA": "Nvidia"
    }
    return mapping.get(t, t)

# --- 3. SEARCH (Yahoo - Working) ---

def clean_yahoo_url(raw_url):
    try:
        decoded = urllib.parse.unquote(raw_url)
        match = re.search(r'(https?://(?:www\.)?investing\.com/[^\s&/]+(?:/[^\s&]+)*)', decoded)
        if match: return match.group(1)
        return None
    except: return None

def search_yahoo(query):
    try:
        # Search specifically for transcript pages
        full_query = f"{query} site:investing.com earnings call transcript"
        url = "https://search.yahoo.com/search"
        params = {'p': full_query, 'ei': 'UTF-8', 'n': 10}
        
        sess = get_session()
        resp = sess.get(url, params=params, timeout=15)
        
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        links = []
        for a in soup.find_all('a', href=True):
            raw_href = a['href']
            if "investing.com" in raw_href or "yahoo.com" in raw_href:
                cleaned = clean_yahoo_url(raw_href)
                if cleaned: links.append(cleaned)
        return list(set(links))
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name}")
    
    links = search_yahoo(name)
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

# --- 4. FETCHING (The Kitchen Sink) ---

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

def fetch_direct(url, mobile=False):
    """Method 1 & 2: Direct Fetch (Desktop/Mobile)"""
    mode = "Mobile" if mobile else "Desktop"
    try:
        logger.info(f"   ⚡ Trying Direct {mode} Fetch...")
        sess = get_session(mobile=mobile)
        resp = sess.get(url, timeout=25) # Long timeout
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            text = clean_text(soup)
            if text and len(text) > 500: return text
        else:
            logger.warning(f"      ↳ Failed ({resp.status_code})")
        return None
    except Exception as e:
        logger.warning(f"      ↳ Error: {e}")
        return None

def fetch_via_translate(url):
    """Method 3: Google Translate Proxy"""
    try:
        logger.info(f"   🛡️ Trying Translate Proxy...")
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        sess = get_session()
        resp = sess.get(proxy_url, timeout=30)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Check for iframe (common in proxy)
            iframe = soup.find('iframe', {'name': 'c'})
            if iframe and iframe.get('src'):
                resp = sess.get(iframe['src'], timeout=20)
                soup = BeautifulSoup(resp.content, 'html.parser')
            
            text = clean_text(soup)
            if text and len(text) > 500: return text
        return None
    except: return None

def fetch_wayback(url):
    """Method 4: Wayback Machine"""
    try:
        logger.info(f"   🏛️ Trying Wayback Machine...")
        api_url = f"https://archive.org/wayback/available?url={url}"
        sess = get_session()
        resp = sess.get(api_url, timeout=15)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get('archived_snapshots', {}).get('closest', {}):
                snap_url = data['archived_snapshots']['closest']['url']
                resp_snap = sess.get(snap_url, timeout=30)
                if resp_snap.status_code == 200:
                    soup = BeautifulSoup(resp_snap.content, 'html.parser')
                    text = clean_text(soup)
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
    for link in candidates[:3]: # Try top 3 links
        logger.info(f"🔗 Target: {link}")
        
        # 1. Direct Desktop (Best Quality)
        text = fetch_direct(link, mobile=False)
        if text: return text, {"source": "Investing.com", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # 2. Direct Mobile (Bypass some blocks)
        time.sleep(1)
        text = fetch_direct(link, mobile=True)
        if text: return text, {"source": "Investing.com (Mobile)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # 3. Translate Proxy (Bypass IP Block)
        time.sleep(1)
        text = fetch_via_translate(link)
        if text: return text, {"source": "Investing.com (Proxy)", "url": link, "title": "Earnings Call", "symbol": ticker}

        # 4. Wayback Machine (Last Resort)
        text = fetch_wayback(link)
        if text: return text, {"source": "Investing.com (Archive)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
