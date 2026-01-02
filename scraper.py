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

# --- 2. INTELLIGENT TICKER LOGIC (From Colab) ---

def normalize_ticker(symbol):
    """Converts tickers to standard formats (e.g. VWS.CO -> VWS.CO)."""
    if not symbol or '.' not in symbol:
        return symbol
    parts = symbol.split('.')
    base = parts[0]
    suffix = parts[1].upper()
    # Map common suffixes to Yahoo/Investing standards
    mapping = {
        'TOK': 'T', 'PAR': 'PA', 'LON': 'L', 'TRT': 'TO',
        'AMS': 'AS', 'BRU': 'BR', 'ETR': 'DE', 'FRA': 'F', 'HKG': 'HK',
        'CPH': 'CO' 
    }
    if suffix in mapping:
        return f"{base}.{mapping[suffix]}"
    return symbol

def resolve_name(ticker):
    """
    Resolves ticker to company name for better search results.
    """
    normalized = normalize_ticker(ticker)
    t = normalized.split('.')[0] # Strip suffix for mapping check
    
    # Hardcoded map for difficult tickers
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "PNDORA": "Pandora A/S",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "NVDA": "Nvidia"
    }
    return mapping.get(t, normalized)

def parse_quarter_score(text):
    """
    Scores a URL/Title based on recency.
    Higher Score = Newer Transcript.
    """
    if not text: return 0
    text = text.upper()
    
    # Extract Year
    year_match = re.search(r'20(\d{2})', text)
    year = int("20" + year_match.group(1)) if year_match else 2020
    
    # Extract Quarter
    q_map = {
        "Q1": 1, "1Q": 1, "FIRST": 1,
        "Q2": 2, "2Q": 2, "SECOND": 2, "HALF": 2,
        "Q3": 3, "3Q": 3, "THIRD": 3,
        "Q4": 4, "4Q": 4, "FOURTH": 4, "FY": 4
    }
    quarter = 0
    for k, v in q_map.items():
        if k in text:
            quarter = v
            break
            
    # Score = Year * 10 + Quarter (e.g., 2025 Q3 = 20253)
    return (year * 10) + quarter

# --- 3. SESSION FACTORY ---

def get_session(mobile=False):
    if mobile:
        ua = f"Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 130)}.0.0.0 Mobile Safari/537.36"
    else:
        ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 130)}.0.0.0 Safari/537.36"

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

# --- 4. SEARCH & FILTERING ---

def clean_yahoo_url(raw_url):
    try:
        decoded = urllib.parse.unquote(raw_url)
        match = re.search(r'(https?://(?:www\.)?investing\.com/[^\s&/]+(?:/[^\s&]+)*)', decoded)
        if match: return match.group(1)
        return None
    except: return None

def search_yahoo(query):
    try:
        full_query = f"{query} site:investing.com earnings call transcript"
        url = "https://search.yahoo.com/search"
        params = {'p': full_query, 'ei': 'UTF-8', 'n': 10}
        
        sess = get_session()
        resp = sess.get(url, params=params, timeout=10)
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
        logger.error(f"Yahoo Error: {e}")
        return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching Yahoo for: {name}")
    
    raw_links = search_yahoo(name)
    logger.info(f"✅ Yahoo found {len(raw_links)} raw links")
    
    valid_candidates = []
    for l in raw_links:
        if "investing.com" in l and ("transcript" in l.lower() or "earnings" in l.lower()):
             if "/news/" in l or "/equities/" in l or "/stock-market-news/" in l:
                 # Calculate Recency Score
                 score = parse_quarter_score(l)
                 valid_candidates.append({'url': l, 'score': score})
    
    # Sort by Score (Desc) -> Newest First
    valid_candidates.sort(key=lambda x: x['score'], reverse=True)
    
    # Return just the URLs
    sorted_urls = [x['url'] for x in valid_candidates]
    logger.info(f"✅ Filtered to {len(sorted_urls)} valid candidates (Newest First).")
    return sorted_urls

# --- 5. FETCHING (Google Cache + Archive Fallback) ---

def clean_text(soup):
    # Try multiple containers
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
    """Strategy A: Google Cache (Text Only)"""
    try:
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        logger.info(f"🛡️ Google Cache: {cache_url}")
        
        sess = get_session()
        sess.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        
        resp = sess.get(cache_url, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            text = clean_text(soup)
            if not text: text = soup.get_text() # Fallback
            return text
        return None
    except: return None

def fetch_archive_org(url):
    """Strategy B: Wayback Machine (From Colab Script)"""
    try:
        logger.info(f"🏛️ Checking Archive.org for: {url}")
        api_url = f"https://archive.org/wayback/available?url={url}"
        
        sess = get_session()
        resp = sess.get(api_url, timeout=15)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get('archived_snapshots', {}).get('closest', {}):
                snapshot_url = data['archived_snapshots']['closest']['url']
                logger.info(f"   ↳ Found Snapshot: {snapshot_url}")
                
                resp_snap = sess.get(snapshot_url, timeout=25)
                if resp_snap.status_code == 200:
                    soup = BeautifulSoup(resp_snap.content, 'html.parser')
                    return clean_text(soup)
        return None
    except Exception as e:
        logger.error(f"Archive Error: {e}")
        return None

# --- 6. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search
    candidates = get_candidates(ticker)
    if not candidates:
        return None, {"error": "No valid transcript links found."}
    
    # 2. Fetch Loop
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        # A. Google Cache (Fastest)
        text = fetch_google_cache(link)
        if text and len(text) > 500:
            return text, {"source": "Investing.com (Google Cache)", "url": link, "title": "Earnings Call", "symbol": ticker}
            
        # B. Archive.org (Resilient Fallback)
        text = fetch_archive_org(link)
        if text and len(text) > 500:
            return text, {"source": "Investing.com (Wayback Machine)", "url": link, "title": "Earnings Call", "symbol": ticker}

    return None, {"error": "All candidates blocked or unavailable."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
