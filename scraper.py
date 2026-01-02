import sys
import logging
import urllib.parse
import re
import time
import random
import requests as std_requests
from bs4 import BeautifulSoup

# --- 1. CONFIGURATION ---
JINA_API_KEY = "jina_18edc5ecbee44fceb94ea05a675f2fd5NYFCvhRikOR-aCOpgK0KCRywSnaq"

logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try curl_cffi for Direct Fetch fallback
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded.")
except ImportError:
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Using standard requests.")

# --- 2. SESSION FACTORY ---

def get_cffi_session():
    """Browser session for Direct Fetch & Search"""
    ver = random.choice(["120", "124", "119"])
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver}.0.0.0 Safari/537.36"
    
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1"
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
        "NVDA": "Nvidia",
        "AAPL": "Apple",
        "MSFT": "Microsoft"
    }
    return mapping.get(t, t)

# --- 3. SEARCH & RANKING (The Fix for Old Transcripts) ---

def parse_quarter_score(text):
    """
    Scores a URL based on recency.
    Higher Score = Newer Transcript.
    Example: 'Q3 2025' -> 20253
    """
    if not text: return 0
    text = text.upper()
    
    # Extract Year (Default to 2024 if missing, but usually present in URL)
    year_match = re.search(r'20(\d{2})', text)
    year = int("20" + year_match.group(1)) if year_match else 2024
    
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

def search_duckduckgo(query):
    try:
        url = "https://html.duckduckgo.com/html/"
        # We explicitly search for "earnings call transcript" to filter noise
        data = {'q': query + " site:investing.com earnings call transcript"}
        sess = get_cffi_session() 
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
    
    raw_links = search_duckduckgo(name)
    candidates = []
    seen = set()
    
    for l in raw_links:
        if l in seen: continue
        seen.add(l)
        
        # Strict Filter: Must be investing.com and look like a transcript
        if "investing.com" in l and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings" in l.lower():
                # Score the link based on Q3/Q2/Year
                score = parse_quarter_score(l)
                candidates.append({'url': l, 'score': score})
    
    # Sort by Score Descending (Highest/Newest first)
    candidates.sort(key=lambda x: x['score'], reverse=True)
    
    sorted_urls = [x['url'] for x in candidates]
    logger.info(f"✅ Found {len(sorted_urls)} candidates (Sorted by Recency).")
    
    if sorted_urls:
        logger.info(f"   🌟 Top Pick: {sorted_urls[0]}")
        
    return sorted_urls

# --- 4. TEXT CLEANER ---

def clean_text(soup):
    # Try generic containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.body
    if not body: return None
    
    for tag in body(["script", "style", "iframe", "button", "figure", "aside", "nav", "footer"]): tag.decompose()
    for div in body.find_all('div'):
        if any(c in str(div.get('class', [])) for c in ['related', 'ad', 'share', 'img']): div.decompose()
        
    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 30 and "Position:" not in txt: text_parts.append(txt)
    return "\n\n".join(text_parts)

# --- 5. FETCHING STRATEGIES ---

def fetch_jina_proxy(url):
    """STRATEGY 1: Jina Reader API (Authenticated & Cleaned)"""
    try:
        logger.info(f"   🤖 Trying Jina Reader...")
        jina_url = f"https://r.jina.ai/{url}"
        
        headers = {
            "Authorization": f"Bearer {JINA_API_KEY}",
            "X-Respond-With": "markdown",
            "X-No-Cache": "true",
            "X-With-Generated-Alt": "false"
        }
        
        resp = std_requests.get(jina_url, headers=headers, timeout=40)
        
        if resp.status_code == 200:
            text = resp.text
            if "Access to this page has been denied" in text or len(text) < 200:
                logger.warning("      ↳ Jina Blocked.")
                return None
            
            # --- POST-PROCESSING CLEANUP ---
            # 1. Find Start (Skip menus/tickers)
            start_markers = ["**Full transcript -", "Earnings call transcript:", "Participants", "Operator"]
            start_idx = -1
            for marker in start_markers:
                idx = text.find(marker)
                if idx != -1:
                    start_idx = idx
                    break
            
            if start_idx != -1:
                text = text[start_idx:] 
            
            # 2. Find End (Skip footer/disclaimers)
            end_markers = ["Risk Disclosure:", "Fusion Media", "Comments", "Terms And Conditions"]
            for marker in end_markers:
                idx = text.find(marker)
                if idx != -1:
                    text = text[:idx]
                    break
            
            logger.info("      ↳ Jina Success! (Cleaned)")
            return text.strip()
        return None
    except Exception as e: return None

def fetch_google_cache(url):
    """STRATEGY 2: Google Web Cache (Text Only)"""
    try:
        logger.info(f"   💾 Trying Google Cache...")
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        
        sess = get_cffi_session()
        sess.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        
        resp = sess.get(cache_url, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            text = clean_text(soup) or soup.get_text()
            if text and len(text) > 500: return text
        elif resp.status_code == 404:
            logger.info("      ↳ Cache Miss")
        return None
    except: return None

def fetch_archive(url):
    """STRATEGY 3: Archive.today / Wayback"""
    try:
        logger.info(f"   🏛️ Trying Archive Mirrors...")
        # 1. Wayback Machine API
        wb_api = f"https://archive.org/wayback/available?url={url}"
        resp = std_requests.get(wb_api, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('archived_snapshots', {}).get('closest', {}):
                snap_url = data['archived_snapshots']['closest']['url']
                resp_snap = std_requests.get(snap_url, timeout=20)
                if resp_snap.status_code == 200:
                    text = clean_text(BeautifulSoup(resp_snap.content, 'html.parser'))
                    if text and len(text) > 500: return text
        
        # 2. Archive.today
        if SESSION_TYPE == "cffi":
            sess = get_cffi_session()
            archive_url = f"https://archive.today/newest/{url}"
            resp = sess.get(archive_url, timeout=20)
            if resp.status_code == 200:
                text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
                if text and len(text) > 500: return text
        return None
    except: return None

def fetch_direct(url):
    """STRATEGY 4: Direct Fetch"""
    try:
        logger.info(f"   ⚡ Trying Direct Fetch...")
        sess = get_cffi_session()
        resp = sess.get(url, timeout=20)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if text and len(text) > 500: return text
        return None
    except: return None

# --- 6. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search & Rank
    candidates = get_candidates(ticker)
    if not candidates:
        return None, {"error": "No candidates found."}
    
    # 2. Fetch Loop
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        # Priority 1: Jina
        text = fetch_jina_proxy(link)
        if text: return text, {"source": "Investing.com (Jina)", "url": link, "title": "Earnings Call", "symbol": ticker}

        # Priority 2: Google Cache
        text = fetch_google_cache(link)
        if text: return text, {"source": "Investing.com (Cache)", "url": link, "title": "Earnings Call", "symbol": ticker}
        
        # Priority 3: Archive
        text = fetch_archive(link)
        if text: return text, {"source": "Investing.com (Archive)", "url": link, "title": "Earnings Call", "symbol": ticker}

        # Priority 4: Direct
        text = fetch_direct(link)
        if text: return text, {"source": "Investing.com (Direct)", "url": link, "title": "Earnings Call", "symbol": ticker}

    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
