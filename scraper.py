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

# Try curl_cffi for Direct Fetch & Google Search
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded (Enhanced Stealth Mode).")
except ImportError:
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Google Search strategy may fail.")

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
        # impersonate="chrome120" is crucial for bypassing Google/Cloudflare blocks
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

# --- 3. SEARCH STRATEGIES (New & Improved) ---

def parse_quarter_score(text):
    if not text: return 0
    text = text.upper()
    year_match = re.search(r'20(\d{2})', text)
    year = int("20" + year_match.group(1)) if year_match else 2024
    q_map = {"Q1": 1, "1Q": 1, "FIRST": 1, "Q2": 2, "2Q": 2, "HALF": 2, "Q3": 3, "3Q": 3, "Q4": 4, "4Q": 4, "FY": 4}
    quarter = 0
    for k, v in q_map.items():
        if k in text:
            quarter = v
            break
    return (year * 10) + quarter

def search_google_cffi(query):
    """Strategy A: Google Search via curl_cffi (Best for Cloud IPs)"""
    try:
        logger.info("   🔍 Strategy A: Google Search...")
        url = "https://www.google.com/search"
        params = {'q': query + " site:investing.com earnings call transcript"}
        
        sess = get_cffi_session()
        resp = sess.get(url, params=params, timeout=10)
        
        if resp.status_code != 200:
            logger.warning(f"      ↳ Google Blocked/Error: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # Parse Google Results (Standard Structure)
        for g in soup.find_all('div', class_='g'):
            a = g.find('a', href=True)
            if a and 'href' in a.attrs:
                link = a['href']
                if "investing.com" in link:
                    links.append(link)
        
        return list(set(links))
    except Exception as e:
        logger.error(f"      ↳ Google Search Failed: {e}")
        return []

def search_ddg_lite(query):
    """Strategy B: DuckDuckGo Lite (HTML version, less strict)"""
    try:
        logger.info("   🔍 Strategy B: DuckDuckGo Lite...")
        url = "https://lite.duckduckgo.com/lite/"
        data = {'q': query + " site:investing.com earnings call transcript"}
        sess = get_cffi_session()
        resp = sess.post(url, data=data, timeout=10)
        
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        links = []
        for a in soup.find_all('a', class_='result-link', href=True):
            links.append(a['href'])
        return links
    except: return []

def search_ddg_html(query):
    """Strategy C: DuckDuckGo HTML (Original, strict on Cloud IPs)"""
    try:
        logger.info("   🔍 Strategy C: DuckDuckGo Standard...")
        url = "https://html.duckduckgo.com/html/"
        data = {'q': query + " site:investing.com earnings call transcript"}
        sess = get_cffi_session() 
        resp = sess.post(url, data=data, timeout=10)
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', class_='result__a', href=True):
            links.append(a['href'])
        return links
    except: return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name}")
    
    # 1. Try Google First (Most Robust with curl_cffi)
    raw_links = search_google_cffi(name)
    
    # 2. Fallback to DDG Lite if Google fails
    if not raw_links:
        raw_links = search_ddg_lite(name)
        
    # 3. Last resort: DDG Standard
    if not raw_links:
        raw_links = search_ddg_html(name)

    # Filter & Score
    candidates = []
    seen = set()
    for l in raw_links:
        if l in seen: continue
        seen.add(l)
        if "investing.com" in l and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings" in l.lower():
                score = parse_quarter_score(l)
                candidates.append({'url': l, 'score': score})
    
    candidates.sort(key=lambda x: x['score'], reverse=True)
    sorted_urls = [x['url'] for x in candidates]
    
    logger.info(f"✅ Found {len(sorted_urls)} candidates.")
    if sorted_urls: logger.info(f"   🌟 Top Pick: {sorted_urls[0]}")
    
    return sorted_urls

# --- 4. VALIDATION & CLEANING (FIXED) ---

def is_valid_content(text):
    """Checks if text is a valid transcript, rejecting Maintenance/Errors."""
    if not text or len(text) < 500: return False
    error_flags = ["Service Unavailable", "down for maintenance", "Error 503", "Access to this page has been denied", "Pardon Our Interruption", "Just a moment...", "Enable JavaScript"]
    if any(flag in text for flag in error_flags): return False
    return True

def clean_text(soup):
    body = soup.find('div', class_='WYSIWYG') or soup.find('div', class_='articlePage') or soup.find('div', id='article-content') or soup.body
    if not body: return None
    for tag in body(["script", "style", "iframe", "button", "figure", "aside", "nav", "footer"]): tag.decompose()
    for div in body.find_all('div'):
        if any(c in str(div.get('class', [])) for c in ['related', 'ad', 'share', 'img']): div.decompose()
    text_parts = [p.get_text().strip() for p in body.find_all(['p', 'h2']) if len(p.get_text().strip()) > 30]
    return "\n\n".join(text_parts)

# --- 5. FETCHING STRATEGIES ---

def fetch_jina_proxy(url):
    try:
        logger.info(f"   🤖 Trying Jina Reader...")
        jina_url = f"https://r.jina.ai/{url}"
        headers = {"Authorization": f"Bearer {JINA_API_KEY}", "X-Respond-With": "markdown", "X-No-Cache": "true"}
        resp = std_requests.get(jina_url, headers=headers, timeout=40)
        if resp.status_code == 200:
            text = resp.text
            if not is_valid_content(text):
                logger.warning("      ↳ Jina Blocked/Maintenance.")
                return None
            start_idx = -1
            for marker in ["**Full transcript -", "Earnings call transcript:", "Participants", "Operator"]:
                idx = text.find(marker)
                if idx != -1: start_idx = idx; break
            if start_idx != -1: text = text[start_idx:]
            end_markers = ["Risk Disclosure:", "Fusion Media", "Comments"]
            for marker in end_markers:
                idx = text.find(marker)
                if idx != -1: text = text[:idx]; break
            logger.info("      ↳ Jina Success!")
            return text.strip()
        return None
    except: return None

def fetch_google_cache(url):
    try:
        logger.info(f"   💾 Trying Google Cache...")
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        sess = get_cffi_session()
        resp = sess.get(cache_url, timeout=15)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if is_valid_content(text): return text
            else: logger.info("      ↳ Cache Invalid.")
        return None
    except: return None

def fetch_archive(url):
    try:
        logger.info(f"   🏛️ Trying Archive...")
        wb_api = f"https://archive.org/wayback/available?url={url}"
        resp = std_requests.get(wb_api, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('archived_snapshots', {}).get('closest', {}):
                snap_url = data['archived_snapshots']['closest']['url']
                resp_snap = std_requests.get(snap_url, timeout=20)
                if resp_snap.status_code == 200:
                    text = clean_text(BeautifulSoup(resp_snap.content, 'html.parser'))
                    if is_valid_content(text): return text
        return None
    except: return None

def fetch_direct(url):
    try:
        logger.info(f"   ⚡ Trying Direct Fetch...")
        sess = get_cffi_session()
        resp = sess.get(url, timeout=20)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if is_valid_content(text): return text
            else: logger.warning("      ↳ Direct hit Maintenance/Block.")
        return None
    except: return None

# --- 6. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    candidates = get_candidates(ticker)
    
    if not candidates:
        logger.error("❌ No candidates found via any search method.")
        return None, {"error": "Search failed"}
    
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        text = fetch_jina_proxy(link)
        if text: return text, {"source": "Investing.com (Jina)", "url": link}
        text = fetch_google_cache(link)
        if text: return text, {"source": "Investing.com (Cache)", "url": link}
        text = fetch_archive(link)
        if text: return text, {"source": "Investing.com (Archive)", "url": link}
        text = fetch_direct(link)
        if text: return text, {"source": "Investing.com (Direct)", "url": link}

    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: 
        print(f"\nSUCCESS! Found {len(t)} characters.")
        print("-" * 40)
        print(t[:2000])
    else:
        print("\nFAILED:", m)
