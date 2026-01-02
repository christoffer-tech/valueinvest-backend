import sys
import logging
import urllib.parse
import re
import random
import json
import requests as std_requests
from bs4 import BeautifulSoup

# --- 1. CONFIGURATION ---
JINA_API_KEY = "jina_18edc5ecbee44fceb94ea05a675f2fd5NYFCvhRikOR-aCOpgK0KCRywSnaq"

logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try curl_cffi for Direct Fetch & Bing Search
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded.")
except ImportError:
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Bing strategy will be weaker.")

# --- 2. SESSION FACTORY ---

def get_cffi_session():
    """Browser session for Direct Fetch / Search"""
    ver = random.choice(["120", "124", "119"])
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver}.0.0.0 Safari/537.36"
    
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
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
        "NVDA": "Nvidia"
    }
    return mapping.get(t, t)

# --- 3. SEARCH STRATEGIES ---

def search_jina(query):
    """Strategy A: Jina Search (s.jina.ai) - Increased Timeout"""
    try:
        logger.info("   🔍 Strategy A: Jina Search...")
        search_query = f"{query} site:investing.com earnings call transcript"
        jina_search_url = f"https://s.jina.ai/{urllib.parse.quote(search_query)}"
        
        headers = {"Authorization": f"Bearer {JINA_API_KEY}", "X-Retain-Images": "none"}
        
        # INCREASED TIMEOUT to 60s
        resp = std_requests.get(jina_search_url, headers=headers, timeout=60)
        
        if resp.status_code != 200:
            logger.warning(f"      ↳ Jina Search Failed: {resp.status_code}")
            return []
            
        urls = re.findall(r'\((https://www\.investing\.com/news/transcripts/[^\)]+)\)', resp.text)
        return list(set(urls))
    except Exception as e:
        logger.warning(f"      ↳ Jina Error: {e}")
        return []

def search_archive_cdx(query, ticker):
    """
    Strategy B: Archive.org CDX Index.
    This bypasses search engines by querying the Wayback Machine's index of investing.com directly.
    """
    try:
        logger.info("   🔍 Strategy B: Archive.org CDX Index...")
        # Query for all transcript URLs under investing.com
        cdx_url = "https://web.archive.org/cdx/search/cdx"
        params = {
            "url": "investing.com/news/transcripts/*",
            "output": "json",
            "collapse": "urlkey",
            "limit": "3000",  # Get last 3000 transcripts indexed
            "fl": "original"  # Field: original url
        }
        
        resp = std_requests.get(cdx_url, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json() # Returns list of lists: [["original"], ["http..."], ...]
            
            candidates = []
            # Normalize query parts (e.g., "Vestas" -> "vestas")
            q_parts = query.lower().split()
            ticker_clean = ticker.split('.')[0].lower()
            
            for row in data:
                url = row[0]
                url_lower = url.lower()
                
                # Check if URL matches the company
                if ticker_clean in url_lower or any(q in url_lower for q in q_parts):
                    candidates.append(url)
            
            logger.info(f"      ↳ CDX found {len(candidates)} matches.")
            return candidates
    except Exception as e:
        logger.warning(f"      ↳ CDX Error: {e}")
        return []
    return []

def search_bing(query):
    """Strategy C: Bing HTML Search (via curl_cffi)"""
    try:
        logger.info("   🔍 Strategy C: Bing Search...")
        url = "https://www.bing.com/search"
        params = {'q': query + " site:investing.com earnings call transcript"}
        
        sess = get_cffi_session()
        resp = sess.get(url, params=params, timeout=10)
        
        if resp.status_code != 200: return []
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        # Bing organic results often in <h2><a> or <div class="b_algo"><h2><a>
        for h2 in soup.find_all('h2'):
            a = h2.find('a', href=True)
            if a:
                l = a['href']
                if "investing.com" in l and "transcript" in l.lower():
                    links.append(l)
        return list(set(links))
    except Exception as e:
        logger.warning(f"      ↳ Bing Error: {e}")
        return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name}")
    
    # 1. Try Jina
    candidates = search_jina(name)
    
    # 2. Try Archive CDX (Very reliable fallback)
    if not candidates:
        candidates = search_archive_cdx(name, ticker)
        
    # 3. Try Bing
    if not candidates:
        candidates = search_bing(name)
    
    # Sort by recency (Year/Quarter in URL)
    candidates.sort(key=lambda x: x if "2025" in x else "0", reverse=True)
    
    if candidates: 
        logger.info(f"✅ Found {len(candidates)} candidates.")
        logger.info(f"   🌟 Top Pick: {candidates[0]}")
    else:
        logger.warning("❌ No candidates found.")
        
    return candidates

# --- 4. TEXT CLEANING & VALIDATION ---

def is_valid_content(text):
    if not text or len(text) < 500: return False
    error_flags = [
        "Service Unavailable", 
        "down for maintenance", 
        "Error 503", 
        "Access to this page has been denied", 
        "Pardon Our Interruption"
    ]
    if any(flag in text for flag in error_flags): return False
    return True

def clean_text(soup):
    body = soup.find('div', class_='WYSIWYG') or soup.find('div', class_='articlePage') or soup.body
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
            
            # Post-processing
            for m in ["**Full transcript -", "Earnings call transcript:", "Participants"]:
                if m in text: text = text[text.find(m):]; break
            for m in ["Risk Disclosure:", "Fusion Media"]:
                if m in text: text = text[:text.find(m)]; break
            return text.strip()
    except: pass
    return None

def fetch_google_cache(url):
    try:
        logger.info(f"   💾 Trying Google Cache...")
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        sess = get_cffi_session()
        resp = sess.get(cache_url, timeout=15)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if is_valid_content(text): return text
    except: pass
    return None

def fetch_direct(url):
    try:
        logger.info(f"   ⚡ Trying Direct Fetch...")
        sess = get_cffi_session()
        resp = sess.get(url, timeout=20)
        if resp.status_code == 200:
            text = clean_text(BeautifulSoup(resp.content, 'html.parser'))
            if is_valid_content(text): return text
    except: pass
    return None

# --- 6. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    candidates = get_candidates(ticker)
    
    if not candidates:
        return None, {"error": "No candidates found (All Search Methods Failed)"}
    
    # Try top 3 candidates
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        text = fetch_jina_proxy(link)
        if text: return text, {"source": "Investing.com (Jina)", "url": link}
        
        text = fetch_google_cache(link)
        if text: return text, {"source": "Investing.com (Cache)", "url": link}
        
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
