import sys
import logging
import urllib.parse
import re
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

# Try curl_cffi for Direct Fetch (Fallback)
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded.")
except ImportError:
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found.")

# --- 2. SESSION FACTORY ---

def get_session():
    """Standard session for Jina interactions"""
    return std_requests.Session()

def get_cffi_session():
    """Browser session for Direct Fetch / Cache"""
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

# --- 3. JINA SEARCH IMPLEMENTATION ---

def search_jina(query):
    """
    Uses Jina's Search Endpoint (s.jina.ai) to find URLs.
    This bypasses Google/DDG blocks on cloud IPs.
    """
    try:
        logger.info("   🔍 Strategy: Jina Search (s.jina.ai)...")
        
        # We construct a specific query to target Investing.com transcripts
        search_query = f"{query} site:investing.com earnings call transcript"
        jina_search_url = f"https://s.jina.ai/{urllib.parse.quote(search_query)}"
        
        headers = {
            "Authorization": f"Bearer {JINA_API_KEY}",
            "X-Retain-Images": "none"
        }
        
        # Jina Search returns a Markdown summary of search results
        resp = std_requests.get(jina_search_url, headers=headers, timeout=20)
        
        if resp.status_code != 200:
            logger.warning(f"      ↳ Jina Search Failed: {resp.status_code}")
            return []
            
        text = resp.text
        
        # Extract URLs using Regex
        # We look for links that match investing.com structure
        # Pattern looks for: (https://www.investing.com/news/transcripts/...)
        urls = re.findall(r'\((https://www\.investing\.com/news/transcripts/[^\)]+)\)', text)
        
        # Clean and deduplicate
        clean_urls = list(set(urls))
        return clean_urls

    except Exception as e:
        logger.error(f"      ↳ Jina Search Error: {e}")
        return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name}")
    
    # Use Jina Search
    candidates = search_jina(name)
    
    if not candidates:
        logger.warning("   ⚠️ No candidates found via Jina Search.")
        return []

    logger.info(f"✅ Found {len(candidates)} candidates.")
    if candidates: logger.info(f"   🌟 Top Pick: {candidates[0]}")
    
    return candidates

# --- 4. TEXT CLEANING & VALIDATION ---

def is_valid_content(text):
    if not text or len(text) < 500: return False
    error_flags = [
        "Service Unavailable", 
        "down for maintenance", 
        "Error 503", 
        "Access to this page has been denied", 
        "Pardon Our Interruption",
        "Just a moment..."
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
            
            # Post-processing to strip headers/footers
            start_markers = ["**Full transcript -", "Earnings call transcript:", "Participants", "Operator"]
            for m in start_markers:
                if m in text: 
                    text = text[text.find(m):]
                    break
            
            # Cut off footer
            end_markers = ["Risk Disclosure:", "Fusion Media", "Comments"]
            for m in end_markers:
                if m in text:
                    text = text[:text.find(m)]
                    break
                    
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
        return None, {"error": "No candidates found (Search Blocked)"}
    
    for link in candidates:
        logger.info(f"🔗 Target: {link}")
        
        # 1. Jina Reader
        text = fetch_jina_proxy(link)
        if text: return text, {"source": "Investing.com (Jina)", "url": link}
        
        # 2. Google Cache
        text = fetch_google_cache(link)
        if text: return text, {"source": "Investing.com (Cache)", "url": link}
        
        # 3. Direct Fetch
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
