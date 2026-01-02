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
        "PNDORA": "Pandora A/S"
    }
    return mapping.get(t, t)

# --- 3. SEARCH (Yahoo Search - STRICT MODE) ---

def clean_yahoo_url(raw_url):
    """
    Extracts the REAL URL from a Yahoo redirect link.
    Example: https://r.search.yahoo.com/.../RU=https%3a%2f%2fwww.investing.com.../RK=...
    """
    try:
        # 1. If it's already clean, return it
        if raw_url.startswith("https://www.investing.com"):
            return raw_url
            
        # 2. Look for the RU= parameter (Real URL)
        if "RU=" in raw_url:
            start = raw_url.find("RU=") + 3
            end = raw_url.find("/RK=")
            if end == -1: end = len(raw_url)
            
            encoded_url = raw_url[start:end]
            real_url = urllib.parse.unquote(encoded_url)
            return real_url
            
        return None
    except:
        return None

def search_yahoo(query):
    try:
        full_query = f"{query} site:investing.com earnings call transcript"
        logger.info(f"🔎 Searching Yahoo for: {full_query}")
        
        url = "https://search.yahoo.com/search"
        params = {'p': full_query, 'ei': 'UTF-8', 'n': 5} # n=5 results
        
        sess = get_session(mobile=False)
        resp = sess.get(url, params=params, timeout=15)
        
        if resp.status_code != 200:
            logger.warning(f"Yahoo Search failed: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        valid_links = []
        
        # Yahoo Search Results are usually in 'h3 > a'
        for a in soup.find_all('a', href=True):
            raw_href = a['href']
            
            # 1. Extract the Real URL
            real_url = clean_yahoo_url(raw_href)
            if not real_url: continue
            
            # 2. STRICT FILTERING: Must be an Investing.com Article
            #    Exclude search pages, profiles, or generic tags
            if "investing.com" in real_url:
                if "/news/" in real_url or "/equities/" in real_url:
                    if "transcript" in real_url.lower() or "earnings" in real_url.lower():
                        valid_links.append(real_url)
        
        # Deduplicate
        return list(set(valid_links))

    except Exception as e:
        logger.error(f"Yahoo Search Error: {e}")
        return []

# --- 4. FETCHING (Google Translate Proxy) ---

def clean_text(soup):
    """Robust text cleaner"""
    # 1. Try finding the main content div specifically
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', class_='article-content') or \
           soup.find('div', id='article-content')

    if not body: return None

    # 2. Nuke junk elements
    for tag in body(["script", "style", "iframe", "aside", "button", "figure", "span"]): 
        # Be careful removing spans, sometimes text is in them, but usually ad-injectors use them
        if tag.name == "span" and "img" in str(tag): tag.decompose()
        elif tag.name != "span": tag.decompose()
    
    # 3. Nuke junk divs
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'img', 'share']):
            div.decompose()

    # 4. Extract text
    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        # Filter out short garbage lines or "Position:" disclaimers
        if len(txt) > 30 and "Position:" not in txt and "confidential tip" not in txt:
            text_parts.append(txt)
            
    return "\n\n".join(text_parts)

def fetch_via_google_translate(url):
    """Proxy fetch using Google Translate"""
    try:
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        logger.info(f"🛡️ Engaging Google Translate Proxy for: {url}")
        
        sess = get_session(mobile=False)
        resp = sess.get(proxy_url, timeout=20)
        
        if resp.status_code != 200: return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # 1. Try cleaning the soup directly (if proxy returned hydrated page)
        text = clean_text(soup)
        
        # 2. If empty, check for iframe (Google sometimes iframes the content)
        if not text or len(text) < 500:
            iframe = soup.find('iframe', {'name': 'c'})
            if iframe and iframe.get('src'):
                logger.info("Following Google Translate Iframe...")
                resp2 = sess.get(iframe['src'], timeout=15)
                soup2 = BeautifulSoup(resp2.content, 'html.parser')
                text = clean_text(soup2)

        return text
    except Exception as e:
        logger.error(f"Translate Proxy Error: {e}")
        return None

def fetch_direct(url):
    """Direct fetch"""
    try:
        logger.info(f"📥 Direct Fetch: {url}")
        sess = get_session(mobile=False)
        resp = sess.get(url, timeout=10)
        if resp.status_code != 200: return None
        soup = BeautifulSoup(resp.content, 'html.parser')
        return clean_text(soup)
    except:
        return None

# --- 5. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    name = resolve_name(ticker)
    
    # 1. Search
    links = search_yahoo(name)
    
    if not links:
        logger.error("❌ No links found via Yahoo.")
        return None, {"error": "No links found"}

    logger.info(f"✅ Found {len(links)} Valid Candidates.")
    
    # 2. Iterate candidates
    for link in links[:3]: 
        logger.info(f"🔗 Processing: {link}")
        
        # A. Try Direct
        text = fetch_direct(link)
        if text and len(text) > 1000:
            return text, {"source": "Investing.com", "url": link, "title": "Earnings Call", "symbol": ticker}
            
        # B. Try Proxy
        text = fetch_via_google_translate(link)
        if text and len(text) > 1000:
            return text, {"source": "Investing.com (Via Proxy)", "url": link, "title": "Earnings Call", "symbol": ticker}
            
    logger.error("❌ All fetch methods failed.")
    return None, {"error": "Server blocked by Investing.com"}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: 
        print(f"SUCCESS! URL: {m['url']}")
        print(f"Sample: {t[:500]}...")
