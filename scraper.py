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

# --- 3. SEARCH (Yahoo Search - Low Block Rate) ---

def search_yahoo(query):
    """
    Uses Yahoo Search to find links. Yahoo is friendlier to Cloud IPs.
    """
    try:
        # We search specifically for investing.com transcripts
        full_query = f"{query} site:investing.com earnings call transcript"
        logger.info(f"🔎 Searching Yahoo for: {full_query}")
        
        # Yahoo Search URL
        url = "https://search.yahoo.com/search"
        params = {'p': full_query, 'ei': 'UTF-8'}
        
        sess = get_session(mobile=False)
        resp = sess.get(url, params=params, timeout=15)
        
        if resp.status_code != 200:
            logger.warning(f"Yahoo Search failed: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # Yahoo results are usually in 'h3 > a' or 'a.d-ib'
        # We grab all hrefs and filter
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Yahoo wraps links (r.search.yahoo...), we need to decode them or find direct ones.
            # Usually the 'href' is the direct link in modern Yahoo output, 
            # but sometimes it's wrapped.
            
            if "investing.com" in href and ("transcript" in href.lower() or "earnings" in href.lower()):
                # Clean up yahoo tracking if present
                if "/RU=" in href:
                    # Extract real URL from Yahoo redirect
                    try:
                        start = href.find("/RU=") + 4
                        end = href.find("/RK=")
                        if start > 4 and end > start:
                            href = urllib.parse.unquote(href[start:end])
                    except:
                        pass
                
                links.append(href)
                
        # Remove duplicates
        return list(set(links))

    except Exception as e:
        logger.error(f"Yahoo Search Error: {e}")
        return []

# --- 4. FETCHING (Google Translate Proxy) ---

def clean_text(soup):
    """Robust text cleaner"""
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', class_='article-content') or \
           soup.find('div', id='article-content')

    if not body: 
        # Fallback: Just look for large blocks of text
        ps = soup.find_all('p')
        if len(ps) > 10:
            return "\n\n".join([p.get_text().strip() for p in ps if len(p.get_text()) > 50])
        return None

    for tag in body(["script", "style", "iframe", "aside", "button", "figure"]): 
        tag.decompose()
    
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'img']):
            div.decompose()

    text_parts = []
    for p in body.find_all(['p', 'h2']):
        txt = p.get_text().strip()
        if len(txt) > 20 and "Position:" not in txt:
            text_parts.append(txt)
            
    return "\n\n".join(text_parts)

def fetch_via_google_translate(url):
    """
    Proxy fetch using Google Translate to bypass Investing.com 403 blocks.
    """
    try:
        # Translate English -> English (Proxy)
        proxy_url = f"https://translate.google.com/translate?sl=en&tl=en&u={urllib.parse.quote(url)}"
        logger.info(f"🛡️ Engaging Google Translate Proxy: {proxy_url}")
        
        sess = get_session(mobile=False)
        resp = sess.get(proxy_url, timeout=20)
        
        if resp.status_code != 200:
            return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Google Translate wraps content in an iframe usually, but simple GET often returns the hydrated page
        # We assume standard parsing first
        text = clean_text(soup)
        
        # If text is suspiciously short, it might be stuck in the iframe source
        if not text or len(text) < 500:
            # Try to find the iframe source URL if present
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
    """Direct fetch (Desktop) - unlikely to work but worth 1 shot."""
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
    
    # 1. Search (Yahoo)
    links = search_yahoo(name)
    
    if not links:
        logger.error("❌ No links found via Yahoo.")
        return None, {"error": "No links found"}

    logger.info(f"✅ Found {len(links)} candidates via Yahoo.")
    
    # 2. Iterate candidates
    for link in links[:3]: # Check top 3
        # A. Try Direct
        text = fetch_direct(link)
        if text and len(text) > 1000:
            return text, {"source": "Investing.com", "url": link, "title": "Earnings Call", "symbol": ticker}
            
        # B. Try Google Translate Proxy
        text = fetch_via_google_translate(link)
        if text and len(text) > 1000:
            return text, {"source": "Investing.com (Via Proxy)", "url": link, "title": "Earnings Call", "symbol": ticker}
            
    logger.error("❌ All fetch methods failed.")
    return None, {"error": "Server blocked by Investing.com"}

if __name__ == "__main__":
    # Local Test
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
