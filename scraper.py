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

# --- 2. SESSION FACTORY ---
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

def resolve_name(ticker):
    t = ticker.upper().split('.')[0]
    mapping = { "VWS": "Vestas Wind Systems", "VWDRY": "Vestas Wind Systems", "PNDORA": "Pandora A/S" }
    return mapping.get(t, t)

# --- 3. SEARCH & CLEANING ---

def clean_yahoo_url(raw_url):
    """
    Aggressively extracts the destination URL from Yahoo redirects.
    """
    try:
        # 1. Decode generic URL
        decoded = urllib.parse.unquote(raw_url)
        
        # 2. Regex to find https://...investing.com... inside the string
        # Looks for http(s)://...investing.com... until the next delimiter
        match = re.search(r'(https?://(?:www\.)?investing\.com/[^\s&/]+(?:/[^\s&]+)*)', decoded)
        if match:
            return match.group(1)
            
        return None
    except:
        return None

def search_yahoo(query):
    try:
        # Search specifically for transcript pages
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
            # Only care if it looks like a redirect or result
            if "investing.com" in raw_href or "yahoo.com" in raw_href:
                cleaned = clean_yahoo_url(raw_href)
                if cleaned:
                    links.append(cleaned)
                    
        return list(set(links))
    except Exception as e:
        logger.error(f"Yahoo Error: {e}")
        return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching Yahoo for: {name}")
    
    raw_links = search_yahoo(name)
    logger.info(f"✅ Yahoo raw results: {len(raw_links)}")
    
    valid_links = []
    for l in raw_links:
        # FILTER LOGIC
        if "investing.com" in l and ("transcript" in l.lower() or "earnings" in l.lower()):
             if "/news/" in l or "/equities/" in l or "/stock-market-news/" in l:
                 valid_links.append(l)
             else:
                 logger.info(f"   🗑️ Rejected (Path): {l}")
        else:
             logger.info(f"   🗑️ Rejected (Content): {l}")
             
    logger.info(f"✅ Final Valid Candidates: {len(valid_links)}")
    return valid_links

# --- 4. FETCHING (Google Cache Priority) ---

def clean_text(soup):
    # Try multiple containers
    body = soup.find('div', class_='WYSIWYG') or \
           soup.find('div', class_='articlePage') or \
           soup.find('div', id='article-content') or \
           soup.body

    if not body: return None

    # Nuke junk
    for tag in body(["script", "style", "iframe", "aside", "button", "figure", "span", "nav", "footer"]): 
        tag.decompose()
    for div in body.find_all('div'):
        if any(cls in str(div.get('class', [])) for cls in ['related', 'carousel', 'promo', 'ad', 'share']):
            div.decompose()

    text_parts = []
    for p in body.find_all(['p', 'h2', 'div']):
        txt = p.get_text().strip()
        if len(txt) > 30 and "Position:" not in txt:
            text_parts.append(txt)
            
    return "\n\n".join(text_parts)

def fetch_google_cache(url):
    """
    Fetches the TEXT-ONLY Google Cache version. 
    Bypasses Cloudflare 99% of the time.
    """
    try:
        # cache:URL&strip=1 (strip=1 is the key)
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url)}&strip=1&vwsrc=0"
        logger.info(f"🛡️ Google Cache: {cache_url}")
        
        sess = get_session()
        # Cache usually requires a simple desktop UA
        sess.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        
        resp = sess.get(cache_url, timeout=15)
        
        if resp.status_code == 404:
            logger.warning("   ↳ Cache Miss (404).")
            return None
        if resp.status_code != 200:
            return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        text = clean_text(soup)
        
        # Fallback: Raw text grab if cleaning failed
        if not text or len(text) < 200:
            text = soup.get_text()
            
        return text
    except Exception as e:
        logger.error(f"Cache Error: {e}")
        return None

# --- 5. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Search & Filter
    candidates = get_candidates(ticker)
    
    if not candidates:
        return None, {"error": "No valid transcript links found after filtering."}
    
    # 2. Fetch
    for link in candidates[:3]:
        logger.info(f"🔗 Target: {link}")
        
        # Try Google Cache (Best Method)
        text = fetch_google_cache(link)
        
        if text and len(text) > 500:
            return text, {
                "source": "Investing.com (Google Cache)",
                "url": link,
                "title": "Earnings Call Transcript",
                "symbol": ticker
            }
            
    return None, {"error": "All candidates failed to fetch."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"SUCCESS: {len(t)} chars")
