import sys
import logging
import urllib.parse
import re
import time
import random
from bs4 import BeautifulSoup

# --- 1. SETUP ---
# Configure logging to show up in Render/Cloud consoles
logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try to use curl_cffi (Critical for bypassing Cloudflare)
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded (Cloudflare bypass active)")
except ImportError:
    import requests as std_requests
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Using standard requests (High chance of 403 Block).")

# --- 2. SESSION FACTORY ---

def get_session(mobile=False):
    """
    Creates a browser session. Swaps between Desktop and Mobile to evade blocks.
    """
    if mobile:
        ua = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
        plat = '"Android"'
    else:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        plat = '"Windows"'

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?1" if mobile else "?0",
        "Sec-Ch-Ua-Platform": plat,
        "Upgrade-Insecure-Requests": "1"
    }

    if SESSION_TYPE == "cffi":
        # Randomize the impersonation fingerprint slightly
        ver = random.choice(["chrome110", "chrome120"])
        return cffi_requests.Session(impersonate=ver, headers=headers)
    else:
        s = std_requests.Session()
        s.headers.update(headers)
        return s

def resolve_name(ticker):
    """Maps ticker symbols to searchable company names."""
    t = ticker.upper().split('.')[0] # Remove .CO, .DE, etc.
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "PNDORA": "Pandora A/S",
        "PNDZY": "Pandora A/S",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "MSFT": "Microsoft", 
        "NVDA": "Nvidia"
    }
    # If ticker is PNDORA, return Pandora A/S
    if "PNDORA" in t: return "Pandora A/S"
    return mapping.get(t, t)

# --- 3. SEARCH (DuckDuckGo Lite) ---

def search_ddg(query):
    try:
        logger.info(f"Trying DuckDuckGo for: {query}")
        url = "https://html.duckduckgo.com/html/"
        # We append 'site:investing.com' to force relevant results
        data = {'q': query + " site:investing.com earnings call transcript"}
        
        # Use a standard desktop session for search (DDG is lenient)
        sess = get_session(mobile=False)
        resp = sess.post(url, data=data, timeout=15)
        
        if resp.status_code != 200:
            logger.warning(f"DDG failed: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', class_='result__a', href=True):
            if "investing.com" in a['href']:
                links.append(a['href'])
        return links
    except Exception as e:
        logger.error(f"DDG Error: {e}")
        return []

# --- 4. FETCH CONTENT (With Retry Logic) ---

def fetch_content(url):
    """
    Attempts to fetch a URL. 
    1. Tries Desktop User-Agent.
    2. If blocked (403), waits and retries with Mobile User-Agent.
    """
    # Attempt 1: Desktop
    try:
        sess = get_session(mobile=False)
        resp = sess.get(url, timeout=15)
        
        # If 403 Forbidden, try switching identity
        if resp.status_code == 403:
            logger.warning(f"⚠️ 403 Blocked (Desktop). Retrying as Mobile...")
            time.sleep(random.uniform(1.0, 2.5))
            sess = get_session(mobile=True)
            resp = sess.get(url, timeout=15)

        if resp.status_code != 200:
            return None, f"Status {resp.status_code}", None

        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Locate Content
        body = soup.find('div', class_='WYSIWYG') or \
               soup.find('div', class_='articlePage') or \
               soup.find('div', id='article-content')
               
        if not body:
            return None, "No body div", None

        # Cleanup Junk
        for tag in body(["script", "style", "iframe", "aside"]): tag.decompose()
        for div in body.find_all('div'):
            # Remove "Related Articles" widgets
            if "related" in str(div.get('class', [])) or "carousel" in str(div.get('class', [])):
                div.decompose()

        # Extract Text
        text = "\n\n".join([p.get_text().strip() for p in body.find_all(['p', 'h2']) 
                           if len(p.get_text().strip()) > 20 and "Position:" not in p.get_text()])
        
        if len(text) < 500:
            return None, "Text too short", None

        title = soup.title.string.strip() if soup.title else "Unknown"
        
        # Date Extraction
        date = "Unknown"
        d_div = soup.find('div', class_='contentSectionDetails')
        if d_div and d_div.find('span'):
             date = d_div.find('span').get_text().replace("Published", "").strip()
            
        return text, title, date

    except Exception as e:
        return None, str(e), None

# --- 5. MAIN LOGIC ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    name = resolve_name(ticker)
    
    # 1. Search
    links = search_ddg(name)
    
    # Filter valid links
    valid_links = []
    seen = set()
    for l in links:
        if l not in seen and ("/news/" in l or "/equities/" in l):
            if "transcript" in l.lower() or "earnings-call" in l.lower():
                valid_links.append(l)
                seen.add(l)

    if not valid_links:
        logger.error("❌ No transcript links found.")
        return None, {"error": "No transcripts found"}

    logger.info(f"✅ Found {len(valid_links)} candidates. Starting fetch loop...")

    # 2. Iterate through ALL candidates until one works
    for i, link in enumerate(valid_links):
        logger.info(f"🔗 Candidate {i+1}/{len(valid_links)}: {link}")
        
        text, title, date = fetch_content(link)
        
        if text:
            logger.info("✅ SUCCESS! Transcript scraped.")
            return text, {
                "source": "Investing.com",
                "url": link,
                "symbol_used": ticker,
                "title": title,
                "date": date
            }
        else:
            logger.warning(f"❌ Failed to parse candidate {i+1}. Trying next...")
            time.sleep(random.uniform(1.5, 3.0)) # Polite delay between attempts

    logger.error("❌ All candidates failed.")
    return None, {"error": "All candidates blocked or empty"}

# --- TEST ---
if __name__ == "__main__":
    # Test block for local debugging
    t, m = get_transcript_data("PNDORA.CO")
    if t: print(f"Success: {m['title']}")
