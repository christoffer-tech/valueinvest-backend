import os
import sys
import warnings
import logging
import re
import time
import urllib.parse
from bs4 import BeautifulSoup

# --- 1. ROBUST IMPORTS (Fixes "Missing Library" Crashes) ---
# Try to use curl_cffi (best for bypassing Cloudflare), fallback to standard requests
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
except ImportError:
    import requests as std_requests
    SESSION_TYPE = "standard"
    print("⚠️ WARNING: curl_cffi not found. Using standard requests (higher chance of blocking).", file=sys.stderr)

# --- 2. LOGGING SETUP ---
# We use sys.stderr.write to force logs to show up in Render/Gunicorn consoles
def log(msg):
    timestamp = time.strftime("%H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [SCRAPER] {msg}\n")
    sys.stderr.flush()

# --- 3. HELPER FUNCTIONS ---

def get_session():
    """Returns a session object with browser headers."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
    """Converts tickers like VWS.CO to search-friendly names."""
    t = ticker.upper().split('.')[0] # VWS.CO -> VWS
    
    # Simple mapping for known tricky tickers
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "NVDA": "Nvidia"
    }
    return mapping.get(t, t)

# --- 4. SEARCH ENGINES ---

def search_duckduckgo(query):
    """Fallback searcher: HTML DuckDuckGo (Low Block Rate)"""
    try:
        log(f"Searching DuckDuckGo for: {query}")
        url = "https://html.duckduckgo.com/html/"
        payload = {'q': query + " site:investing.com"}
        
        sess = get_session()
        # DDG HTML requires POST usually, but sometimes GET works. POST is safer for html version.
        resp = sess.post(url, data=payload, timeout=15)
        
        if resp.status_code != 200:
            log(f"DuckDuckGo failed: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', class_='result__a', href=True):
            links.append(a['href'])
            
        return links
    except Exception as e:
        log(f"DuckDuckGo Exception: {e}")
        return []

def search_bing(query):
    """Primary searcher: Bing (Higher Block Rate, better results)"""
    try:
        log(f"Searching Bing for: {query}")
        # Broad search, filtering later
        full_query = f"{query} investing.com earnings call transcript"
        url = f"https://www.bing.com/search?q={urllib.parse.quote(full_query)}"
        
        sess = get_session()
        resp = sess.get(url, timeout=10)
        
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for h2 in soup.find_all('h2'):
            a = h2.find('a', href=True)
            if a: links.append(a['href'])
        
        return links
    except Exception as e:
        log(f"Bing Exception: {e}")
        return []

def filter_links(raw_links):
    """Filters mixed search results for valid Investing.com transcripts."""
    valid = []
    seen = set()
    for link in raw_links:
        if link in seen: continue
        seen.add(link)
        
        l = link.lower()
        if "investing.com" not in l: continue
        
        # Must act like a transcript URL
        if "/news/" in l or "/equities/" in l:
            if "transcript" in l or "earnings-call" in l:
                valid.append(link)
    return valid

# --- 5. PARSER ---

def fetch_content(url):
    try:
        log(f"Fetching URL: {url}")
        sess = get_session()
        resp = sess.get(url, timeout=20)
        
        if resp.status_code != 200:
            return None, f"HTTP Status {resp.status_code}", None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # 1. Find Body
        body = soup.find('div', class_='WYSIWYG') or \
               soup.find('div', class_='articlePage') or \
               soup.find('div', id='article-content')
               
        if not body:
            return None, "No content div found", None
            
        # 2. Extract Text (Removing Ads)
        text_parts = []
        for tag in body.find_all(['p', 'h2']):
            # Skip ad divs/scripts inside
            if tag.name == 'div': continue 
            
            txt = tag.get_text().strip()
            if not txt: continue
            if "Position:" in txt or "Have a confidential tip" in txt: continue
            
            text_parts.append(txt)
            
        full_text = "\n\n".join(text_parts)
        
        if len(full_text) < 200:
            return None, "Extracted text too short (paywall/block)", None
            
        # 3. Meta
        title = soup.title.string.strip() if soup.title else "Unknown Title"
        date_str = "Unknown Date"
        d_div = soup.find('div', class_='contentSectionDetails')
        if d_div and d_div.find('span'):
            date_str = d_div.find('span').get_text().replace("Published", "").strip()
            
        return full_text, title, date_str
        
    except Exception as e:
        return None, str(e), None

# --- 6. MAIN EXPORT ---

def get_transcript_data(ticker):
    """
    Main function called by your API (main.py).
    Returns: (text_content, metadata_dict) or (None, error_dict)
    """
    try:
        log(f"Starting Scrape for Ticker: {ticker}")
        
        # 1. Resolve Name
        company_name = resolve_name(ticker)
        query = f"{company_name} earnings call transcript"
        
        # 2. Search (Bing -> Fallback to DDG)
        links = search_bing(query)
        valid_links = filter_links(links)
        
        if not valid_links:
            log("Bing returned 0 valid links. Trying DuckDuckGo...")
            links = search_duckduckgo(company_name + " earnings call transcript")
            valid_links = filter_links(links)
            
        if not valid_links:
            log("❌ No links found on Bing or DuckDuckGo.")
            return None, {"error": "No transcripts found after searching."}
            
        log(f"✅ Found {len(valid_links)} potential transcripts.")
        
        # 3. Pick Best Link (Simple logic: take the first one, usually most relevant)
        target_url = valid_links[0]
        
        # 4. Fetch
        text, title, date = fetch_content(target_url)
        
        if not text:
            return None, {"error": f"Failed to parse content: {title}"}
            
        meta = {
            "source": "Investing.com",
            "url": target_url,
            "symbol_used": ticker,
            "title": title,
            "date": date
        }
        
        return text, meta
        
    except Exception as e:
        log(f"🔥 CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None, {"error": str(e)}

# --- 7. TEST BLOCK ---
if __name__ == "__main__":
    print("--- RUNNING LOCAL TEST ---")
    txt, data = get_transcript_data("VWS.CO")
    if txt:
        print(f"SUCCESS! Title: {data['title']}")
        print(f"URL: {data['url']}")
    else:
        print(f"FAILED: {data}")
