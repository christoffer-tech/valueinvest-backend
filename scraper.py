import os
import sys
import warnings
import logging
import re
import time
import urllib.parse
from bs4 import BeautifulSoup

# --- 1. SETUP & IMPORTS ---
# Try to use curl_cffi (best for bypassing Cloudflare), fallback to requests
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
except ImportError:
    import requests as std_requests
    SESSION_TYPE = "standard"
    sys.stderr.write("⚠️ [SCRAPER] curl_cffi not installed. Using standard requests (higher block risk).\n")

# Use stderr for logs so they appear in Render/Gunicorn streams
def log(msg):
    sys.stderr.write(f"[SCRAPER] {msg}\n")
    sys.stderr.flush()

# --- 2. HELPER: GET SESSION ---
def get_session():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/"
    }
    if SESSION_TYPE == "cffi":
        return cffi_requests.Session(impersonate="chrome120", headers=headers)
    else:
        s = std_requests.Session()
        s.headers.update(headers)
        return s

# --- 3. HELPER: NAME RESOLUTION ---
def resolve_name(ticker):
    """
    Converts tickers to searchable company names.
    Vital for VWS.CO -> Vestas Wind Systems
    """
    t = ticker.upper().split('.')[0] # Strip .CO, .DE etc
    
    # Manual map for common issues
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "NVDA": "Nvidia",
        "MSFT": "Microsoft",
        "GOOG": "Alphabet",
        "GOOGL": "Alphabet",
        "META": "Meta Platforms"
    }
    return mapping.get(t, t)

# --- 4. SEARCH LOGIC (DuckDuckGo Lite) ---
def search_duckduckgo_lite(query):
    """
    Uses DuckDuckGo Lite (HTML). Very resilient to server blocks.
    """
    try:
        log(f"🔎 Searching DDG for: {query}")
        # We search specifically for investing.com transcripts
        full_query = f"{query} site:investing.com earnings call transcript"
        url = "https://html.duckduckgo.com/html/"
        
        sess = get_session()
        # DDG Lite uses POST for search
        resp = sess.post(url, data={'q': full_query}, timeout=15)
        
        if resp.status_code != 200:
            log(f"DDG Error: Status {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # Extract links from DDG Lite results
        for a in soup.find_all('a', class_='result__a', href=True):
            links.append(a['href'])
            
        log(f"✅ DDG found {len(links)} raw links.")
        return links
    except Exception as e:
        log(f"Search Exception: {e}")
        return []

def filter_links(raw_links):
    """Clean and filter links to ensure they are valid transcripts."""
    valid = []
    seen = set()
    for link in raw_links:
        if link in seen: continue
        seen.add(link)
        
        l = link.lower()
        if "investing.com" not in l: continue
        
        # Must contain transcript indicators
        if "/news/" in l or "/equities/" in l:
            if "transcript" in l or "earnings-call" in l:
                valid.append(link)
    return valid

# --- 5. CONTENT PARSER ---
def fetch_content(url):
    try:
        log(f"📥 Fetching content from: {url}")
        sess = get_session()
        resp = sess.get(url, timeout=20)
        
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}", None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Attempt to find the main article body
        body = soup.find('div', class_='WYSIWYG') or \
               soup.find('div', class_='articlePage') or \
               soup.find('div', id='article-content')
               
        if not body:
            return None, "No article body found", None
            
        # Extract text paragraphs
        text_parts = []
        for tag in body.find_all(['p', 'h2']):
            txt = tag.get_text().strip()
            # Basic cleanup
            if not txt: continue
            if "Position:" in txt or "confidential tip" in txt.lower(): continue
            text_parts.append(txt)
            
        full_text = "\n\n".join(text_parts)
        
        if len(full_text) < 200:
            return None, "Text too short (blocked/paywall)", None
            
        # Metadata
        title = soup.title.string.strip() if soup.title else "Unknown Title"
        date_str = "Unknown"
        d_div = soup.find('div', class_='contentSectionDetails')
        if d_div and d_div.find('span'):
            date_str = d_div.find('span').get_text().replace("Published", "").strip()
            
        return full_text, title, date_str
        
    except Exception as e:
        return None, str(e), None

# --- 6. MAIN EXPORT ---
def get_transcript_data(ticker):
    """
    The main function called by your API.
    """
    try:
        # 1. Resolve Ticker -> Name (Crucial for VWS.CO)
        company_name = resolve_name(ticker)
        log(f"Processing: {ticker} -> {company_name}")
        
        # 2. Search
        links = search_duckduckgo_lite(company_name)
        valid_links = filter_links(links)
        
        if not valid_links:
            log(f"❌ No valid links found for {company_name}")
            return None, {"error": "No transcripts found"}
        
        # 3. Fetch Best Match
        target_url = valid_links[0]
        text, title, date = fetch_content(target_url)
        
        if not text:
            log(f"❌ Failed to parse: {title}")
            return None, {"error": "Failed to parse content"}
            
        # 4. Success
        meta = {
            "source": "Investing.com",
            "url": target_url,
            "symbol_used": ticker,
            "title": title,
            "date": date
        }
        log(f"✅ Success: {title}")
        return text, meta
        
    except Exception as e:
        log(f"🔥 Critical Error: {e}")
        return None, {"error": str(e)}

if __name__ == "__main__":
    # Test block
    print("Running test...")
    t, m = get_transcript_data("VWS.CO")
    if t: print(f"Title: {m['title']}\nURL: {m['url']}")
    else: print("Failed.")
