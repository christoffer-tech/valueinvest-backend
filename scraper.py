import sys
import logging
import urllib.parse
import re
from bs4 import BeautifulSoup

# --- 1. SETUP ---
# Set up logging to print to stderr (visible in Render logs)
logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try importing curl_cffi, fallback to requests
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded (Best for bypassing Cloudflare)")
except ImportError:
    import requests as std_requests
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Using standard requests.")

# --- 2. HELPER FUNCTIONS ---

def get_session():
    """Returns a session with browser-like headers."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.investing.com/"
    }
    if SESSION_TYPE == "cffi":
        return cffi_requests.Session(impersonate="chrome120", headers=headers)
    else:
        s = std_requests.Session()
        s.headers.update(headers)
        return s

def resolve_name(ticker):
    """Maps tickers to company names for better search results."""
    t = ticker.upper().split('.')[0]
    mapping = {
        "VWS": "Vestas Wind Systems",
        "VWDRY": "Vestas Wind Systems",
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "GOOG": "Alphabet",
        "NVDA": "Nvidia"
    }
    return mapping.get(t, t)

# --- 3. SEARCH METHOD 1: DUCKDUCKGO LITE ---

def search_ddg(query):
    """Searches DuckDuckGo HTML version (Cloud-friendly)."""
    try:
        logger.info(f"Trying DuckDuckGo for: {query}")
        url = "https://html.duckduckgo.com/html/"
        data = {'q': query + " site:investing.com earnings call transcript"}
        
        sess = get_session()
        # DDG Lite uses POST
        resp = sess.post(url, data=data, timeout=10)
        
        if resp.status_code != 200:
            logger.warning(f"DDG failed with status {resp.status_code}")
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

# --- 4. SEARCH METHOD 2: INVESTING.COM INTERNAL SEARCH ---

def search_investing_internal(company_name):
    """Searches Investing.com directly (Bypasses search engine blocks)."""
    try:
        logger.info(f"Trying Internal Search for: {company_name}")
        base_url = "https://www.investing.com/search/"
        params = {"q": company_name, "tab": "news"} # 'news' tab usually has transcripts
        
        sess = get_session()
        # Use simple requests to get the search page
        resp = sess.get(base_url, params=params, timeout=15)
        
        if resp.status_code != 200:
            return []
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # Look for news results
        # Investing.com search results structure varies, looking for generic article links
        for a in soup.find_all('a', href=True):
            href = a['href']
            title = a.get_text().lower()
            
            # Filter for transcript-like URLs/Titles
            if "/news/" in href or "/equities/" in href:
                if "transcript" in title or "earnings call" in title:
                     # Investing.com search results often have relative URLs
                    if href.startswith("/"):
                        href = "https://www.investing.com" + href
                    links.append(href)
                    
        return links
    except Exception as e:
        logger.error(f"Internal Search Error: {e}")
        return []

# --- 5. PARSER ---

def fetch_transcript(url):
    try:
        logger.info(f"Fetching content: {url}")
        sess = get_session()
        resp = sess.get(url, timeout=20)
        
        if resp.status_code != 200:
            logger.error(f"Failed to fetch {url}: {resp.status_code}")
            return None, None, None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Locate Content
        body = soup.find('div', class_='WYSIWYG') or \
               soup.find('div', class_='articlePage') or \
               soup.find('div', id='article-content')
               
        if not body:
            return None, None, None

        # Clean Content
        for tag in body(["script", "style", "iframe"]): tag.decompose()
        for div in body.find_all('div'):
             if "related" in str(div.get('class', [])) or "carousel" in str(div.get('class', [])):
                div.decompose()

        # Extract Text
        text = "\n\n".join([p.get_text().strip() for p in body.find_all(['p', 'h2']) 
                           if p.get_text().strip() and "Position:" not in p.get_text()])
        
        if len(text) < 200: return None, None, None

        title = soup.title.string.strip() if soup.title else "Unknown"
        date = "Unknown"
        d_div = soup.find('div', class_='contentSectionDetails')
        if d_div and d_div.find('span'):
            date = d_div.find('span').get_text().replace("Published", "").strip()
            
        return text, title, date

    except Exception as e:
        logger.error(f"Parse Error: {e}")
        return None, None, None

# --- 6. MAIN ORCHESTRATOR ---

def get_transcript_data(ticker):
    """
    Main function. Tries multiple search methods to ensure result.
    """
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    
    # 1. Resolve Name
    name = resolve_name(ticker)
    
    # 2. Search Strategy
    # Try DDG First
    links = search_ddg(name)
    
    # If DDG blocked/empty, Try Internal Search
    if not links:
        logger.info("DDG yielded 0 results. Switching to Internal Search...")
        links = search_investing_internal(name)
        
    # Filter valid links
    valid_links = []
    seen = set()
    for l in links:
        if l not in seen and ("transcript" in l.lower() or "earnings-call" in l.lower()):
            valid_links.append(l)
            seen.add(l)
            
    if not valid_links:
        logger.error("❌ No links found after all attempts.")
        return None, {"error": "No transcripts found"}
        
    logger.info(f"✅ Found {len(valid_links)} candidates.")
    
    # 3. Sort by 'recency' (heuristic based on year in URL)
    # Give priority to 2025/2026 links
    valid_links.sort(key=lambda x: "2026" in x or "2025" in x, reverse=True)
    
    # 4. Fetch Best Candidate
    target = valid_links[0]
    text, title, date = fetch_transcript(target)
    
    if text:
        logger.info("✅ Successfully scraped transcript.")
        return text, {
            "source": "Investing.com",
            "url": target,
            "symbol_used": ticker,
            "title": title,
            "date": date
        }
    else:
        logger.error("❌ Failed to parse content from best link.")
        return None, {"error": "Failed to parse content"}

# --- TEST BLOCK ---
if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t:
        print(f"Title: {m['title']}")
    else:
        print("Fail")
