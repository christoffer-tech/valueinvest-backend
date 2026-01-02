import sys
import logging
import urllib.parse
import re
import random
import xml.etree.ElementTree as ET
import requests as std_requests
from bs4 import BeautifulSoup

# --- 1. CONFIGURATION ---
logger = logging.getLogger("Scraper")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('[SCRAPER] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Try curl_cffi for Direct Fetch & Search (Crucial for bypassing blocks)
try:
    from curl_cffi import requests as cffi_requests
    SESSION_TYPE = "cffi"
    logger.info("✅ curl_cffi loaded (Stealth Mode).")
except ImportError:
    SESSION_TYPE = "standard"
    logger.warning("⚠️ curl_cffi not found. Falling back to standard requests (High Risk of Block).")

# --- 2. SESSION FACTORY ---

def get_cffi_session():
    """Browser session for Direct Fetch & Search"""
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

# --- 3. SEARCH STRATEGIES (Dynamic Discovery) ---

def search_google_rss(query):
    """
    Strategy A: Google News RSS.
    Why: RSS feeds are rarely IP-blocked compared to HTML search pages.
    """
    try:
        logger.info("   🔍 Strategy A: Google News RSS...")
        # Exact query structure to target Investing.com transcripts
        q = f"{query} earnings call transcript site:investing.com"
        rss_url = f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}"
        
        sess = get_cffi_session()
        resp = sess.get(rss_url, timeout=15)
        
        if resp.status_code != 200:
            logger.warning(f"      ↳ RSS Blocked: {resp.status_code}")
            return []
            
        # Parse XML
        root = ET.fromstring(resp.content)
        candidates = []
        
        for item in root.findall(".//item"):
            title = item.find("title").text if item.find("title") is not None else ""
            link = item.find("link").text if item.find("link") is not None else ""
            
            # Filter for actual transcripts
            if "investing.com" in link and ("transcript" in title.lower() or "transcript" in link.lower()):
                # Google RSS links are redirects; we'll resolve them later or use as is
                candidates.append({"url": link, "title": title})
                
        return candidates
    except Exception as e:
        logger.warning(f"      ↳ RSS Error: {e}")
        return []

def search_investing_internal(query):
    """
    Strategy B: Investing.com Internal Search.
    Why: Searching the source directly mimics a real user.
    """
    try:
        logger.info("   🔍 Strategy B: Investing.com Internal Search...")
        url = "https://www.investing.com/search/"
        params = {"q": query}
        
        sess = get_cffi_session()
        resp = sess.get(url, params=params, timeout=15)
        
        if resp.status_code != 200: return []
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        candidates = []
        
        # Parse 'News' or 'Analysis' sections
        for a in soup.find_all('a', href=True):
            href = a['href']
            title = a.get_text().strip()
            
            if "/news/transcripts/" in href or ("transcript" in title.lower() and "/news/" in href):
                full_url = href if href.startswith("http") else f"https://www.investing.com{href}"
                candidates.append({"url": full_url, "title": title})
                
        return candidates
    except Exception as e:
        logger.warning(f"      ↳ Internal Search Error: {e}")
        return []

def get_candidates(ticker):
    name = resolve_name(ticker)
    logger.info(f"🔎 Searching for: {name}")
    
    # 1. Google RSS (Most Robust)
    candidates = search_google_rss(name)
    
    # 2. Internal Search (Fallback)
    if not candidates:
        candidates = search_investing_internal(name)
    
    # Sort/Filter
    # Prioritize "Q3 2025" or "2025"
    unique_urls = []
    seen = set()
    
    for c in candidates:
        u = c['url']
        if u in seen: continue
        seen.add(u)
        
        score = 0
        if "2025" in u or "2025" in c['title']: score += 10
        if "Q3" in u or "Q3" in c['title']: score += 5
        
        unique_urls.append((score, u))
        
    unique_urls.sort(key=lambda x: x[0], reverse=True)
    final_list = [x[1] for x in unique_urls]
    
    if final_list:
        logger.info(f"✅ Found {len(final_list)} candidates.")
        logger.info(f"   🌟 Top Pick: {final_list[0]}")
    else:
        logger.error("❌ No candidates found.")
        
    return final_list

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
    # Investing.com specific cleanup
    body = soup.find('div', class_='WYSIWYG') or soup.find('div', class_='articlePage') or soup.body
    if not body: return None
    
    for tag in body(["script", "style", "iframe", "button", "figure", "aside", "nav", "footer"]): tag.decompose()
    
    # Remove ads and related links
    for div in body.find_all('div'):
        if any(c in str(div.get('class', [])) for c in ['related', 'ad', 'share', 'img', 'discussion']): div.decompose()
        
    text_parts = [p.get_text().strip() for p in body.find_all(['p', 'h2']) if len(p.get_text().strip()) > 30]
    return "\n\n".join(text_parts)

# --- 5. FETCHING (With Redirect Handling) ---

def fetch_content(url):
    try:
        logger.info(f"   ⚡ Fetching: {url}")
        sess = get_cffi_session()
        
        # Allow redirects (crucial for Google News links)
        resp = sess.get(url, timeout=20, allow_redirects=True)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            text = clean_text(soup)
            
            if is_valid_content(text):
                # Clean header/footer noise
                start_markers = ["**Full transcript -", "Earnings call transcript:", "Participants", "Operator"]
                for m in start_markers:
                    if m in text: 
                        text = text[text.find(m):]
                        break
                
                end_markers = ["Risk Disclosure:", "Fusion Media", "Comments"]
                for m in end_markers:
                    if m in text:
                        text = text[:text.find(m)]
                        break
                        
                return text.strip()
            else:
                logger.warning("      ↳ Content blocked or invalid (Maintenance Page).")
        else:
            logger.warning(f"      ↳ HTTP Error: {resp.status_code}")
            
        return None
    except Exception as e:
        logger.warning(f"      ↳ Fetch Error: {e}")
        return None

# --- 6. MAIN ---

def get_transcript_data(ticker):
    logger.info(f"🚀 STARTING SCRAPE FOR: {ticker}")
    candidates = get_candidates(ticker)
    
    if not candidates:
        return None, {"error": "No candidates found via RSS or Internal Search"}
    
    for link in candidates[:3]: # Try top 3
        text = fetch_content(link)
        if text:
            return text, {"source": "Investing.com", "url": link}
            
    return None, {"error": "All fetch methods failed."}

if __name__ == "__main__":
    t, m = get_transcript_data("VWS.CO")
    if t: 
        print(f"\nSUCCESS! Found {len(t)} characters.")
        print("-" * 40)
        print(t[:2000])
    else:
        print("\nFAILED:", m)
