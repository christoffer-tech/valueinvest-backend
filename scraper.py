import os
import sys
import warnings
import logging
import re
import time
import random
import urllib.parse
from bs4 import BeautifulSoup
from curl_cffi import requests
from datetime import datetime

# --- 1. CONFIGURATION & LOGGING ---
os.environ['PYTHONWARNINGS'] = 'ignore'
warnings.simplefilter("ignore")
logging.captureWarnings(True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- 2. COMPANY NAME RESOLUTION ---

def clean_company_name(name):
    """Removes legal suffixes."""
    if not name: return name
    suffixes = [
        r' co\.,? ?ltd\.?$', r' co,? ?ltd\.?$', r' ltd\.?$',
        r' inc\.?$', r' corp\.?$', r' corporation$', r' plc$',
        r' s\.a\.$', r' n\.v\.$', r' k\.k\.$', r' kabushiki kaisha$',
        r' adr$', r' \(adr\)$', r' a/s$', r' ag$', r' ab$'
    ]
    clean = name.strip()
    for pattern in suffixes:
        clean = re.sub(pattern, '', clean, flags=re.IGNORECASE)
    return clean.strip()

def get_company_name_lightweight(symbol):
    """
    Resolves 'VWS.CO' -> 'Vestas Wind Systems' using Yahoo's public Typeahead API.
    """
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1&newsCount=0"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        
        if 'quotes' in data and len(data['quotes']) > 0:
            long_name = data['quotes'][0].get('longname')
            short_name = data['quotes'][0].get('shortname')
            found_name = long_name or short_name
            if found_name:
                cleaned = clean_company_name(found_name)
                logger.info(f"Resolved {symbol} -> {cleaned}")
                return cleaned
    except Exception as e:
        logger.warning(f"Name fetch failed for {symbol}: {e}")
    
    # Fallback
    if "." in symbol:
        return symbol.split(".")[0]
    return symbol

def parse_quarter_from_string(text):
    if not text: return (0, 0)
    text = text.upper()
    year_match = re.search(r'20(\d{2})', text)
    year = int("20" + year_match.group(1)) if year_match else 0
        
    q_map = {
        "Q1": 1, "1Q": 1, "FIRST": 1,
        "Q2": 2, "2Q": 2, "SECOND": 2, "HALF": 2,
        "Q3": 3, "3Q": 3, "THIRD": 3,
        "Q4": 4, "4Q": 4, "FOURTH": 4, "FULL YEAR": 4, "FY": 4
    }
    quarter = 0
    for key, val in q_map.items():
        if key in text:
            quarter = val
            break
            
    return (year, quarter)

# --- 3. ROBUST SEARCH LOGIC (BING PRIMARY) ---

def search_bing(query):
    """
    Scrapes Bing HTML. Much more robust for Render IPs than DDG/Google.
    """
    try:
        # Use a random US/EU market to broaden results
        url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}&setmkt=en-US"
        
        # Bing requires a very standard User-Agent to not block
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5"
        }
        
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, headers=headers, timeout=20)
        
        if resp.status_code != 200:
            logger.warning(f"Bing returned status {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # Bing Results are usually in <h2><a> tags or <div class="b_algo"><h2><a>
        for h2 in soup.find_all('h2'):
            a = h2.find('a', href=True)
            if a:
                links.append(a['href'])
                
        logger.info(f"Bing found {len(links)} raw links for '{query}'")
        return links
    except Exception as e:
        logger.error(f"Bing Search Error: {e}")
        return []

def extract_valid_links(raw_links):
    valid = []
    seen = set()
    
    for link in raw_links:
        if link in seen: continue
        seen.add(link)
        l = link.lower()
        
        # 1. Filter Search Engine Junk
        if any(x in l for x in ["bing.com", "google.", "yahoo.", "microsoft.", "search?"]):
            continue

        # 2. Relaxed Keyword Matching
        # We look for "transcript" OR "earnings call" OR just "earnings" on major sites
        is_transcript = "transcript" in l or "earnings-call" in l or "results" in l
        
        if "seekingalpha.com/article" in l and is_transcript:
            valid.append((link, "Seeking Alpha"))
        elif "investing.com" in l and is_transcript:
            valid.append((link, "Investing.com"))
        elif "fool.com" in l and is_transcript:
            valid.append((link, "Motley Fool"))
        elif "marketscreener.com" in l and is_transcript:
            valid.append((link, "MarketScreener"))
        elif "thestreet.com" in l and is_transcript:
             valid.append((link, "TheStreet"))
        elif "finance.yahoo.com" in l and is_transcript:
             valid.append((link, "Yahoo Finance"))

    return valid

def find_transcript_candidates(symbol):
    name = get_company_name_lightweight(symbol)
    queries = []
    
    # Query 1: Name + Transcript (Most accurate)
    if name and name != symbol:
        queries.append(f"{name} earnings call transcript")
        
    # Query 2: Ticker + Transcript
    base_ticker = symbol.split('.')[0]
    queries.append(f"{base_ticker} earnings call transcript")
    
    logger.info(f"🔎 Processing {symbol} using Bing. Queries: {queries}")
    
    all_candidates = []
    
    for q in queries:
        links = search_bing(q)
        valid = extract_valid_links(links)
        all_candidates.extend(valid)
        
        if len(all_candidates) >= 2: break
        time.sleep(1.5)

    if not all_candidates:
        logger.error(f"❌ Zero candidates found for {symbol}")
        return []

    # Rank Candidates
    ranked = []
    for link, source in list(set(all_candidates)):
        slug = link.split('/')[-1].replace('-', ' ')
        y, q = parse_quarter_from_string(slug)
        score = (y * 10) + q
        
        prio = 1
        if "seekingalpha" in source.lower(): prio = 3
        if "fool.com" in source.lower(): prio = 2
        
        ranked.append({ "score": score, "prio": prio, "url": link, "source": source })

    ranked.sort(key=lambda x: (x['score'], x['prio']), reverse=True)
    return [( (0,0), r['url'], r['source'] ) for r in ranked[:3]]

# --- 4. FETCHING & PARSING ---

def extract_date(soup):
    try:
        text = soup.get_text()[:2000]
        match = re.search(r'(\w{3}\s+\d{1,2},?\s+\d{4})|(\d{4}-\d{2}-\d{2})', text)
        if match: return match.group(0)
    except: pass
    return "Unknown Date"

def parse_generic_soup(soup, url, symbol, source):
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form", "iframe"]):
        tag.decompose()
        
    # Heuristic: Find the element with the most <p> tags
    max_p_count = 0
    content_div = None
    
    candidates = soup.find_all(['div', 'article', 'section'])
    for c in candidates:
        p_count = len(c.find_all('p', recursive=False)) # Direct children preferred
        if p_count > max_p_count:
            max_p_count = p_count
            content_div = c
            
    # Fallback to just all P tags if no clear container
    if not content_div or max_p_count < 3:
        paragraphs = soup.find_all('p')
    else:
        paragraphs = content_div.find_all('p')
        
    clean_text = []
    for p in paragraphs:
        txt = p.get_text().strip()
        if len(txt) > 40: clean_text.append(txt)
            
    if len(clean_text) < 5: return None, {"error": "Content too short"}
    
    return "\n\n".join(clean_text), {
        "source": source, "url": url, "symbol_used": symbol,
        "title": soup.title.string.strip() if soup.title else "No Title",
        "date": extract_date(soup)
    }

def fetch_page(url, source, symbol):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    try:
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, headers=headers, timeout=25)
        if resp.status_code != 200: return None, {"error": f"Status {resp.status_code}"}
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        if "captcha" in (soup.title.string or "").lower(): return None, {"error": "Blocked by CAPTCHA"}
        
        return parse_generic_soup(soup, url, symbol, source)
    except Exception as e:
        return None, {"error": str(e)}

# --- 5. MAIN ---

def get_transcript_data(ticker):
    try:
        candidates = find_transcript_candidates(ticker)
        if not candidates: return None, {"error": "No candidates found"}

        for _, url, source in candidates:
            logger.info(f"Attempting fetch: {url}")
            text, meta = fetch_page(url, source, ticker)
            if text: return text, meta
                
        return None, {"error": "Candidates found but parsing failed"}
    except Exception as e:
        logger.error(f"Scraper Exception: {e}")
        return None, {"error": str(e)}
