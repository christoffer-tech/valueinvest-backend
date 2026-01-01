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

# Force logging to show us exactly what is happening
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- 2. INTELLIGENT PARSING & HELPERS ---

def clean_company_name(name):
    """Removes legal suffixes to get a clean search term (e.g., 'Vestas Wind Systems A/S' -> 'Vestas Wind Systems')"""
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
    Fetches company name via a lightweight Yahoo Query API (bypassing the heavy yfinance library).
    This is much faster and less prone to '429 Too Many Requests' on Render.
    """
    try:
        # Direct Typeahead API (Very permissive)
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1&newsCount=0"
        
        # Standard user agent
        headers = {'User-Agent': 'Mozilla/5.0'}
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
    
    # Fallback: Strip suffix manually (e.g., VWS.CO -> VWS)
    if "." in symbol:
        return symbol.split(".")[0]
    return symbol

def parse_quarter_from_string(text):
    if not text: return (0, 0)
    text = text.upper()
    # Find Year (2020-2030)
    year_match = re.search(r'20(\d{2})', text)
    if year_match:
        year = int("20" + year_match.group(1))
    else:
        year = 0
        
    # Find Quarter
    q_map = {
        "Q1": 1, "1Q": 1, "FIRST QUARTER": 1,
        "Q2": 2, "2Q": 2, "SECOND QUARTER": 2,
        "Q3": 3, "3Q": 3, "THIRD QUARTER": 3,
        "Q4": 4, "4Q": 4, "FOURTH QUARTER": 4, "FULL YEAR": 4, "FY": 4
    }
    quarter = 0
    for key, val in q_map.items():
        if key in text:
            quarter = val
            break
            
    return (year, quarter)

# --- 3. ROBUST SEARCH LOGIC ---

def search_ddg_html(query):
    """
    Searches DuckDuckGo HTML version. 
    This is the ONLY free method that reliably works on Render/Cloud IPs 
    without getting blocked by CAPTCHAs immediately.
    """
    try:
        session = requests.Session(impersonate="chrome120")
        
        # We use the HTML endpoint which is designed for non-JS clients (easier to scrape)
        url = "https://html.duckduckgo.com/html/"
        payload = {'q': query}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://html.duckduckgo.com/",
            "Origin": "https://html.duckduckgo.com",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        # Must be a POST request for the HTML version
        resp = session.post(url, data=payload, headers=headers, timeout=20)
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # DDG HTML results are in anchor tags with class 'result__a'
        for a in soup.find_all('a', class_='result__a', href=True):
            href = a['href']
            # DDG wraps links; extract the real URL
            if "uddg=" in href:
                try:
                    # url decode the inner url
                    target = urllib.parse.unquote(href.split("uddg=")[1].split("&")[0])
                    links.append(target)
                except:
                    pass
            else:
                links.append(href)
                
        return links
    except Exception as e:
        logger.error(f"Search Engine Error: {e}")
        return []

def extract_valid_links(raw_links):
    valid = []
    seen = set()
    
    for link in raw_links:
        if link in seen: continue
        seen.add(link)
        
        l = link.lower()
        
        # Filter Junk
        if any(x in l for x in ["google.", "yahoo.", "bing.", "duckduckgo.", "search?", "youtube."]):
            continue
            
        # 1. Seeking Alpha
        if "seekingalpha.com/article" in l and ("transcript" in l or "earnings-call" in l):
            valid.append((link, "Seeking Alpha"))
            
        # 2. Investing.com
        elif "investing.com" in l and "transcript" in l:
            valid.append((link, "Investing.com"))
            
        # 3. Motley Fool
        elif "fool.com" in l and "transcript" in l:
            valid.append((link, "Motley Fool"))
            
        # 4. MarketScreener (Good for European stocks like VWS.CO)
        elif "marketscreener.com" in l and "transcript" in l:
            valid.append((link, "MarketScreener"))

    return valid

def find_transcript_candidates(symbol):
    # 1. Get a searchable name (Vital for non-US tickers like VWS.CO)
    name = get_company_name_lightweight(symbol)
    
    # 2. Build Query List (Specific -> Broad)
    queries = []
    
    # Query A: "Vestas Wind Systems earnings call transcript" (Best for EU stocks)
    if name and name != symbol:
        queries.append(f"{name} earnings call transcript")
        
    # Query B: "VWS earnings call transcript" (Ticker without suffix)
    base_ticker = symbol.split('.')[0]
    if base_ticker != symbol:
        queries.append(f"{base_ticker} earnings call transcript")
        
    # Query C: Fallback explicit site searches
    if name:
        queries.append(f"site:seekingalpha.com {name} earnings transcript")
    
    logger.info(f"🔎 Processing {symbol}. Name: {name}. Queries: {queries}")
    
    all_candidates = []
    
    # 3. Execute Searches
    for q in queries:
        links = search_ddg_html(q)
        valid = extract_valid_links(links)
        all_candidates.extend(valid)
        
        # If we found good results, stop searching to save time
        if len(all_candidates) >= 2:
            break
        
        time.sleep(1) # Be polite to the search engine

    if not all_candidates:
        logger.error(f"❌ Zero candidates found for {symbol} after checking {len(queries)} queries.")
        return []

    # 4. Rank Candidates (Newest Year/Quarter first)
    ranked = []
    for link, source in list(set(all_candidates)):
        slug = link.split('/')[-1].replace('-', ' ')
        y, q = parse_quarter_from_string(slug)
        
        # Score = Year * 10 + Quarter (e.g., 20241)
        score = (y * 10) + q
        
        # Prioritize Sources: Investing.com > Seeking Alpha (SA often has paywalls)
        prio = 1
        if "investing.com" in source.lower(): prio = 3
        if "fool.com" in source.lower(): prio = 2
        
        ranked.append({
            "score": score,
            "prio": prio,
            "url": link,
            "source": source
        })

    # Sort: High Score (Date) -> High Prio (Source)
    ranked.sort(key=lambda x: (x['score'], x['prio']), reverse=True)
    
    # Return Top 3
    return [( (0,0), r['url'], r['source'] ) for r in ranked[:3]]

# --- 4. FETCHING & PARSING (Standard) ---

def extract_date(soup, source_label="Generic"):
    """Attempts to extract the publication date from the soup."""
    date_str = "Unknown Date"
    try:
        text_start = soup.get_text()[:2000]
        # Look for "Oct 24, 2024" or "2024-10-24"
        match = re.search(r'(\w{3}\s+\d{1,2},?\s+\d{4})|(\d{4}-\d{2}-\d{2})', text_start)
        if match:
            return match.group(0)
    except: pass
    return date_str

def parse_generic_soup(soup, url, symbol, source_label):
    # Remove junk
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()
        
    paragraphs = soup.find_all('p')
    clean_text = []
    
    for p in paragraphs:
        txt = p.get_text().strip()
        if len(txt) > 40: # Filter short menu items
            clean_text.append(txt)
            
    if len(clean_text) < 5: return None, {"error": "Content too short"}
    
    full_text = "\n\n".join(clean_text)
    title = soup.title.string.strip() if soup.title else "No Title"
    
    return full_text, {
        "source": source_label,
        "url": url,
        "symbol_used": symbol,
        "title": title,
        "date": extract_date(soup)
    }

def fetch_page(url, source, symbol):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }
    
    try:
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, headers=headers, timeout=25)
        
        if resp.status_code != 200:
            return None, {"error": f"Status {resp.status_code}"}
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Check for CAPTCHA title
        if "captcha" in (soup.title.string or "").lower():
            return None, {"error": "Blocked by CAPTCHA"}

        # Route to parsers (For now, generic works for 90% of transcript sites)
        return parse_generic_soup(soup, url, symbol, source)
        
    except Exception as e:
        return None, {"error": str(e)}

# --- 5. MAIN ENTRY POINT ---

def get_transcript_data(ticker):
    try:
        # Step 1: Find URLs
        candidates = find_transcript_candidates(ticker)
        
        if not candidates:
            return None, {"error": "No candidates found"}

        # Step 2: Fetch Content (Try top 3)
        for _, url, source in candidates:
            logger.info(f"Trying to fetch: {url}")
            text, meta = fetch_page(url, source, ticker)
            if text:
                return text, meta
                
        return None, {"error": "Candidates found but parsing failed (Paywall/Captcha)"}
        
    except Exception as e:
        logger.error(f"Scraper Exception: {e}")
        return None, {"error": str(e)}
