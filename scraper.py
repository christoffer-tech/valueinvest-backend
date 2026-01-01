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
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1&newsCount=0"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        if 'quotes' in data and len(data['quotes']) > 0:
            found = data['quotes'][0].get('longname') or data['quotes'][0].get('shortname')
            if found:
                return clean_company_name(found)
    except:
        pass
    if "." in symbol: return symbol.split(".")[0]
    return symbol

def parse_quarter_from_string(text):
    if not text: return (0, 0)
    text = text.upper()
    year_match = re.search(r'20(\d{2})', text)
    year = int("20" + year_match.group(1)) if year_match else 0
    q_map = {"Q1": 1, "1Q": 1, "FIRST": 1, "Q2": 2, "2Q": 2, "SECOND": 2, "HALF": 2, "Q3": 3, "3Q": 3, "THIRD": 3, "Q4": 4, "4Q": 4, "FOURTH": 4, "FY": 4}
    quarter = 0
    for k, v in q_map.items():
        if k in text:
            quarter = v
            break
    return (year, quarter)

# --- 3. ROBUST SEARCH (BING -> INVESTING.COM ONLY) ---

def search_bing(query):
    try:
        # We append 'site:investing.com' here to force Bing to only look there
        full_query = f"site:investing.com {query}"
        url = f"https://www.bing.com/search?q={urllib.parse.quote(full_query)}&setmkt=en-US"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, headers=headers, timeout=20)
        
        if resp.status_code != 200: return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for h2 in soup.find_all('h2'):
            a = h2.find('a', href=True)
            if a: links.append(a['href'])
        
        logger.info(f"Bing found {len(links)} links for '{full_query}'")
        return links
    except Exception as e:
        logger.error(f"Bing Error: {e}")
        return []

def extract_valid_links(raw_links):
    valid = []
    seen = set()

    for link in raw_links:
        if link in seen: continue
        seen.add(link)
        l = link.lower()
        
        # STRICT FILTER: Must be investing.com
        if "investing.com" not in l:
            continue

        # Must look like a transcript url
        # investing.com URLs usually look like /equities/apple-computer-inc-earnings-calls-transcripts
        # or /news/stock-market-news/earnings-call-transcript
        if "earnings" in l and ("transcript" in l or "call" in l):
            valid.append((link, "Investing.com"))

    return valid

def find_transcript_candidates(symbol):
    name = get_company_name_lightweight(symbol)
    queries = []
    
    # 1. Best: "Vestas Wind Systems earnings call transcript"
    if name and name != symbol:
        queries.append(f"{name} earnings call transcript")
    
    # 2. Backup: "VWS earnings call transcript"
    base_ticker = symbol.split('.')[0]
    queries.append(f"{base_ticker} earnings call transcript")
    
    logger.info(f"🔎 Searching Investing.com for {symbol} ({name})...")
    
    all_candidates = []
    for q in queries:
        links = search_bing(q)
        valid = extract_valid_links(links)
        all_candidates.extend(valid)
        if len(all_candidates) >= 2: break
        time.sleep(1.5)

    if not all_candidates:
        logger.error(f"❌ Zero Investing.com transcripts found for {symbol}")
        return []

    # Rank by Date in URL
    ranked = []
    for link, source in list(set(all_candidates)):
        slug = link.split('/')[-1].replace('-', ' ')
        y, q = parse_quarter_from_string(slug)
        score = (y * 10) + q
        ranked.append({ "score": score, "url": link })

    ranked.sort(key=lambda x: x['score'], reverse=True)
    return [( (0,0), r['url'], "Investing.com" ) for r in ranked[:3]]

# --- 4. INVESTING.COM SPECIFIC PARSER ---

def parse_investing_com(soup, url, symbol):
    # Investing.com usually puts content in <div class="WYSIWYG articlePage">
    content_div = soup.find('div', class_='WYSIWYG') or \
                  soup.find('div', class_='articlePage') or \
                  soup.find('div', id='article-content')
                  
    if not content_div:
        return None, {"error": "Could not find Investing.com content div"}

    # Remove junk inside the article
    for tag in content_div(["script", "style", "iframe", "div"]):
        # Investing.com puts ads in divs inside the text, remove them
        # BUT be careful not to remove text divs. Usually ads have specific classes.
        if tag.name == 'div' and tag.get('class'):
             # Generic ad classes often used
             if any(c in str(tag.get('class')) for c in ['related', 'img', 'video', 'carousel']):
                 tag.decompose()
        elif tag.name in ['script', 'style', 'iframe']:
             tag.decompose()

    # Extract text cleanly
    text_content = []
    for tag in content_div.find_all(['p', 'h2']):
        txt = tag.get_text().strip()
        if not txt: continue
        if "Position:" in txt: continue # Signature line
        if "Have a confidential tip" in txt: continue
        text_content.append(txt)

    if len(text_content) < 5:
        return None, {"error": "Content too short (maybe paywalled?)"}

    # Extract Date
    date_str = "Unknown Date"
    date_div = soup.find('div', class_='contentSectionDetails')
    if date_div:
        span = date_div.find('span')
        if span: date_str = span.get_text().replace("Published", "").strip()

    return "\n\n".join(text_content), {
        "source": "Investing.com",
        "url": url,
        "symbol_used": symbol,
        "title": soup.title.string.strip() if soup.title else "Investing.com Transcript",
        "date": date_str
    }

def fetch_page(url, source, symbol):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, headers=headers, timeout=25)
        
        if resp.status_code != 200: return None, {"error": f"Status {resp.status_code}"}
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        if "captcha" in (soup.title.string or "").lower():
            return None, {"error": "Blocked by CAPTCHA"}
            
        return parse_investing_com(soup, url, symbol)
        
    except Exception as e:
        return None, {"error": str(e)}

# --- 5. MAIN ---

def get_transcript_data(ticker):
    try:
        candidates = find_transcript_candidates(ticker)
        if not candidates: return None, {"error": "No Investing.com transcripts found"}

        for _, url, source in candidates:
            logger.info(f"Fetching: {url}")
            text, meta = fetch_page(url, source, ticker)
            if text: return text, meta
                
        return None, {"error": "Candidates found but parsing failed"}
    except Exception as e:
        logger.error(f"Scraper Exception: {e}")
        return None, {"error": str(e)}
