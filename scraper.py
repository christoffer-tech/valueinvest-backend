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

# --- 1. CONFIGURATION & LOGGING SETUP ---
os.environ['PYTHONWARNINGS'] = 'ignore'
warnings.simplefilter("ignore")
logging.captureWarnings(True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- 2. INTELLIGENT PARSING & TICKER LOGIC ---

def normalize_ticker(symbol):
    if not symbol or '.' not in symbol:
        return symbol
    parts = symbol.split('.')
    base = parts[0]
    suffix = parts[1].upper()
    mapping = {
        'TOK': 'T', 'PAR': 'PA', 'LON': 'L', 'TRT': 'TO',
        'AMS': 'AS', 'BRU': 'BR', 'ETR': 'DE', 'FRA': 'F', 'HKG': 'HK',
        'CO': 'CO', # Add Copenhagen explicit mapping if needed
    }
    if suffix in mapping:
        return f"{base}.{mapping[suffix]}"
    return symbol

def clean_company_name(name):
    if not name: return name
    suffixes = [
        r' co\.,? ?ltd\.?$', r' co,? ?ltd\.?$', r' ltd\.?$',
        r' inc\.?$', r' corp\.?$', r' corporation$', r' plc$',
        r' s\.a\.$', r' n\.v\.$', r' k\.k\.$', r' kabushiki kaisha$',
        r' adr$', r' \(adr\)$', r' a/s$', r' ag$'
    ]
    clean = name.strip()
    for pattern in suffixes:
        clean = re.sub(pattern, '', clean, flags=re.IGNORECASE)
    return clean.strip()

def parse_quarter_from_string(text):
    if not text: return (0, 0)
    text = text.upper()
    year_match = re.search(r'20(\d{2})', text)
    if year_match:
        year = int("20" + year_match.group(1))
    else:
        year_short = re.search(r'\b(\d{2})\b', text)
        if year_short and 20 <= int(year_short.group(1)) <= 30:
             year = int("20" + year_short.group(1))
        else:
            return (0, 0)
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

def get_company_name(symbol):
    # 1. Try yfinance
    try:
        import yfinance as yf
        normalized = normalize_ticker(symbol)
        ticker = yf.Ticker(normalized)
        # Set a short timeout for info fetch to avoid hanging
        name = ticker.info.get('longName')
        if name:
            return clean_company_name(name)
    except:
        pass
    
    # 2. Fallback: Heuristic cleaning (e.g., VWS.CO -> VWS)
    if "." in symbol:
        return symbol.split(".")[0]
        
    return symbol

# --- 3. SEARCH & RANKING LOGIC ---

def extract_valid_links(raw_links):
    valid = []
    # Remove duplicates
    raw_links = list(set(raw_links))
    
    for link in raw_links:
        try:
            link_lower = link.lower()
            # Filter junk
            if any(x in link_lower for x in ["search.yahoo", "google.com", "bing.com", "ask.com", "ad_url"]):
                continue
                
            # Valid Sources
            if "seekingalpha.com/article" in link_lower and ("transcript" in link_lower or "earnings-call" in link_lower):
                valid.append((link, "Seeking Alpha"))
            elif "investing.com" in link_lower and "transcript" in link_lower:
                valid.append((link, "Investing.com"))
            elif "fool.com" in link_lower and "transcript" in link_lower:
                valid.append((link, "Motley Fool"))
            elif "finance.yahoo.com" in link_lower and "transcript" in link_lower:
                 valid.append((link, "Yahoo Finance"))
            elif "marketscreener.com" in link_lower and "transcript" in link_lower:
                 valid.append((link, "MarketScreener"))
        except:
            continue
    return list(set(valid))

def search_ask_fallback(query):
    """
    Backup search engine. Ask.com is often friendlier to Data Center IPs than Yahoo/Google.
    """
    try:
        # Ask.com search URL
        url = f"https://www.ask.com/web?q={urllib.parse.quote(query)}"
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, timeout=15)
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # Ask.com results are usually in 'div.PartialSearchResults-item-title > a'
        for div in soup.find_all('div', class_='PartialSearchResults-item-title'):
            a = div.find('a', href=True)
            if a:
                links.append(a['href'])
                
        # Also check standard links just in case structure changed
        if not links:
            for a in soup.find_all('a', href=True):
                if "http" in a['href'] and "ask.com" not in a['href']:
                    links.append(a['href'])

        return links
    except Exception as e:
        logger.error(f"Ask.com Search Error: {e}")
        return []

def search_yahoo_fallback(query):
    try:
        profile = random.choice(["chrome120", "chrome124", "safari15_5"])
        session = requests.Session(impersonate=profile)
        url = f"https://search.yahoo.com/search?p={urllib.parse.quote(query)}"
        resp = session.get(url, timeout=15)
        
        # Check for CAPTCHA/Blocking
        if "captcha" in resp.text.lower() or resp.status_code == 429:
            logger.warning("Yahoo Search blocked (CAPTCHA/429).")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            target = href
            if "RU=" in href:
                try:
                    target = urllib.parse.unquote(href.split("RU=")[1].split("/")[0])
                except:
                    pass
            if "search.yahoo.com" not in target:
                links.append(target)
        return links
    except Exception as e:
        logger.error(f"Yahoo Search Error: {e}")
        return []

def find_transcript_candidates(symbol):
    # 1. Determine Search Terms
    company_name = get_company_name(symbol)
    clean_ticker = symbol.split('.')[0] if '.' in symbol else symbol
    
    # Priority: Name > Ticker > Clean Ticker
    search_terms = []
    if company_name and company_name != symbol:
        search_terms.append(company_name)
    if clean_ticker != symbol:
        search_terms.append(clean_ticker)
    search_terms.append(symbol)

    # 2. Build Queries
    queries = []
    primary_term = search_terms[0]
    
    queries.append(f"{primary_term} earnings call transcript 2024 2025")
    queries.append(f"site:seekingalpha.com {primary_term} earnings transcript")
    
    logger.info(f"🔎 Searching for '{symbol}' using term: '{primary_term}'")
    
    candidates = []
    
    for query in queries:
        # A. Try Yahoo First
        raw_links = search_yahoo_fallback(query)
        
        # B. If Yahoo failed/blocked, Try Ask.com
        if not raw_links:
            logger.info("Yahoo returned 0 results. Trying Backup (Ask.com)...")
            raw_links = search_ask_fallback(query)
            
        current_valid = extract_valid_links(raw_links)
        candidates.extend(current_valid)
        
        if len(set([c[0] for c in candidates])) >= 3:
            break
        
        time.sleep(1) # Be polite
            
    unique_candidates = list(set(candidates))
    
    if not unique_candidates:
        logger.error(f"❌ No candidates found for {symbol}. Sources checked: Yahoo, Ask.com")
        return []

    ranked_candidates = []
    for link, source in unique_candidates:
        slug = link.split('/')[-1].replace('-', ' ')
        score_tuple = parse_quarter_from_string(slug)
        # Convert tuple (Year, Quarter) to a sortable integer
        # e.g., (2024, 3) -> 20243
        score = (score_tuple[0] * 10) + score_tuple[1]
        ranked_candidates.append((score, link, source))

    def sort_key(item):
        score, link, source = item
        prio = 0
        if "Investing.com" in source: prio = 2
        elif "Seeking Alpha" in source: prio = 1
        return (score, prio)

    ranked_candidates.sort(key=sort_key, reverse=True)
    
    # Format for the fetcher: ( (Year, Qtr), Link, Source )
    final_list = []
    for score_int, link, source in ranked_candidates:
         # Recover tuple from score integer if needed, or re-parse
         # We just re-parse to be safe and match original format
         final_list.append((parse_quarter_from_string(link), link, source))
         
    return final_list

# --- 4. FETCHING & PARSING UTILS (Unchanged) ---
# ... (Keep your extract_date, parse_generic_soup, etc. exactly as they were) ...
def extract_date(soup, source_label="Generic"):
    """Attempts to extract the publication date from the soup."""
    date_str = "Unknown Date"
    try:
        if "Seeking Alpha" in source_label:
            meta_date = soup.find('meta', {'property': 'article:published_time'})
            if meta_date: return meta_date['content'][:10]
            time_tag = soup.find('span', {'data-test-id': 'post-date'}) or soup.find('time')
            if time_tag: return time_tag.get_text().strip()
        elif "Investing.com" in source_label:
            details = soup.find('div', class_='contentSectionDetails')
            if details:
                span = details.find('span')
                if span: return span.get_text().replace("Published", "").strip()
        text_start = soup.get_text()[:1000]
        date_match = re.search(r'(\w{3,9}\s\d{1,2},\s\d{4})', text_start) 
        if date_match: return date_match.group(1)
        date_match_iso = re.search(r'(\d{4}-\d{2}-\d{2})', text_start)
        if date_match_iso: return date_match_iso.group(1)
    except: pass
    return date_str

def parse_generic_soup(soup, url, symbol, source_label="Generic"):
    for script in soup(["script", "style", "nav", "footer", "header"]):
        script.extract()
    paragraphs = soup.find_all('p')
    text_content = [p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 20]
    if len(text_content) < 5:
        divs = soup.find_all('div')
        for d in divs:
            t = d.get_text().strip()
            if len(t) > 500 and t not in text_content: text_content.append(t)
    if len(text_content) > 3:
        full_text = "\n\n".join(text_content)
        title = soup.title.string.strip() if soup.title else ""
        date = extract_date(soup, source_label)
        return full_text, {
            "source": source_label, "url": url, "symbol_used": symbol,
            "title": title, "date": date, "quarter_info": parse_quarter_from_string(title)
        }
    return None, {"error": "Generic parse failed"}

def parse_seeking_alpha_soup(soup, url, symbol, source_label="Seeking Alpha"):
    content_div = soup.find('div', {'data-test-id': 'article-content'}) or \
                  soup.find('div', {'itemprop': 'articleBody'}) or \
                  soup.find('article') or \
                  soup.find('div', id='content-body') or \
                  soup.find('div', class_='sa-art')
    if not content_div: return parse_generic_soup(soup, url, symbol, source_label + " (Generic Fallback)")
    text_content = []
    skip_phrases = ["Share", "Save", "Comments", "Follow", "Like", "See all our", "have not been edited"]
    for tag in content_div.find_all(['p', 'h2', 'h3']):
        text = tag.get_text().strip()
        if not text: continue
        if len(text) < 20 and any(n in text for n in skip_phrases): continue
        text_content.append(text)
    if not text_content: return None, {"error": "Empty content"}
    return "\n\n".join(text_content), {
        "source": source_label, "url": url, "symbol_used": symbol,
        "title": soup.title.string.strip() if soup.title else "",
        "date": extract_date(soup, source_label),
        "quarter_info": parse_quarter_from_string(soup.title.string if soup.title else "")
    }

def parse_investing_com_soup(soup, url, symbol, source_label="Investing.com"):
    selectors = [('div', {'class': 'WYSIWYG articlePage'}), ('div', {'class': 'articlePage'}),
                 ('div', {'id': 'article-content'}), ('div', {'class': 'article_container'}), ('div', {'class': 'mainArticle'})]
    content_div = None
    for tag, attrs in selectors:
        content_div = soup.find(tag, attrs)
        if content_div: break
    if not content_div: return parse_generic_soup(soup, url, symbol, source_label + " (Generic Fallback)")
    text_content = []
    for tag in content_div.find_all(['p', 'h2']):
        text = tag.get_text().strip()
        if text and "Position:" not in text: text_content.append(text)
    full_text = "\n\n".join(text_content)
    if len(full_text) < 500: return parse_generic_soup(soup, url, symbol, source_label + " (Short Content Fallback)")
    return full_text, {
        "source": source_label, "url": url, "symbol_used": symbol,
        "title": soup.title.string.strip() if soup.title else "",
        "date": extract_date(soup, source_label),
        "quarter_info": parse_quarter_from_string(soup.title.string if soup.title else "")
    }

def parse_motley_fool_soup(soup, url, symbol, source_label="Motley Fool"):
    content_div = soup.find('div', class_='article-body') or soup.find('div', class_='article-content')
    if not content_div: return parse_generic_soup(soup, url, symbol, source_label + " (Generic Fallback)")
    text_content = []
    for tag in content_div.find_all(['p', 'h2']):
        text = tag.get_text().strip()
        if "Discount" in text or "Stock Advisor" in text: continue
        if text: text_content.append(text)
    if not text_content: return None, {"error": "Empty Motley Fool content"}
    return "\n\n".join(text_content), {
        "source": source_label, "url": url, "symbol_used": symbol,
        "title": soup.title.string.strip() if soup.title else "",
        "date": extract_date(soup, source_label),
        "quarter_info": parse_quarter_from_string(soup.title.string if soup.title else "")
    }

def fetch_page(url, source, symbol):
    headers = {"Referer": "https://www.google.com/", "Accept-Language": "en-US,en;q=0.9"}
    if source == "Seeking Alpha":
        mirrors = ["https://archive.today", "https://archive.is", "https://archive.ph"]
        for mirror in mirrors:
            try:
                session = requests.Session(impersonate="chrome120")
                target = f"{mirror}/newest/{url}"
                resp = session.get(target, timeout=40)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    if "working..." not in soup.get_text().lower():
                        res, err = parse_seeking_alpha_soup(soup, url, symbol, f"Archive ({mirror})")
                        if res: return res, err
            except: continue
            
    profiles = ["chrome124", "chrome120", "safari15_5"]
    random.shuffle(profiles)
    for profile in profiles:
        try:
            session = requests.Session(impersonate=profile)
            resp = session.get(url, headers=headers, timeout=25)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                if source == "Seeking Alpha": res, err = parse_seeking_alpha_soup(soup, url, symbol, source)
                elif source == "Investing.com": res, err = parse_investing_com_soup(soup, url, symbol, source)
                elif source == "Motley Fool": res, err = parse_motley_fool_soup(soup, url, symbol, source)
                else: res, err = parse_generic_soup(soup, url, symbol, source)
                if res: return res, err
        except: time.sleep(1)
        
    return None, {"error": "Failed to fetch"}

# --- 5. EXPORT FUNCTION ---
def get_transcript_data(ticker):
    """
    Main entry point for the API.
    """
    try:
        candidates = find_transcript_candidates(ticker)
        if not candidates:
            return None, {"error": "No candidates found"}
        for score, url, source in candidates[:3]:
            text, meta = fetch_page(url, source, ticker)
            if text: return text, meta
        return None, {"error": "Candidates found but failed to parse"}
    except Exception as e:
        logger.error(f"Scraper Error: {e}")
        return None, {"error": str(e)}

if __name__ == "__main__":
    # Test Block
    tgt = "VWS.CO"
    print(f"Testing {tgt}...")
    txt, meta = get_transcript_data(tgt)
    if txt:
        print(f"✅ FOUND: {meta['title']}")
    else:
        print("❌ FAILED")
