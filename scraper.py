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

# Set logging to ERROR to suppress INFO/DEBUG logs
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

class NoisyLogFilter(logging.Filter):
    def filter(self, record):
        return True

logging.getLogger().addFilter(NoisyLogFilter())
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
    try:
        import yfinance as yf
        normalized = normalize_ticker(symbol)
        ticker = yf.Ticker(normalized)
        name = ticker.info.get('longName')
        if name:
            return clean_company_name(name)
        return symbol
    except:
        return symbol

def extract_date(soup, source_label="Generic"):
    """Attempts to extract the publication date from the soup."""
    date_str = "Unknown Date"
    
    try:
        if "Seeking Alpha" in source_label:
            # Try meta tags first
            meta_date = soup.find('meta', {'property': 'article:published_time'})
            if meta_date: return meta_date['content'][:10]
            # Try span tags
            time_tag = soup.find('span', {'data-test-id': 'post-date'}) or soup.find('time')
            if time_tag: return time_tag.get_text().strip()

        elif "Investing.com" in source_label:
            details = soup.find('div', class_='contentSectionDetails')
            if details:
                span = details.find('span')
                if span: return span.get_text().replace("Published", "").strip()
            
        # Generic Fallback: Look for standard date patterns in the first 1000 chars
        text_start = soup.get_text()[:1000]
        date_match = re.search(r'(\w{3,9}\s\d{1,2},\s\d{4})', text_start) # e.g., Oct 24, 2024
        if date_match:
            return date_match.group(1)
        
        date_match_iso = re.search(r'(\d{4}-\d{2}-\d{2})', text_start)
        if date_match_iso:
            return date_match_iso.group(1)
            
    except:
        pass
        
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
            if len(t) > 500 and t not in text_content:
                text_content.append(t)
                
    if len(text_content) > 3:
        full_text = "\n\n".join(text_content)
        title = soup.title.string.strip() if soup.title else ""
        date = extract_date(soup, source_label)
        return full_text, {
            "source": source_label,
            "url": url,
            "symbol_used": symbol,
            "title": title,
            "date": date,
            "quarter_info": parse_quarter_from_string(title)
        }
    return None, {"error": "Generic parse failed"}

def parse_seeking_alpha_soup(soup, url, symbol, source_label="Seeking Alpha"):
    content_div = soup.find('div', {'data-test-id': 'article-content'}) or \
                  soup.find('div', {'itemprop': 'articleBody'}) or \
                  soup.find('article') or \
                  soup.find('div', id='content-body') or \
                  soup.find('div', class_='sa-art')
    if not content_div:
        return parse_generic_soup(soup, url, symbol, source_label + " (Generic Fallback)")
    text_content = []
    skip_phrases = ["Share", "Save", "Comments", "Follow", "Like", "See all our", "have not been edited"]
    for tag in content_div.find_all(['p', 'h2', 'h3']):
        text = tag.get_text().strip()
        if not text: continue
        if len(text) < 20 and any(n in text for n in skip_phrases): continue
        text_content.append(text)
    if not text_content: return None, {"error": "Empty content"}
    full_text = "\n\n".join(text_content)
    title = soup.title.string.strip() if soup.title else ""
    date = extract_date(soup, source_label)
    meta = {
        "source": source_label,
        "url": url,
        "symbol_used": symbol,
        "title": title,
        "date": date,
        "quarter_info": parse_quarter_from_string(title)
    }
    return full_text, meta

def parse_investing_com_soup(soup, url, symbol, source_label="Investing.com"):
    selectors = [
        ('div', {'class': 'WYSIWYG articlePage'}),
        ('div', {'class': 'articlePage'}),
        ('div', {'id': 'article-content'}),
        ('div', {'class': 'article_container'}),
        ('div', {'class': 'mainArticle'})
    ]
    content_div = None
    for tag, attrs in selectors:
        content_div = soup.find(tag, attrs)
        if content_div: break
    if not content_div:
        return parse_generic_soup(soup, url, symbol, source_label + " (Generic Fallback)")
    text_content = []
    for tag in content_div.find_all(['p', 'h2']):
        text = tag.get_text().strip()
        if text and "Position:" not in text:
            text_content.append(text)
    full_text = "\n\n".join(text_content)
    if len(full_text) < 500:
         return parse_generic_soup(soup, url, symbol, source_label + " (Short Content Fallback)")
    title = soup.title.string.strip() if soup.title else ""
    date = extract_date(soup, source_label)
    meta = {
        "source": source_label,
        "url": url,
        "symbol_used": symbol,
        "title": title,
        "date": date,
        "quarter_info": parse_quarter_from_string(title)
    }
    return full_text, meta

def parse_motley_fool_soup(soup, url, symbol, source_label="Motley Fool"):
    content_div = soup.find('div', class_='article-body') or \
                  soup.find('div', class_='article-content')
    if not content_div:
        return parse_generic_soup(soup, url, symbol, source_label + " (Generic Fallback)")
    text_content = []
    for tag in content_div.find_all(['p', 'h2']):
        text = tag.get_text().strip()
        if "Discount" in text or "Stock Advisor" in text: continue
        if text: text_content.append(text)
    if not text_content: return None, {"error": "Empty Motley Fool content"}
    full_text = "\n\n".join(text_content)
    title = soup.title.string.strip() if soup.title else ""
    date = extract_date(soup, source_label)
    meta = {
        "source": source_label,
        "url": url,
        "symbol_used": symbol,
        "title": title,
        "date": date,
        "quarter_info": parse_quarter_from_string(title)
    }
    return full_text, meta

# --- 3. SEARCH & RANKING LOGIC ---

def search_yahoo_fallback(query):
    try:
        profile = random.choice(["chrome120", "chrome124", "safari15_5"])
        session = requests.Session(impersonate=profile)
        url = f"https://search.yahoo.com/search?p={urllib.parse.quote(query)}"
        resp = session.get(url, timeout=15)
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
    except:
        return []

def extract_valid_links(raw_links):
    valid = []
    raw_links = list(set(raw_links))
    for link in raw_links:
        try:
            link_lower = link.lower()
            if any(x in link_lower for x in ["search.yahoo.com", "google.com/search", "bing.com/search", "chat.yahoo.com", "shopping.yahoo.com", "help.yahoo.com"]):
                continue
            if "seekingalpha.com/article" in link_lower and ("transcript" in link_lower or "earnings-call" in link_lower):
                valid.append((link, "Seeking Alpha"))
            elif "investing.com" in link_lower and "transcript" in link_lower:
                valid.append((link, "Investing.com"))
            elif "fool.com" in link_lower and "transcript" in link_lower:
                valid.append((link, "Motley Fool"))
            elif "finance.yahoo.com" in link_lower and "transcript" in link_lower:
                 valid.append((link, "Yahoo Finance"))
        except:
            continue
    return list(set(valid))

def find_transcript_candidates(symbol):
    company_name = get_company_name(symbol)
    search_term = company_name if company_name else symbol
    queries = [
        f"site:investing.com {search_term} earnings transcript",
        f"{search_term} earnings call transcript q3 2025",
        f"site:seekingalpha.com {search_term} earnings call transcript",
        f"{search_term} earnings call transcript"
    ]
    candidates = []
    
    # We silently process queries here
    for query in queries:
        raw_links = search_yahoo_fallback(query)
        current_valid = extract_valid_links(raw_links)
        candidates.extend(current_valid)
        if len(set([c[0] for c in candidates])) >= 3:
            break
            
    unique_candidates = list(set(candidates))
    if not unique_candidates:
        return []

    ranked_candidates = []
    for link, source in unique_candidates:
        slug = link.split('/')[-1].replace('-', ' ')
        score = parse_quarter_from_string(slug)
        ranked_candidates.append((score, link, source))

    def sort_key(item):
        score, link, source = item
        prio = 0
        if "Investing.com" in source: prio = 2
        elif "Seeking Alpha" in source: prio = 1
        return (score, prio)

    ranked_candidates.sort(key=sort_key, reverse=True)
    return ranked_candidates

# --- 4. FETCHING ---

def fetch_page(url, source, symbol):
    headers = {
        "Referer": "https://www.google.com/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9"
    }

    if source == "Seeking Alpha":
        mirrors = ["https://archive.today", "https://archive.is", "https://archive.ph", "https://archive.li", "https://archive.vn"]
        archive_profiles = ["chrome124", "safari15_5", "chrome110"]
        for i, mirror in enumerate(mirrors):
            profile = archive_profiles[i % len(archive_profiles)]
            try:
                session = requests.Session(impersonate=profile)
                target = f"{mirror}/newest/{url}"
                time.sleep(random.uniform(1, 3))
                resp = session.get(target, timeout=40)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    text_preview = soup.get_text().lower()
                    if "working..." in text_preview or "submit_url" in resp.url or "/wip/" in resp.url:
                        time.sleep(10)
                        resp = session.get(resp.url, timeout=40)
                        soup = BeautifulSoup(resp.content, 'html.parser')
                    res, err = parse_seeking_alpha_soup(soup, url, symbol, f"Archive ({mirror})")
                    if res: return res, err
            except:
                continue

    profiles = ["chrome124", "chrome120", "safari15_5", "chrome110"]
    random.shuffle(profiles)
    for profile in profiles:
        try:
            session = requests.Session(impersonate=profile)
            time.sleep(random.uniform(2, 5))
            resp = session.get(url, headers=headers, timeout=25)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                page_title = soup.title.string.lower() if soup.title else ""
                if any(x in page_title for x in ["human", "challenge", "captcha", "security"]):
                     continue
                if source == "Seeking Alpha":
                    res, err = parse_seeking_alpha_soup(soup, url, symbol, source)
                elif source == "Investing.com":
                    res, err = parse_investing_com_soup(soup, url, symbol, source)
                elif source == "Motley Fool":
                    res, err = parse_motley_fool_soup(soup, url, symbol, source)
                else:
                    res, err = parse_generic_soup(soup, url, symbol, source)
                if res: return res, err
        except:
            time.sleep(1)

    # Cache Fallbacks
    cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{url}&strip=1&vwsrc=0"
    try:
        session = requests.Session(impersonate="chrome120")
        resp = session.get(cache_url, headers=headers, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            if "Error 404" not in soup.get_text():
                 return parse_generic_soup(soup, url, symbol, "Google Cache")
    except:
        pass

    try:
        session = requests.Session(impersonate="chrome120")
        api_url = f"https://archive.org/wayback/available?url={url}"
        resp = session.get(api_url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('archived_snapshots', {}).get('closest', {}):
                snapshot_url = data['archived_snapshots']['closest']['url']
                resp = session.get(snapshot_url, timeout=20)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    return parse_generic_soup(soup, url, symbol, "Wayback Machine")
    except:
        pass

    return None, {"error": "Failed to fetch"}

# --- 5. MODULE EXPORT ---

def get_transcript_data(ticker):
    """
    Main entry point for the API.
    Returns: (transcript_text, metadata) or (None, error_dict)
    """
    try:
        candidates = find_transcript_candidates(ticker)
        
        if not candidates:
            return None, {"error": "No candidates found"}

        # Try the top 3 candidates
        for score, url, source in candidates[:3]:
            text, meta = fetch_page(url, source, ticker)
            if text:
                return text, meta
        
        return None, {"error": "Candidates found but failed to parse content"}
        
    except Exception as e:
        logger.error(f"Scraper Error: {e}")
        return None, {"error": str(e)}
