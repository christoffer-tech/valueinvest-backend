import requests
from bs4 import BeautifulSoup
import re
import time
import random
import pdfplumber
import io
from urllib.parse import urljoin
from datetime import datetime, timedelta

# --- CONFIGURATION ---
TYPE_PRIORITY = {
    "HTML_TRANSCRIPT": 1,
    "PDF_TRANSCRIPT": 2,
    "PDF_PRESENTATION": 3,
    "PDF_TANSHIN": 4,
    "OTHER": 99
}

def get_headers():
    agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
    ]
    return {
        'User-Agent': random.choice(agents),
        'Accept-Language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7'
    }

def get_soup(url, log_func=print):
    """
    Fetches a URL and returns a BeautifulSoup object.
    Includes error handling and logging.
    """
    try:
        time.sleep(1) # Politeness delay
        response = requests.get(url, headers=get_headers(), timeout=15)
        response.raise_for_status() # Raise error for 403/404/500
        
        # USE .content instead of .text for correct Japanese encoding detection
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        log_func(f" [!] Error fetching {url}: {e}") 
        return None

def parse_date_from_text(text):
    if not text: return None
    # Try YYYY.MM.DD, YYYY/MM/DD, YYYY年MM月DD日
    match = re.search(r'(20\d{2})[./年\-](\d{1,2})[./月\-](\d{1,2})', text)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except:
            pass
    return None

def extract_text_from_pdf_bytes(pdf_bytes):
    text = ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                extracted = page.extract_text()
                if extracted:
                    text += extracted + "\n"
    except Exception as e:
        print(f"PDF Extraction Error: {e}")
    return text

def stitch_html_transcript(item, log_func=print):
    full_text = []
    page = 1
    url = item['url']
    
    log_func(f"Stitching HTML: {url}")
    
    while page < 15:
        target = f"{url}?page={page}" if page > 1 else url
        soup = get_soup(target, log_func)
        if not soup: break
        
        # Expanded selectors for resilience
        main = soup.find('div', class_=re.compile(r'article-body|log-container|article-content|post-content|body-text')) or soup.find('article')
        
        # Fallback: Find the div with the most paragraph tags if no class matches
        if not main:
            divs = soup.find_all('div')
            if divs:
                main = max(divs, key=lambda d: len(d.find_all('p')))

        if main:
            # Get text from common block elements
            ps = main.find_all(['p', 'div', 'h2', 'li'])
            valid = []
            for el in ps:
                txt = el.get_text().strip()
                # Filter out navigation, page numbers, and social buttons
                if len(txt) > 1 and not re.match(r'^\d+\s?/\s?\d+', txt):
                    if not any(c in el.get('class',[]) for c in ['paging','sns-share','breadcrumb']):
                        valid.append(txt)
            
            # Deduplicate sequential lines
            deduped = []
            for i, x in enumerate(valid):
                if i==0 or x != valid[i-1]: deduped.append(x)
            
            text_chunk = "\n\n".join(deduped)
            
            # Stop if page exists but has no content (often happens on paginated sites going out of bounds)
            if not text_chunk: break
            
            full_text.append(text_chunk)
        else:
            break
            
        # Check for "Next" button to decide if we continue
        next_btn = soup.find('a', rel='next') or soup.find('a', string=re.compile(r'次へ|Next')) or soup.find('li', class_='next')
        if not next_btn: 
            break
        page += 1
        
    return "\n\n".join(full_text)

def scrape_japanese_transcript(ticker):
    logs = []
    def log(msg):
        print(msg)
        logs.append(str(msg))

    log(f"Starting scrape for {ticker}")
    
    clean_ticker = ticker.replace('.T', '').strip()
    
    # Use Logmi specific query
    query = f"{clean_ticker} ログミー finance"
    search_url = f"https://search.yahoo.co.jp/search?p={query}"
    
    log(f"Searching: {search_url}")
    
    try:
        # 1. Search Yahoo JP for the Logmi company page
        soup = get_soup(search_url, log)
        company_url = None
        
        if soup:
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Look for finance.logmi.jp/companies/1234 or articles directly
                if 'finance.logmi.jp/companies/' in href:
                    company_url = href
                    # Clean yahoo redirect wrapper if present
                    if 'RU=' in company_url:
                        try:
                            import urllib.parse
                            qs = urllib.parse.parse_qs(urllib.parse.urlparse(company_url).query)
                            if 'RU' in qs: company_url = qs['RU'][0]
                        except: pass
                    break
        
        # 1b. Fallback: If no company page, look for direct article link
        if not company_url and soup:
             for link in soup.find_all('a', href=True):
                href = link['href']
                if 'finance.logmi.jp/articles/' in href:
                    # We found a direct article, use it directly if it looks recent
                    log(f"Found Direct Article URL: {href}")
                    return stitch_html_transcript({'url': href}, log), logs

        if not company_url:
            log("Company URL not found in search results.")
            return None, logs

        log(f"Found Company URL: {company_url}")

        # 2. Visit Company Page
        soup = get_soup(company_url, log)
        if not soup: return None, logs
        
        items = []
        seen_urls = set()
        
        # 3. Find Transcripts on Company Page
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True)
            full_url = urljoin("https://finance.logmi.jp", href)
            
            if full_url in seen_urls: continue
            
            item_type = "OTHER"
            priority = TYPE_PRIORITY["OTHER"]
            is_html = "/articles/" in href
            is_pdf = "active_storage" in href or href.lower().endswith(".pdf")
            
            if is_html:
                item_type = "HTML_TRANSCRIPT"
                priority = TYPE_PRIORITY["HTML_TRANSCRIPT"]
            elif is_pdf:
                if "書き起こし" in text:
                    item_type = "PDF_TRANSCRIPT"
                    priority = TYPE_PRIORITY["PDF_TRANSCRIPT"]
                elif "説明会資料" in text or "説明資料" in text:
                    item_type = "PDF_PRESENTATION"
                    priority = TYPE_PRIORITY["PDF_PRESENTATION"]
                elif "短信" in text or "決算" in text:
                    item_type = "PDF_TANSHIN"
                    priority = TYPE_PRIORITY["PDF_TANSHIN"]
                else: continue
            else: continue

            # Date Parsing
            date_obj = parse_date_from_text(text)
            if not date_obj:
                # Look at parent or sibling elements for date
                parent = link.parent
                if parent: date_obj = parse_date_from_text(parent.get_text())
                if not date_obj:
                    prev = link.find_previous_sibling()
                    if prev: date_obj = parse_date_from_text(prev.get_text())
            
            # Fallback: If finding date fails, use current date minus index (assuming chronological order) to sort
            if not date_obj:
                 date_obj = datetime.now() 

            if date_obj:
                seen_urls.add(full_url)
                items.append({
                    'type': item_type,
                    'date': date_obj,
                    'url': full_url,
                    'priority': priority
                })

        if not items: 
            log("No items found on company page.")
            return None, logs
        
        # 4. Sort and Select Best Item
        # Priority: HTML Transcript > PDF Transcript > Presentation > Others
        # Secondary Sort: Date (Newest first)
        items.sort(key=lambda x: (x['priority'], -x['date'].timestamp()))
        
        best = items[0]
        log(f"Selected: {best['type']} ({best['date']}) - {best['url']}")
        
        # 5. Extract Text
        try:
            if best['type'] == 'HTML_TRANSCRIPT':
                transcript_text = stitch_html_transcript(best, log)
                return transcript_text, logs
            else:
                resp = requests.get(best['url'], headers=get_headers())
                if resp.status_code == 200:
                    text = extract_text_from_pdf_bytes(resp.content)
                    return text, logs
        except Exception as e:
            log(f"Extraction failed: {e}")
            return None, logs
            
    except Exception as e:
        log(f"Global Scraper Error: {e}")
        return None, logs
    
    return None, logs
