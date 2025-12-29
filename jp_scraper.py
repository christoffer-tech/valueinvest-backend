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
    try:
        time.sleep(1) 
        response = requests.get(url, headers=get_headers(), timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        log_func(f" [!] Error fetching {url}: {e}") 
        return None

def parse_date_from_text(text):
    if not text: return None
    # Matches 2025.11.12, 2025/11/12, 2025年11月12日, 2025-11-12
    match = re.search(r'(20\d{2})[./年\-](\d{1,2})[./月\-](\d{1,2})', text)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except:
            pass
    return None

def extract_text_from_pdf_bytes(pdf_bytes, log_func=print):
    text = ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                extracted = page.extract_text()
                if extracted:
                    text += extracted + "\n"
    except Exception as e:
        log_func(f"PDF Extraction Error: {e}")
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
        
        main = soup.find('div', class_=re.compile(r'article-body|log-container|article-content|post-content|body-text')) or soup.find('article')
        
        if not main:
            divs = soup.find_all('div')
            if divs:
                main = max(divs, key=lambda d: len(d.find_all('p')))

        if main:
            ps = main.find_all(['p', 'div', 'h2', 'li'])
            valid = []
            for el in ps:
                txt = el.get_text().strip()
                if len(txt) > 1 and not re.match(r'^\d+\s?/\s?\d+', txt):
                    if not any(c in el.get('class',[]) for c in ['paging','sns-share','breadcrumb']):
                        valid.append(txt)
            
            deduped = []
            for i, x in enumerate(valid):
                if i==0 or x != valid[i-1]: deduped.append(x)
            
            text_chunk = "\n\n".join(deduped)
            if not text_chunk: break
            full_text.append(text_chunk)
        else:
            break
            
        next_btn = soup.find('a', rel='next') or soup.find('a', string=re.compile(r'次へ|Next')) or soup.find('li', class_='next')
        if not next_btn: 
            break
        page += 1
        
    return "\n\n".join(full_text)

def analyze_company_page(soup, logs):
    items = []
    seen_urls = set()
    
    all_links = soup.find_all('a', href=True)

    for link in all_links:
        href = link['href']
        text = link.get_text(strip=True)
        full_url = urljoin("https://finance.logmi.jp", href)
        
        if full_url in seen_urls: continue
        
        # --- Type Detection ---
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

        # --- Robust Date Extraction (Tree Climber) ---
        date_obj = None
        # 1. Check link text
        date_obj = parse_date_from_text(text)
        # 2. Check previous sibling
        if not date_obj:
            prev = link.find_previous_sibling()
            if prev: date_obj = parse_date_from_text(prev.get_text())
        # 3. Check parents (up to 3 levels up - crucial for tables)
        if not date_obj:
            curr = link
            for _ in range(3):
                parent = curr.parent
                if parent:
                    # Get text of parent, but be careful not to grab the whole page
                    block_text = parent.get_text(" ", strip=True)
                    date_obj = parse_date_from_text(block_text)
                    if date_obj: break
                    curr = parent
                else: break
        
        if date_obj:
            seen_urls.add(full_url)
            items.append({
                'type': item_type,
                'date': date_obj,
                'url': full_url,
                'priority': priority,
                'title': text[:50]
            })

    return items

def scrape_japanese_transcript(ticker):
    logs = []
    def log(msg):
        print(msg)
        logs.append(str(msg))

    log(f"Starting scrape for {ticker}")
    clean_ticker = ticker.replace('.T', '').strip()
    
    # FIX: Use simple query (removed 'finance' which might skew results)
    query = f"{clean_ticker} ログミー"
    search_url = f"https://search.yahoo.co.jp/search?p={query}"
    
    log(f"Searching: {search_url}")
    
    try:
        # 1. Search Yahoo JP
        soup = get_soup(search_url, log)
        if not soup: return None, logs
        
        company_url = None
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'finance.logmi.jp/companies/' in href:
                company_url = href
                if 'RU=' in company_url: # Clean Yahoo redirect
                    try:
                        import urllib.parse
                        qs = urllib.parse.parse_qs(urllib.parse.urlparse(company_url).query)
                        if 'RU' in qs: company_url = qs['RU'][0]
                    except: pass
                break
        
        # 1b. Fallback: Direct article
        if not company_url:
             for link in soup.find_all('a', href=True):
                href = link['href']
                if 'finance.logmi.jp/articles/' in href:
                    log(f"Found Direct Article URL: {href}")
                    return stitch_html_transcript({'url': href}, log), logs

        if not company_url:
            log("Company URL not found.")
            return None, logs

        log(f"Found Company URL: {company_url}")

        # 2. Visit Company Page
        soup = get_soup(company_url, log)
        if not soup: return None, logs
        
        # 3. Analyze Items
        items = analyze_company_page(soup, logs)
        
        if not items:
            log("No items found on company page.")
            return None, logs

        # 4. SMART SELECTION LOGIC (The Fix)
        # Find absolute latest date
        items.sort(key=lambda x: x['date'], reverse=True)
        latest_date = items[0]['date']
        log(f"Latest content date: {latest_date.strftime('%Y-%m-%d')}")

        # Define 10-day window
        window_start = latest_date - timedelta(days=10)
        
        # Filter items in window
        candidates = [i for i in items if i['date'] >= window_start]
        
        # Sort candidates by Priority (HTML > PDF)
        candidates.sort(key=lambda x: (x['priority'], -x['date'].timestamp()))
        
        best = candidates[0]
        log(f"🏆 Selected Winner: {best['type']} ({best['date'].strftime('%Y-%m-%d')})")
        log(f"URL: {best['url']}")
        
        # 5. Extract Text
        try:
            if best['type'] == 'HTML_TRANSCRIPT':
                return stitch_html_transcript(best, log), logs
            else:
                log("Downloading PDF for extraction...")
                resp = requests.get(best['url'], headers=get_headers())
                if resp.status_code == 200:
                    text = extract_text_from_pdf_bytes(resp.content, log)
                    return text, logs
                else:
                    log(f"PDF Download failed: {resp.status_code}")
        except Exception as e:
            log(f"Extraction failed: {e}")
            return None, logs
            
    except Exception as e:
        log(f"Global Scraper Error: {e}")
        return None, logs
    
    return None, logs
