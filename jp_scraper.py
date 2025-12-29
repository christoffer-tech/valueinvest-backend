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

def get_soup(url):
    try:
        time.sleep(1) # Politeness delay
        response = requests.get(url, headers=get_headers(), timeout=10)
        if response.status_code != 200: return None
        return BeautifulSoup(response.text, 'html.parser')
    except:
        return None

def parse_date_from_text(text):
    if not text: return None
    # Matches 2025.11.12, 2025/11/12, 2025年11月12日
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

def stitch_html_transcript(item):
    full_text = []
    page = 1
    url = item['url']
    
    # Limit pages to prevent infinite loops
    while page < 15:
        target = f"{url}?page={page}" if page > 1 else url
        soup = get_soup(target)
        if not soup: break
        
        # Logmi Article Body Selector
        main = soup.find('div', class_=re.compile(r'article-body|log-container')) or soup.find('article')
        
        if main:
            ps = main.find_all(['p', 'div', 'h2'])
            valid = []
            for el in ps:
                txt = el.get_text().strip()
                # Filter out pagination numbers like "1/5" or empty strings
                if len(txt) > 1 and not re.match(r'^\d+\s?/\s?\d+', txt):
                    if not any(c in el.get('class',[]) for c in ['paging','sns-share']):
                        valid.append(txt)
            
            # Simple deduping of adjacent identical lines
            deduped = []
            for i, x in enumerate(valid):
                if i==0 or x != valid[i-1]: deduped.append(x)
            
            text_chunk = "\n\n".join(deduped)
            if not text_chunk: break
            full_text.append(text_chunk)
        else:
            break
            
        # Check for "Next" button to continue
        if not (soup.find('a', rel='next') or soup.find('a', string=re.compile(r'次へ'))): 
            break
        page += 1
        
    return "\n\n".join(full_text)

def scrape_japanese_transcript(ticker):
    # 1. Clean Ticker (remove .T)
    clean_ticker = ticker.replace('.T', '').strip()
    
    # 2. Find Company Page via Yahoo JP Search
    query = f"{clean_ticker} ログミー"
    search_url = f"https://search.yahoo.co.jp/search?p={query}"
    soup = get_soup(search_url)
    company_url = None
    
    if soup:
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Look for finance.logmi.jp link
            match = re.search(r'finance\.logmi\.jp/companies/(\d+)', href)
            if match:
                company_url = f"https://finance.logmi.jp/companies/{match.group(1)}"
                break
    
    if not company_url:
        return None

    # 3. Analyze Company Page for Transcripts
    soup = get_soup(company_url)
    if not soup: return None
    
    items = []
    seen_urls = set()
    
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

        date_obj = parse_date_from_text(text)
        if not date_obj:
            # Try finding date in previous element
            prev = link.find_previous_sibling()
            if prev: date_obj = parse_date_from_text(prev.get_text())
            
        if date_obj:
            seen_urls.add(full_url)
            items.append({
                'type': item_type,
                'date': date_obj,
                'url': full_url,
                'priority': priority
            })

    # 4. Selection Logic (Recent Window + Priority)
    if not items: return None
    
    # Sort by newest first
    items.sort(key=lambda x: x['date'], reverse=True)
    latest_date = items[0]['date']
    
    # Only look at items within 10 days of the latest release
    window_start = latest_date - timedelta(days=10)
    candidates = [i for i in items if i['date'] >= window_start]
    
    # Fallback if filtering removed everything (rare)
    if not candidates: candidates = [items[0]] 
    
    # Rank: Priority first (HTML=1), then Date (Newest)
    ranked = sorted(candidates, key=lambda x: (x['priority'], -x['date'].timestamp()))
    best = ranked[0]
    
    # 5. Extract Text
    try:
        if best['type'] == 'HTML_TRANSCRIPT':
            return stitch_html_transcript(best)
        else:
            # Download PDF to memory
            resp = requests.get(best['url'], headers=get_headers())
            if resp.status_code == 200:
                return extract_text_from_pdf_bytes(resp.content)
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None
        
    return None
