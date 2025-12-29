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
        # Increased timeout to 15s to match Colab
        response = requests.get(url, headers=get_headers(), timeout=15)
        response.raise_for_status() # Raise error for 403/404/500
        
        # USE .content instead of .text for correct Japanese encoding detection
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f" [!] Error fetching {url}: {e}") # Log the error so you can see it in Render logs
        return None

def parse_date_from_text(text):
    if not text: return None
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
    
    print(f"Stitching HTML: {url}")
    
    while page < 15:
        target = f"{url}?page={page}" if page > 1 else url
        soup = get_soup(target)
        if not soup: break
        
        main = soup.find('div', class_=re.compile(r'article-body|log-container')) or soup.find('article')
        
        if main:
            ps = main.find_all(['p', 'div', 'h2'])
            valid = []
            for el in ps:
                txt = el.get_text().strip()
                if len(txt) > 1 and not re.match(r'^\d+\s?/\s?\d+', txt):
                    if not any(c in el.get('class',[]) for c in ['paging','sns-share']):
                        valid.append(txt)
            
            deduped = []
            for i, x in enumerate(valid):
                if i==0 or x != valid[i-1]: deduped.append(x)
            
            text_chunk = "\n\n".join(deduped)
            if not text_chunk: break
            full_text.append(text_chunk)
        else:
            break
            
        if not (soup.find('a', rel='next') or soup.find('a', string=re.compile(r'次へ'))): 
            break
        page += 1
        
    return "\n\n".join(full_text)

def scrape_japanese_transcript(ticker):
    print(f"Scraping JP Transcript for: {ticker}")
    
    clean_ticker = ticker.replace('.T', '').strip()
    
    query = f"{clean_ticker} ログミー"
    search_url = f"https://search.yahoo.co.jp/search?p={query}"
    soup = get_soup(search_url)
    company_url = None
    
    if soup:
        for link in soup.find_all('a', href=True):
            href = link['href']
            match = re.search(r'finance\.logmi\.jp/companies/(\d+)', href)
            if match:
                company_url = f"https://finance.logmi.jp/companies/{match.group(1)}"
                print(f"Found Company URL: {company_url}")
                break
    
    if not company_url:
        print("Company URL not found in search results.")
        return None

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

    if not items: 
        print("No items found on company page.")
        return None
    
    items.sort(key=lambda x: x['date'], reverse=True)
    latest_date = items[0]['date']
    print(f"Latest Date: {latest_date}")
    
    # RESTORED 30 DAY WINDOW (Matches Colab)
    window_start = latest_date - timedelta(days=30)
    candidates = [i for i in items if i['date'] >= window_start]
    
    if not candidates: candidates = [items[0]] 
    
    ranked = sorted(candidates, key=lambda x: (x['priority'], -x['date'].timestamp()))
    best = ranked[0]
    print(f"Selected: {best['type']} ({best['date']})")
    
    try:
        if best['type'] == 'HTML_TRANSCRIPT':
            return stitch_html_transcript(best)
        else:
            resp = requests.get(best['url'], headers=get_headers())
            if resp.status_code == 200:
                return extract_text_from_pdf_bytes(resp.content)
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None
        
    return None
