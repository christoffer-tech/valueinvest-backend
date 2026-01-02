import requests
from bs4 import BeautifulSoup
import re
import time
import random
import pdfplumber
import gc
import tempfile
import os
import logging
from urllib.parse import urljoin
from datetime import datetime, timedelta

# --- OCR LIBRARIES (STEP 2) ---
try:
    import pytesseract
    from pdf2image import convert_from_path
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

# --- 1. SILENCE PDF NOISE ---
logging.getLogger("pdfminer").setLevel(logging.ERROR)

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
    match = re.search(r'(20\d{2})[./年\-](\d{1,2})[./月\-](\d{1,2})', text)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except:
            pass
    return None

def format_doc_header(item):
    t = item['type']
    label = "DOCUMENT"
    if "TRANSCRIPT" in t:
        label = "EARNINGS CALL TRANSCRIPT"
    elif "PRESENTATION" in t:
        label = "PRESENTATION SLIDES"
    elif "TANSHIN" in t:
        label = "FINANCIAL RESULTS (TANSHIN)"
        
    return f"\n\n{'='*40}\n=== {label} ===\nDATE: {item['date'].strftime('%Y-%m-%d')}\nTYPE: {t}\n{'='*40}\n"

# --- MEMORY OPTIMIZED DOWNLOADER WITH OCR FALLBACK ---
def download_and_extract_pdf(url, log_func=print):
    text = ""
    start_time = time.time()
    
    # SAFETY: Must be slightly less than Gunicorn --timeout (set to 120s)
    MAX_EXECUTION_TIME = 100 
    
    temp_filename = None
    
    try:
        log_func(f"Streaming PDF to disk...")
        with requests.get(url, headers=get_headers(), stream=True, timeout=30) as r:
            r.raise_for_status()
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tf:
                for chunk in r.iter_content(chunk_size=8192): 
                    tf.write(chunk)
                temp_filename = tf.name
        
        # --- STEP 1: Standard Extraction (Fast) ---
        with pdfplumber.open(temp_filename) as pdf:
            total_pages = len(pdf.pages)
            log_func(f"PDF saved. Extracting all {total_pages} pages (Standard)...")
            
            for i, page in enumerate(pdf.pages):
                if time.time() - start_time > MAX_EXECUTION_TIME:
                    log_func(f"⚠️ Time limit reached ({i}/{total_pages} pages). Returning partial text.")
                    text += "\n[...Truncated: Server Time Limit Reached...]\n"
                    break
                
                try:
                    extracted = page.extract_text(laparams={"detect_vertical": True}) 
                    if extracted:
                        text += extracted + "\n"
                except Exception as e:
                    pass 
                
                page.flush_cache()
                if i % 5 == 0: gc.collect()

        # --- STEP 2: OCR Fallback (Slow, for Image PDFs) ---
        # If we have very little text (< 50 chars), assume it's an image-based PDF
        if len(text.strip()) < 50:
            if OCR_AVAILABLE:
                log_func("⚠️ Standard extraction failed (Image PDF). Attempting OCR (Step 2)...")
                try:
                    # Convert PDF to images
                    # Note: limit to first 10-15 pages to prevent timeouts on large decks
                    images = convert_from_path(temp_filename, last_page=15)
                    
                    ocr_text = ""
                    for i, image in enumerate(images):
                        if time.time() - start_time > MAX_EXECUTION_TIME:
                            ocr_text += "\n[...OCR Truncated: Time Limit...]\n"
                            break
                            
                        # Try Japanese + English
                        page_str = pytesseract.image_to_string(image, lang='jpn+eng')
                        ocr_text += f"\n--- Page {i+1} (OCR) ---\n{page_str}\n"
                    
                    if len(ocr_text.strip()) > 0:
                        text = ocr_text
                        log_func(f"✅ OCR successful. Extracted {len(text)} characters.")
                    else:
                        log_func("❌ OCR yielded no text.")

                except Exception as e:
                    log_func(f"❌ OCR Error: {e} (Check poppler/tesseract installation)")
            else:
                log_func("⚠️ PDF is image-based but OCR libraries (pytesseract/pdf2image) are missing.")
                text += "\n[ERROR: This document is an image-based PDF. Text extraction failed.]\n"
                text += f"Link to original: {url}\n"

    except Exception as e:
        log_func(f"PDF Error: {e}")
        
    finally:
        if temp_filename and os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
            except: pass
        gc.collect()
        
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
        if not date_obj:
            curr = link
            for _ in range(3):
                parent = curr.parent
                if parent:
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
    
    query = f"{clean_ticker} ログミー"
    search_url = f"https://search.yahoo.co.jp/search?p={query}"
    
    log(f"Searching: {search_url}")
    
    try:
        soup = get_soup(search_url, log)
        if not soup: return None, logs
        
        company_url = None
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'finance.logmi.jp/companies/' in href:
                company_url = href
                if 'RU=' in company_url:
                    try:
                        import urllib.parse
                        qs = urllib.parse.parse_qs(urllib.parse.urlparse(company_url).query)
                        if 'RU' in qs: company_url = qs['RU'][0]
                    except: pass
                break
        
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

        soup = get_soup(company_url, log)
        if not soup: return None, logs
        
        items = analyze_company_page(soup, logs)
        if not items:
            log("No items found on company page.")
            return None, logs

        items.sort(key=lambda x: (x['date'], -x['priority']), reverse=True)
        latest_date = items[0]['date']
        
        window_start = latest_date - timedelta(days=10)
        recent_candidates = [i for i in items if i['date'] >= window_start]
        
        recent_candidates.sort(key=lambda x: x['priority'])
        
        current_winner = recent_candidates[0]
        log(f"✅ Primary: {current_winner['type']} ({current_winner['date'].strftime('%Y-%m-%d')})")

        secondary_doc = None
        
        transcripts = [i for i in items if i['type'] in ['HTML_TRANSCRIPT', 'PDF_TRANSCRIPT']]
        transcripts.sort(key=lambda x: x['date'], reverse=True)
        
        if transcripts:
            for tx in transcripts:
                days_diff = (current_winner['date'] - tx['date']).days
                if 60 < days_diff < 250:
                    secondary_doc = tx
                    log(f"✅ Context: {secondary_doc['type']} (Historical from {days_diff} days ago)")
                    break

        final_output = ""
        
        docs_to_fetch = [current_winner]
        if secondary_doc:
            docs_to_fetch.append(secondary_doc)

        for doc in docs_to_fetch:
            try:
                gc.collect()
                doc_text = ""
                header = format_doc_header(doc)
                
                if doc['type'] == 'HTML_TRANSCRIPT':
                    doc_text = stitch_html_transcript(doc, log)
                else:
                    log(f"Downloading PDF ({doc['type']})...")
                    doc_text = download_and_extract_pdf(doc['url'], log)

                # --- VALIDATION CHECK ---
                if doc_text and len(doc_text.strip()) > 50:
                    final_output += header + doc_text
                else:
                    # Logic: If it failed here, it failed both PDFMiner AND OCR
                    log(f"⚠️ Warning: {doc['type']} returned empty text even after OCR attempts.")
                    final_output += header + "\n[WARNING: No text could be extracted from this document.]\n"
                    final_output += f"Please view original: {doc['url']}\n"

            except Exception as e:
                log(f"Failed to fetch {doc['type']}: {e}")

        return final_output if final_output else None, logs
            
    except Exception as e:
        log(f"Global Scraper Error: {e}")
        return None, logs
    
    return None, logs
