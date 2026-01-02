import requests
from bs4 import BeautifulSoup
import re
import time
import random
import gc
import os
import logging
from urllib.parse import urljoin
from datetime import datetime, timedelta
import fitz  # PyMuPDF
import google.generativeai as genai

# --- CONFIGURATION ---
# Configure Gemini API
GENAI_KEY = os.environ.get("GEMINI_API_KEY")
if GENAI_KEY:
    genai.configure(api_key=GENAI_KEY)

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

def format_doc_header(item, label_override=None):
    t = item['type']
    label = label_override if label_override else "DOCUMENT"
    if not label_override:
        if "TRANSCRIPT" in t: label = "EARNINGS CALL TRANSCRIPT"
        elif "PRESENTATION" in t: label = "PRESENTATION SLIDES"
        elif "TANSHIN" in t: label = "FINANCIAL RESULTS (TANSHIN)"
        
    return f"\n\n{'='*40}\n=== {label} ===\nDATE: {item['date'].strftime('%Y-%m-%d')}\nTYPE: {t}\n{'='*40}\n"

# --- PYMUPDF TEXT EXTRACTOR ---
def extract_text_from_pdf_bytes(pdf_bytes, log_func=print):
    text = ""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            # Check if PDF is encrypted or empty
            if doc.is_encrypted:
                log_func("⚠️ PDF is encrypted.")
                return None
                
            total_pages = len(doc)
            log_func(f"   (PDF has {total_pages} pages)")
            
            for page in doc:
                # flags=fitz.TEXT_PRESERVE_WHITESPACE helps with layout
                extracted = page.get_text("text", flags=fitz.TEXT_PRESERVE_WHITESPACE)
                if extracted:
                    text += extracted + "\n"
            
            # Heuristic: If text is extremely short relative to page count, it's likely images
            if len(text.strip()) < 100 and total_pages > 0:
                log_func("⚠️ Extracted text is too short (likely image-based PDF).")
                return None
                
    except Exception as e:
        log_func(f"PyMuPDF Error: {e}")
        return None
        
    return text

# --- GEMINI VISION FALLBACK ---
def process_vision_fallback(pdf_bytes, item, log_func=print):
    """
    Slices the first 15 pages of the PDF and sends them to Gemini 2.5 Flash
    for direct analysis (OCR/Vision).
    """
    if not GENAI_KEY:
        log_func("❌ Gemini API Key not found. Cannot perform vision fallback.")
        return None

    log_func(f"👀 Initiating Gemini Vision Fallback for {item['type']}...")

    try:
        # 1. Create a new PDF in memory with only the first 15 pages
        with fitz.open(stream=pdf_bytes, filetype="pdf") as src_doc:
            last_page = min(15, len(src_doc)) - 1
            src_doc.select(range(last_page + 1)) # Keep only first 15 pages
            trimmed_bytes = src_doc.tobytes()
            log_func(f"   Sliced PDF to first {last_page + 1} pages for analysis.")

        # 2. Configure Model
        model = genai.GenerativeModel('gemini-2.0-flash-exp') # Or 'gemini-1.5-flash' depending on access
        
        # 3. Prompt
        prompt = (
            "You are analyzing the first 15 slides of a Japanese financial presentation. "
            "Extract the key financial results (Revenue, Operating Profit), strategic highlights, "
            "and any forecasts for the next period. "
            "Output the data as a clean text summary."
        )

        # 4. Generate
        log_func("   Sending to Gemini...")
        response = model.generate_content([
            prompt,
            {
                "mime_type": "application/pdf",
                "data": trimmed_bytes
            }
        ])
        
        return response.text

    except Exception as e:
        log_func(f"❌ Gemini Vision Error: {e}")
        return None

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

        # Sort: Latest date first, then by priority (Transcript < Presentation < Tanshin)
        items.sort(key=lambda x: (x['date'], -x['priority']), reverse=True)
        latest_date = items[0]['date']
        
        # Get all recent items (last 14 days from latest event)
        window_start = latest_date - timedelta(days=14)
        candidates = [i for i in items if i['date'] >= window_start]
        candidates.sort(key=lambda x: x['priority']) # Sort by Priority
        
        final_text = ""
        best_pdf_for_fallback = None
        best_pdf_bytes = None
        
        # --- PHASE 1: Try to extract Pure Text ---
        for doc in candidates:
            log(f"⬇️ Processing Candidate: {doc['type']} ({doc['date'].strftime('%Y-%m-%d')})")
            
            try:
                if doc['type'] == 'HTML_TRANSCRIPT':
                    txt = stitch_html_transcript(doc, log)
                    if txt and len(txt) > 200:
                        final_text = format_doc_header(doc) + txt
                        return final_text, logs
                else:
                    # It's a PDF
                    log(f"   Downloading PDF...")
                    r = requests.get(doc['url'], headers=get_headers(), timeout=30)
                    r.raise_for_status()
                    pdf_data = r.content
                    
                    # Store as potential fallback if it's a Presentation/Tanshin
                    if not best_pdf_for_fallback and doc['type'] in ['PDF_PRESENTATION', 'PDF_TANSHIN']:
                        best_pdf_for_fallback = doc
                        best_pdf_bytes = pdf_data
                    
                    # Try PyMuPDF Extraction
                    txt = extract_text_from_pdf_bytes(pdf_data, log)
                    if txt and len(txt) > 200:
                        log(f"✅ Success! Extracted text from {doc['type']}.")
                        final_text = format_doc_header(doc) + txt
                        return final_text, logs
                    
            except Exception as e:
                log(f"❌ Failed to process {doc['type']}: {e}")
                continue

        # --- PHASE 2: Fallback (Vision Analysis) ---
        # If we are here, we have no text. 
        # If we have a stored PDF (Presentation/Tanshin), send slides to Gemini.
        if best_pdf_for_fallback and best_pdf_bytes:
            log("⚠️ Text extraction failed for all docs. Attempting Gemini Vision fallback...")
            
            vision_text = process_vision_fallback(best_pdf_bytes, best_pdf_for_fallback, log)
            
            if vision_text:
                header = format_doc_header(best_pdf_for_fallback, label_override="VISION ANALYSIS OF SLIDES")
                final_text = header + vision_text
                return final_text, logs
            else:
                log("❌ Vision fallback failed or produced no output.")

        return None, logs
            
    except Exception as e:
        log(f"Global Scraper Error: {e}")
        return None, logs
    
    return None, logs
