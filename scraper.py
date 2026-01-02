import os
import sys
import warnings
import logging
import re
import time
import urllib.parse
from bs4 import BeautifulSoup
from curl_cffi import requests

# --- 1. CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)

# --- 2. SEARCH LOGIC (FIXED) ---

def search_bing_broad(query):
    """
    Searches Bing broadly (without strict site: operators) to avoid 0-result blocks.
    """
    try:
        # We add 'investing.com' as a keyword, not a strict operator
        full_query = f"{query} investing.com"
        url = f"https://www.bing.com/search?q={urllib.parse.quote(full_query)}&setmkt=en-US"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        
        # Use curl_cffi to mimic a real Chrome browser
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, headers=headers, timeout=20)
        
        if resp.status_code != 200: 
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        links = []
        
        # Extract all result links
        for h2 in soup.find_all('h2'):
            a = h2.find('a', href=True)
            if a: links.append(a['href'])
        
        return links
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return []

def filter_investing_links(raw_links):
    """
    Filters raw links for valid Investing.com transcripts.
    """
    valid = []
    seen = set()

    for link in raw_links:
        if link in seen: continue
        seen.add(link)
        l = link.lower()
        
        # 1. MUST be investing.com
        if "investing.com" not in l: continue

        # 2. MUST be a transcript or news article
        # Valid patterns:
        # - /news/transcripts/earnings-call-transcript...
        # - /equities/vestas...earnings-calls-transcripts
        if "/news/" in l or "/equities/" in l:
            if "transcript" in l or "earnings-call" in l:
                valid.append(link)

    return valid

def parse_quarter_score(url):
    """Scores URLs based on recency (Year/Quarter) found in the slug."""
    # Heuristic: Higher score = Newer
    # Extract year
    year_match = re.search(r'20(\d{2})', url)
    year = int("20" + year_match.group(1)) if year_match else 2020
    
    # Extract quarter
    q_map = {"q1": 1, "q2": 2, "q3": 3, "q4": 4}
    q = 0
    for k, v in q_map.items():
        if k in url.lower():
            q = v
            break
            
    return (year * 10) + q

# --- 3. PAGE PARSER ---

def fetch_transcript_text(url):
    try:
        logger.info(f"📥 Fetching: {url}")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        
        session = requests.Session(impersonate="chrome120")
        resp = session.get(url, headers=headers, timeout=25)
        
        if resp.status_code != 200:
            return None, f"Status {resp.status_code}"

        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # 1. Locate Content
        # Investing.com article body is usually in 'WYSIWYG' or 'articlePage' class
        body = soup.find('div', class_='WYSIWYG') or soup.find('div', class_='articlePage')
        
        if not body:
            return None, "Could not find article body div"

        # 2. Clean Junk (Ads, etc)
        for tag in body(["script", "style", "iframe"]):
            tag.decompose()
        
        # Remove 'Related Articles' divs
        for div in body.find_all('div'):
            if "related" in str(div.get('class', [])) or "carousel" in str(div.get('class', [])):
                div.decompose()

        # 3. Extract Text
        paragraphs = [p.get_text().strip() for p in body.find_all(['p', 'h2']) if p.get_text().strip()]
        
        # Filter out boilerplate
        clean_paragraphs = []
        for p in paragraphs:
            if "Position:" in p: continue
            if "disclosure" in p.lower(): continue
            clean_paragraphs.append(p)
            
        full_text = "\n\n".join(clean_paragraphs)
        
        title = soup.title.string.strip() if soup.title else "No Title"
        return full_text, title

    except Exception as e:
        return None, str(e)

# --- 4. MAIN ---

if __name__ == "__main__":
    # Name is better than ticker for transcript searching
    COMPANY = "Vestas Wind Systems" 
    
    print(f"--- 🔎 Searching for {COMPANY} Transcripts ---")
    
    # 1. Search
    query = f"{COMPANY} earnings call transcript"
    raw_links = search_bing_broad(query)
    valid_links = filter_investing_links(raw_links)
    
    if not valid_links:
        print("❌ No Investing.com links found. (Search engine might be blocking or empty)")
        sys.exit(1)
        
    print(f"✅ Found {len(valid_links)} potential links.")
    
    # 2. Sort by Date (Best guess from URL)
    valid_links.sort(key=parse_quarter_score, reverse=True)
    target_url = valid_links[0]
    
    print(f"🎯 Target URL: {target_url}")
    
    # 3. Fetch
    text, title = fetch_transcript_text(target_url)
    
    if text:
        print("\n" + "="*60)
        print(f"TITLE: {title}")
        print("="*60 + "\n")
        print(text[:1500] + "...\n")
        print("="*60)
        
        # Save
        with open("vestas_transcript.txt", "w", encoding="utf-8") as f:
            f.write(f"URL: {target_url}\n\n{text}")
        print("✅ Transcript saved to 'vestas_transcript.txt'")
    else:
        print(f"❌ Failed to parse content: {title}")
