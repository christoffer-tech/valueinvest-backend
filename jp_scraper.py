# ... imports ...

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
        soup = get_soup(search_url)
        if not soup:
            log("Failed to fetch Yahoo Search results (soup is None)")
            return None, logs # Return tuple
            
        company_url = None
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'finance.logmi.jp/companies/' in href:
                company_url = href
                log(f"Found Company URL: {company_url}")
                break
        
        if not company_url:
            log("No Logmi URL found in search results.")
            return None, logs

        # ... (rest of your scraping logic, replace all print() with log()) ...
        
        # When returning success:
        return full_text, logs

    except Exception as e:
        log(f"EXCEPTION: {str(e)}")
        return None, logs
