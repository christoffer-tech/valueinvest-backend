import os
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from defeatbeta_api.data.ticker import Ticker
from jp_scraper import scrape_japanese_transcript
# Import the new scraper module
from scraper import get_transcript_data

app = Flask(__name__)
CORS(app)

def sanitize(data):
    """
    Recursively convert Pandas Timestamps and other non-JSON types.
    """
    if isinstance(data, dict):
        return {str(k): sanitize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize(v) for v in data]
    elif isinstance(data, (pd.Timestamp, pd.DatetimeIndex)):
        return str(data)
    elif hasattr(data, 'item'): 
        return data.item()
    return data

@app.route('/')
def home():
    return "ValueInvest AI Backend is Running on Render!"

@app.route('/api/transcript/<symbol>', methods=['GET'])
def get_transcript(symbol):
    # --- 1. JAPANESE STOCK HANDLER ---
    if symbol.endswith('.T') or (symbol.isdigit() and len(symbol) == 4):
        logs = []
        try:
            print(f"Fetching Japanese transcript for {symbol} via Logmi...")
            transcript_text, logs = scrape_japanese_transcript(symbol)
            
            if transcript_text:
                return jsonify({
                    "symbol": symbol,
                    "transcript": transcript_text,
                    "source": "Logmi (Japan)",
                    "debug_logs": logs
                })
            else:
                return jsonify({
                    "error": "No relevant Japanese transcript/material found",
                    "debug_logs": logs
                }), 404
        except Exception as e:
            print(f"JP Transcript Error: {e}")
            return jsonify({
                "error": str(e),
                "debug_logs": logs if logs else [str(e)]
            }), 500

    # --- 2. US STOCK HANDLER (Primary: DefeatBeta) ---
    try:
        print(f"Fetching transcript for {symbol} via DefeatBeta...")
        ticker = Ticker(symbol)
        transcripts = ticker.earning_call_transcripts()
        
        available_df = transcripts.get_transcripts_list()
        if available_df is not None and not available_df.empty:
            latest = available_df.sort_values(['fiscal_year', 'fiscal_quarter'], ascending=False).iloc[0]
            year = int(latest['fiscal_year'])
            quarter = int(latest['fiscal_quarter'])
            
            raw_data = transcripts.get_transcript(year, quarter)
            
            if raw_data is not None and not raw_data.empty:
                full_text = f"EARNINGS TRANSCRIPT: {symbol} | FY{year} Q{quarter}\n====================================\n"
                for _, row in raw_data.iterrows():
                    speaker = row['speaker'].upper() if row['speaker'] else "UNKNOWN"
                    content = row['content']
                    full_text += f"[{speaker}]: {content}\n\n"
                    
                return jsonify({
                    "symbol": symbol,
                    "year": year,
                    "quarter": quarter,
                    "transcript": full_text,
                    "source": "DefeatBeta"
                })
            else:
                print("DefeatBeta returned empty content. Proceeding to fallback...")
        else:
            print("DefeatBeta found no transcript list. Proceeding to fallback...")

    except Exception as e:
        print(f"DefeatBeta Error: {e}. Proceeding to fallback...")

    # --- 3. US STOCK HANDLER (Fallback: Custom Scraper) ---
    try:
        print(f"Attempting Fallback Scraper for {symbol}...")
        transcript_text, meta = get_transcript_data(symbol)
        
        if transcript_text:
            return jsonify({
                "symbol": symbol,
                "transcript": transcript_text,
                "source": f"Scraper ({meta.get('source', 'Unknown')})",
                "meta": meta
            })
        
        return jsonify({
            "error": "No transcripts found via DefeatBeta or Scraper",
            "details": meta if 'meta' in locals() and meta else "No candidates found"
        }), 404

    except Exception as e:
        print(f"Scraper Error: {e}")
        return jsonify({"error": f"All methods failed. Last error: {str(e)}"}), 500

@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        print(f"Fetching data for {ticker}...")
        
        # REMOVED custom session to fix "Yahoo API requires curl_cffi" error
        stock = yf.Ticker(ticker)
        period = request.args.get('range', '2y')
        
        # Force a history fetch first to check if the ticker is valid/accessible
        hist = stock.history(period=period)
        
        if hist.empty:
            # Fallback: Try fetching info to see if we get a specific error
            try:
                _ = stock.info
            except Exception as info_e:
                if "Too Many Requests" in str(info_e) or "429" in str(info_e):
                    return jsonify({"status": "error", "message": "Rate limited by upstream provider. Try again in 1 minute."}), 429
            return jsonify({"status": "error", "message": f"No price history found for {ticker}"}), 404

        # Robust Info Fetching
        try:
            info = sanitize(stock.info)
        except Exception as e:
            print(f"Info fetch warning: {e}")
            info = {}

        # Safe History Parsing
        history_list = []
        # Reset index to make 'Date' a column, easier to process than index
        hist_reset = hist.reset_index()
        for _, row in hist_reset.iterrows():
            # Handle different date column names (Date, Datetime)
            date_col = 'Date' if 'Date' in row else 'Datetime' if 'Datetime' in row else None
            date_val = str(row[date_col]) if date_col else "Unknown"
            
            history_list.append({
                'date': date_val,
                'close': row['Close'] if pd.notna(row['Close']) else None,
                'adjClose': row['Close'] if pd.notna(row['Close']) else None # Simplify: use Close if adj not present
            })
        
        financials = {}
        try:
            # Check if attributes exist before accessing to prevent crashes on different yf versions
            if hasattr(stock, 'income_stmt') and stock.income_stmt is not None and not stock.income_stmt.empty:
                financials['income'] = sanitize(stock.income_stmt.to_dict())
            if hasattr(stock, 'balance_sheet') and stock.balance_sheet is not None and not stock.balance_sheet.empty:
                financials['balance'] = sanitize(stock.balance_sheet.to_dict())
            if hasattr(stock, 'cashflow') and stock.cashflow is not None and not stock.cashflow.empty:
                financials['cashflow'] = sanitize(stock.cashflow.to_dict())
            
            # Quarterly
            if hasattr(stock, 'quarterly_income_stmt') and stock.quarterly_income_stmt is not None and not stock.quarterly_income_stmt.empty:
                financials['quarterly_income'] = sanitize(stock.quarterly_income_stmt.to_dict())
            if hasattr(stock, 'quarterly_balance_sheet') and stock.quarterly_balance_sheet is not None and not stock.quarterly_balance_sheet.empty:
                financials['quarterly_balance'] = sanitize(stock.quarterly_balance_sheet.to_dict())
            if hasattr(stock, 'quarterly_cashflow') and stock.quarterly_cashflow is not None and not stock.quarterly_cashflow.empty:
                financials['quarterly_cashflow'] = sanitize(stock.quarterly_cashflow.to_dict())
        except Exception as e:
            print(f"Financials warning for {ticker}: {e}")
        
        return jsonify({
            "status": "success",
            "info": info,
            "history": history_list,
            "financials": financials
        })
        
    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        # Explicitly return 429 if the error message implies rate limiting
        if "Too Many Requests" in str(e) or "429" in str(e):
             return jsonify({"status": "error", "message": "Too Many Requests. Rate limited. Try after a while."}), 429
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
