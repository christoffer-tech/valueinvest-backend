import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from defeatbeta_api.data.ticker import Ticker  # <--- ADDED IMPORT

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

# --- NEW TRANSCRIPT ENDPOINT ---
@app.route('/api/transcript/<symbol>', methods=['GET'])
def get_transcript(symbol):
    try:
        print(f"Fetching transcript for {symbol}...")
        ticker = Ticker(symbol)
        transcripts = ticker.earning_call_transcripts()
        
        # 1. Get list of available transcripts
        available_df = transcripts.get_transcripts_list()
        if available_df is None or available_df.empty:
             return jsonify({"error": "No transcripts found"}), 404
        
        # 2. Find latest (Sort by Year DESC, then Quarter DESC)
        latest = available_df.sort_values(['fiscal_year', 'fiscal_quarter'], ascending=False).iloc[0]
        year = int(latest['fiscal_year'])
        quarter = int(latest['fiscal_quarter'])
        
        # 3. Fetch specific transcript content
        raw_data = transcripts.get_transcript(year, quarter)
        
        if raw_data is None or raw_data.empty:
             return jsonify({"error": "Transcript content empty"}), 404

        # 4. Format into a readable string for the AI
        full_text = f"EARNINGS TRANSCRIPT: {symbol} | FY{year} Q{quarter}\n====================================\n"
        for _, row in raw_data.iterrows():
            speaker = row['speaker'].upper() if row['speaker'] else "UNKNOWN"
            content = row['content']
            full_text += f"[{speaker}]: {content}\n\n"
            
        return jsonify({
            "symbol": symbol,
            "year": year,
            "quarter": quarter,
            "transcript": full_text
        })

    except Exception as e:
        print(f"Transcript Error: {e}")
        # Return 500 but with JSON error so frontend handles it gracefully
        return jsonify({"error": str(e)}), 500

@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        period = request.args.get('range', '2y')
        
        info = sanitize(stock.info)
        
        hist = stock.history(period=period)
        history_list = []
        if not hist.empty:
            for idx, row in hist.iterrows():
                history_list.append({
                    'date': str(idx),
                    'close': row['Close'] if pd.notna(row['Close']) else None,
                    'adjClose': row['Close'] if pd.notna(row['Close']) else None
                })
        
        financials = {}
        try:
            if stock.income_stmt is not None and not stock.income_stmt.empty:
                financials['income'] = sanitize(stock.income_stmt.to_dict())
            if stock.balance_sheet is not None and not stock.balance_sheet.empty:
                financials['balance'] = sanitize(stock.balance_sheet.to_dict())
            if stock.cashflow is not None and not stock.cashflow.empty:
                financials['cashflow'] = sanitize(stock.cashflow.to_dict())
            if stock.quarterly_income_stmt is not None and not stock.quarterly_income_stmt.empty:
                financials['quarterly_income'] = sanitize(stock.quarterly_income_stmt.to_dict())
            if stock.quarterly_balance_sheet is not None and not stock.quarterly_balance_sheet.empty:
                financials['quarterly_balance'] = sanitize(stock.quarterly_balance_sheet.to_dict())
            if stock.quarterly_cashflow is not None and not stock.quarterly_cashflow.empty:
                financials['quarterly_cashflow'] = sanitize(stock.quarterly_cashflow.to_dict())
        except Exception as e:
            print(f"Financials warning: {e}")
        
        return jsonify({
            "status": "success",
            "info": info,
            "history": history_list,
            "financials": financials
        })
        
    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
