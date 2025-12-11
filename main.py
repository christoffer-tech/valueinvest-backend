import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd

app = Flask(__name__)
CORS(app)

def sanitize(data):
    """
    Recursively convert Pandas Timestamps and other non-JSON types to strings/native types.
    Fixes 'keys must be str' error.
    """
    if isinstance(data, dict):
        return {str(k): sanitize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize(v) for v in data]
    elif isinstance(data, (pd.Timestamp, pd.DatetimeIndex)):
        return str(data)
    elif hasattr(data, 'item'):  # Handle numpy float/int types
        return data.item()
    return data

@app.route('/')
def home():
    return "ValueInvest AI Backend is Running on Render!"

@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        
        # Get optional parameters
        period = request.args.get('range', '2y')
        
        # 1. Basic Info
        info = sanitize(stock.info)
        
        # 2. History
        hist = stock.history(period=period)
        history_list = []
        if not hist.empty:
            # Convert to list of dicts with date, close, adjClose
            for idx, row in hist.iterrows():
                history_list.append({
                    'date': str(idx),
                    'close': row['Close'] if pd.notna(row['Close']) else None,
                    'adjClose': row['Close'] if pd.notna(row['Close']) else None
                })
        
        # 3. Financials (CRITICAL FIX: Add Quarterly Data)
        financials = {}
        try:
            # === ANNUAL DATA ===
            if stock.income_stmt is not None and not stock.income_stmt.empty:
                financials['income'] = sanitize(stock.income_stmt.to_dict())
            if stock.balance_sheet is not None and not stock.balance_sheet.empty:
                financials['balance'] = sanitize(stock.balance_sheet.to_dict())
            if stock.cashflow is not None and not stock.cashflow.empty:
                financials['cashflow'] = sanitize(stock.cashflow.to_dict())
            
            # === QUARTERLY DATA (THIS WAS MISSING!) ===
            if stock.quarterly_income_stmt is not None and not stock.quarterly_income_stmt.empty:
                financials['quarterly_income'] = sanitize(stock.quarterly_income_stmt.to_dict())
            if stock.quarterly_balance_sheet is not None and not stock.quarterly_balance_sheet.empty:
                financials['quarterly_balance'] = sanitize(stock.quarterly_balance_sheet.to_dict())
            if stock.quarterly_cashflow is not None and not stock.quarterly_cashflow.empty:
                financials['quarterly_cashflow'] = sanitize(stock.quarterly_cashflow.to_dict())
                
        except Exception as e:
            print(f"Financials warning: {e}")
        
        print(f"✅ Successfully fetched {ticker}. Quarterly income periods: {len(financials.get('quarterly_income', {}))}")
        
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
