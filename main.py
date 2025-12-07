import os
from flask import Flask, jsonify
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
        
        # 1. Basic Info
        # sanitize() handles potential Timestamps in the info dict
        info = sanitize(stock.info)
        
        # 2. History (2 Years for SMAs)
        hist = stock.history(period="2y")
        history_list = hist['Close'].tolist() if not hist.empty else []
        # Convert nan to None for valid JSON
        history_list = [x if x == x else None for x in history_list] 
        
        # 3. Financials (Safe retrieval)
        financials = {}
        try:
            # .to_dict() on DataFrames often results in Timestamp keys (columns are dates)
            # sanitize() fixes "keys must be str" error here
            if stock.income_stmt is not None:
                financials['income'] = sanitize(stock.income_stmt.to_dict())
            if stock.balance_sheet is not None:
                financials['balance'] = sanitize(stock.balance_sheet.to_dict())
            if stock.cashflow is not None:
                financials['cashflow'] = sanitize(stock.cashflow.to_dict())
        except Exception as e:
            print(f"Financials warning: {e}")

        return jsonify({
            "status": "success",
            "info": info,
            "history": history_list,
            "financials": financials
        })
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
