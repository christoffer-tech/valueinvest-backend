import os
from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return "ValueInvest AI Backend is Running on Railway!"

@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        
        # 1. Basic Info
        # fast_info is faster, but info contains the specific keys your frontend expects
        info = stock.info 
        
        # 2. History (2 Years for SMAs)
        hist = stock.history(period="2y")
        history_list = hist['Close'].tolist() if not hist.empty else []
        
        # 3. Financials (Safe retrieval)
        financials = {}
        try:
            financials['income'] = stock.income_stmt.to_dict() if stock.income_stmt is not None else {}
            financials['balance'] = stock.balance_sheet.to_dict() if stock.balance_sheet is not None else {}
            financials['cashflow'] = stock.cashflow.to_dict() if stock.cashflow is not None else {}
        except:
            pass

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
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
