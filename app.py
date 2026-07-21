from flask import Flask, request, jsonify
from flask_cors import CORS
import os, requests, json, threading, time, logging
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)
CORS(app, origins=["https://precision-alpha-ai.netlify.app", "*"])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ALPACA_KEY        = os.environ.get("ALPACA_KEY", "")
ALPACA_SECRET     = os.environ.get("ALPACA_SECRET", "")
ALPACA_BASE_URL   = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
ALPACA_DATA_URL   = "https://data.alpaca.markets/v2"
EMAILJS_SERVICE   = os.environ.get("EMAILJS_SERVICE", "service_rucosmz")
EMAILJS_TEMPLATE  = os.environ.get("EMAILJS_TEMPLATE", "template_qajvk5t")
EMAILJS_PUBLIC    = os.environ.get("EMAILJS_PUBLIC", "i9a72iQL0ChaDHoZL")
ALERT_EMAIL       = os.environ.get("ALERT_EMAIL", "pinnacleperformancetax@gmail.com")

MARKET_SCAN_LIST = ['AAPL','TSLA','NVDA','SPY','QQQ','MSFT','AMD','META','GOOGL','AMZN','NFLX','SOFI','PLTR','RIVN','COIN']

RULES = {
    'maxDailyLoss': 50, 'maxTrades': 3, 'maxPositionSize': 200,
    'maxLossPerTrade': 15, 'takeProfitTarget': 30,
    'minConfidence': 75, 'maxVolatility': 70, 'minSyncScore': 75,
}

engine_state = {
    'running': False, 'weekly_trades': [], 'today_pl': 0.0,
    'last_date': '', 'week_key': '', 'scan_log': [], 'trade_log': [],
}

def get_week_key():
    d = datetime.now(pytz.timezone('America/New_York'))
    return f"{d.year}-W{d.isocalendar()[1]}"

def is_market_hours():
    est = datetime.now(pytz.timezone('America/New_York'))
    h, m = est.hour, est.minute
    return (h > 10 or (h == 10)) and (h < 15 or (h == 15 and m <= 30))

def reset_if_needed():
    today = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    if engine_state['last_date'] != today:
        engine_state['today_pl'] = 0.0
        engine_state['last_date'] = today
    wk = get_week_key()
    if engine_state['week_key'] != wk:
        engine_state['weekly_trades'] = []
        engine_state['week_key'] = wk

def alpaca_hdrs():
    return {'APCA-API-KEY-ID': ALPACA_KEY, 'APCA-API-SECRET-KEY': ALPACA_SECRET, 'Content-Type': 'application/json'}

def log_scan(msg):
    est = datetime.now(pytz.timezone('America/New_York'))
    entry = f"{est.strftime('%I:%M:%S %p')} — {msg}"
    engine_state['scan_log'].insert(0, entry)
    engine_state['scan_log'] = engine_state['scan_log'][:50]
    logger.info(msg)

def send_email(symbol, side, qty, price, reason, verdict):
    try:
        requests.post("https://api.emailjs.com/api/v1.0/email/send", json={
            "service_id": EMAILJS_SERVICE, "template_id": EMAILJS_TEMPLATE, "user_id": EMAILJS_PUBLIC,
            "template_params": {
                "to_email": ALERT_EMAIL,
                "subject": f"🤖 Precision Alpha: {verdict} — {side.upper()} {qty} {symbol}",
                "trade_symbol": symbol, "trade_side": side.upper(), "trade_qty": qty,
                "trade_price": f"${price:.2f}", "trade_total": f"${price*qty:.2f}",
                "trade_reason": reason, "trade_verdict": verdict,
                "trade_time": datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %I:%M %p EST'),
                "stop_loss": f"${price-(RULES['maxLossPerTrade']/qty):.2f}",
                "take_profit": f"${price+(RULES['takeProfitTarget']/qty):.2f}",
            }
        }, timeout=10)
    except Exception as e:
        logger.error(f"Email failed: {e}")

def quick_ai_check(symbol, price, price_change):
    prompt = f"""Precision Alpha AI auto-scanner. Evaluate for paper trade.
Stock: {symbol} | Price: ${price:.2f} | 1-day change: ${price_change:.2f} ({(price_change/max(price,1)*100):.1f}%)
Respond ONLY with JSON (no markdown): {{"confidence":0-100,"volatility":0-100,"sync":0-100,"side":"buy" or "sell","reason":"one sentence"}}
Be conservative. confidence>75, volatility<70, sync>75 required."""
    res = requests.post("https://api.anthropic.com/v1/messages",
        headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
        json={"model": "claude-haiku-4-5-20251001", "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]},
        timeout=20)
    text = res.json()['content'][0]['text'].replace('```json','').replace('```','').strip()
    return json.loads(text)

def auto_scan():
    reset_if_needed()
    if not is_market_hours():
        log_scan("⏰ Outside trading hours — scan skipped"); return
    if engine_state['today_pl'] <= -RULES['maxDailyLoss']:
        log_scan("🔴 Daily loss limit hit"); return
    if len(engine_state['weekly_trades']) >= RULES['maxTrades']:
        log_scan(f"🔴 Weekly limit reached ({RULES['maxTrades']})"); return

    log_scan(f"🔍 Scanning {len(MARKET_SCAN_LIST)} stocks...")
    for symbol in MARKET_SCAN_LIST:
        if len(engine_state['weekly_trades']) >= RULES['maxTrades']: break
        try:
            qr = requests.get(f"{ALPACA_DATA_URL}/stocks/{symbol}/trades/latest", headers=alpaca_hdrs(), timeout=10)
            if not qr.ok: continue
            price = qr.json().get('trade', {}).get('p', 0)
            if not price or price < 5 or price > 150: continue

            end = datetime.utcnow().isoformat() + 'Z'
            start = (datetime.utcnow() - timedelta(days=3)).isoformat() + 'Z'
            br = requests.get(f"{ALPACA_DATA_URL}/stocks/{symbol}/bars?timeframe=1Day&start={start}&end={end}&limit=5", headers=alpaca_hdrs(), timeout=10)
            price_change = 0
            if br.ok:
                bars = br.json().get('bars', [])
                if len(bars) >= 2: price_change = bars[-1]['c'] - bars[-2]['c']

            try:
                ai = quick_ai_check(symbol, price, price_change)
            except: continue

            conf, vol, sync = ai.get('confidence',0), ai.get('volatility',100), ai.get('sync',0)
            side, reason = ai.get('side','buy'), ai.get('reason','')

            if conf < RULES['minConfidence'] or vol > RULES['maxVolatility'] or sync < RULES['minSyncScore']:
                log_scan(f"⚫ {symbol} — blocked (C:{conf} V:{vol} S:{sync})"); continue

            qty = max(1, int(RULES['maxPositionSize'] / price))
            log_scan(f"✅ {symbol} — {side.upper()} signal. Placing...")

            or_ = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
                json={"symbol": symbol, "qty": str(qty), "side": side, "type": "market", "time_in_force": "day"}, timeout=10)
            if not or_.ok:
                log_scan(f"❌ {symbol} — order failed"); continue

            engine_state['weekly_trades'].append({'symbol': symbol, 'side': side, 'qty': qty, 'price': price})
            entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · AUTO: {side.upper()} {qty} {symbol} @ ${price:.2f} · {reason}"
            engine_state['trade_log'].insert(0, entry)
            engine_state['trade_log'] = engine_state['trade_log'][:50]
            log_scan(f"🚀 ORDER PLACED: {side.upper()} {qty} {symbol} @ ${price:.2f}")
            send_email(symbol, side, qty, price, reason, 'AUTO TRADE')
            break
        except Exception as e:
            log_scan(f"⚫ {symbol} — {str(e)[:40]}"); continue
        time.sleep(0.5)
    log_scan("✓ Scan complete — next in 5 min")

def engine_loop():
    while engine_state['running']:
        try: auto_scan()
        except Exception as e: logger.error(f"Engine error: {e}")
        time.sleep(300)

@app.route("/")
def index():
    return jsonify({"app": "Precision Alpha AI Backend", "status": "running", "mode": "paper-only"})

@app.route("/api/engine/start", methods=["POST"])
def start_engine():
    if not engine_state['running']:
        engine_state['running'] = True
        threading.Thread(target=engine_loop, daemon=True).start()
        log_scan("🚀 Auto engine started")
    return jsonify({"status": "running"})

@app.route("/api/engine/stop", methods=["POST"])
def stop_engine():
    engine_state['running'] = False
    log_scan("⏹ Auto engine stopped")
    return jsonify({"status": "stopped"})

@app.route("/api/engine/kill", methods=["POST"])
def kill_engine():
    engine_state['running'] = False
    log_scan("⛔ KILL SWITCH activated")
    return jsonify({"status": "killed"})

@app.route("/api/engine/status")
def engine_status():
    return jsonify({
        "running": engine_state['running'],
        "weekly_trades": len(engine_state['weekly_trades']),
        "today_pl": engine_state['today_pl'],
        "scan_log": engine_state['scan_log'][:30],
        "trade_log": engine_state['trade_log'][:20],
        "is_market_hours": is_market_hours(),
    })

@app.route("/api/bars/<symbol>")
def get_bars(symbol):
    try:
        res = requests.get(f"{ALPACA_DATA_URL}/stocks/{symbol}/bars?timeframe=1Day&start={request.args.get('start','')}&end={request.args.get('end','')}&limit=5", headers=alpaca_hdrs(), timeout=10)
        return jsonify(res.json()), res.status_code
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/quote/<symbol>")
def get_quote(symbol):
    try:
        res = requests.get(f"{ALPACA_DATA_URL}/stocks/{symbol}/trades/latest", headers=alpaca_hdrs(), timeout=10)
        return jsonify(res.json()), res.status_code
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/account")
def get_account():
    try:
        res = requests.get(f"{ALPACA_BASE_URL}/account", headers=alpaca_hdrs(), timeout=10)
        return jsonify(res.json()), res.status_code
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/positions")
def get_positions():
    try:
        res = requests.get(f"{ALPACA_BASE_URL}/positions", headers=alpaca_hdrs(), timeout=10)
        return jsonify(res.json()), res.status_code
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/orders", methods=["GET","POST"])
def orders():
    try:
        if request.method == "POST":
            res = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(), json=request.get_json(), timeout=10)
        else:
            res = requests.get(f"{ALPACA_BASE_URL}/orders?status={request.args.get('status','all')}&limit={request.args.get('limit','50')}", headers=alpaca_hdrs(), timeout=10)
        return jsonify(res.json()), res.status_code
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ai/analyze", methods=["POST"])
def ai_analyze():
    try:
        data = request.get_json()
        res = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": data.get("max_tokens", 500), "messages": [{"role": "user", "content": data.get("prompt", "")}]},
            timeout=25)
        return jsonify(res.json()), res.status_code
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
