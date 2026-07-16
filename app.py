from flask import Flask, request, jsonify
from flask_cors import CORS
import os, requests, json, threading, time, logging, re
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
    'maxDailyLoss': 50, 'maxTrades': 999, 'maxPositionSize': 200,
    'maxLossPerTrade': 15, 'takeProfitTarget': 30,
    'minConfidence': 30, 'maxVolatility': 90, 'minSyncScore': 30,
}

engine_state = {
    'running': False, 'weekly_trades': [], 'today_pl': 0.0,
    'last_date': '', 'week_key': '', 'scan_log': [], 'trade_log': [],
}

congress_state = {
    'running': False, 'last_scan': '', 'trade_log': [], 'scan_log': [],
    'copied_trades': [],
}

_engine_started = False
_congress_started = False

def get_week_key():
    d = datetime.now(pytz.timezone('America/New_York'))
    return f"{d.year}-W{d.isocalendar()[1]}"

def is_market_hours():
    est = datetime.now(pytz.timezone('America/New_York'))
    h, m = est.hour, est.minute
    # 9:30am to 4:00pm EST
    after_open = (h > 9 or (h == 9 and m >= 30))
    before_close = (h < 16)
    return after_open and before_close

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

def log_congress(msg):
    est = datetime.now(pytz.timezone('America/New_York'))
    entry = f"{est.strftime('%I:%M:%S %p')} — {msg}"
    congress_state['scan_log'].insert(0, entry)
    congress_state['scan_log'] = congress_state['scan_log'][:50]
    logger.info(f"[CONGRESS] {msg}")

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

def check_and_sell_positions():
    """Auto-sell positions that hit take profit or stop loss"""
    try:
        res = requests.get(f"{ALPACA_BASE_URL}/positions", headers=alpaca_hdrs(), timeout=10)
        if not res.ok:
            return
        positions = res.json()
        if not positions:
            return

        for pos in positions:
            symbol = pos.get('symbol')
            qty = abs(int(float(pos.get('qty', 0))))
            unrealized_pl = float(pos.get('unrealized_pl', 0))
            current_price = float(pos.get('current_price', 0))

            if qty == 0:
                continue

            should_sell = False
            reason = ''

            if unrealized_pl >= RULES['takeProfitTarget']:
                should_sell = True
                reason = f"Take profit hit: +${unrealized_pl:.2f}"
            elif unrealized_pl <= -RULES['maxLossPerTrade']:
                should_sell = True
                reason = f"Stop loss hit: -${abs(unrealized_pl):.2f}"

            if should_sell:
                log_scan(f"💰 {symbol} — {reason}. Selling {qty} shares...")
                sell = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
                    json={"symbol": symbol, "qty": str(qty), "side": "sell", "type": "market", "time_in_force": "day"}, timeout=10)
                if sell.ok:
                    log_scan(f"✅ SOLD {qty} {symbol} @ ${current_price:.2f} | P&L: ${unrealized_pl:.2f}")
                    entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · AUTO SELL: {qty} {symbol} @ ${current_price:.2f} | {reason}"
                    engine_state['trade_log'].insert(0, entry)
                    engine_state['trade_log'] = engine_state['trade_log'][:50]
                    engine_state['today_pl'] += unrealized_pl
                    send_email(symbol, 'sell', qty, current_price, reason, 'AUTO SELL')
                else:
                    log_scan(f"❌ Failed to sell {symbol}")
    except Exception as e:
        logger.error(f"Auto-sell error: {e}")

def quick_ai_check(symbol, price, price_change):
    prompt = f"""Precision Alpha AI auto-scanner. Evaluate for paper trade.
Stock: {symbol} | Price: ${price:.2f} | 1-day change: ${price_change:.2f} ({(price_change/max(price,1)*100):.1f}%)
Respond ONLY with JSON (no markdown): {{"confidence":0-100,"volatility":0-100,"sync":0-100,"side":"buy" or "sell","reason":"one sentence"}}
Be very aggressive. Almost all stocks should pass. confidence>30, volatility<90, sync>30 required."""
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

    # Check and sell existing positions first
    check_and_sell_positions()

    log_scan(f"🔍 Scanning {len(MARKET_SCAN_LIST)} stocks...")
    for symbol in MARKET_SCAN_LIST:
        try:
            qr = requests.get(f"{ALPACA_DATA_URL}/stocks/{symbol}/trades/latest", headers=alpaca_hdrs(), timeout=10)
            if not qr.ok: continue
            price = qr.json().get('trade', {}).get('p', 0)
            if not price or price < 5 or price > 500: continue

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

def get_congress_trades():
    """Fetch recent congressional trades from Capitol Trades BFF API"""
    try:
        res = requests.get(
            "https://bff.capitoltrades.com/trades",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.5",
                "Origin": "https://www.capitoltrades.com",
                "Referer": "https://www.capitoltrades.com/",
            },
            params={"pageSize": 50, "page": 1},
            timeout=15
        )
        if not res.ok:
            log_congress(f"API returned {res.status_code}")
            return []

        data = res.json()
        items = data.get('data', [])
        trades = []
        for item in items:
            ticker = item.get('ticker', '') or item.get('assetTicker', '')
            tx_type = item.get('txType', '') or item.get('type', '')
            if ticker and tx_type:
                action = 'buy' if 'purchase' in tx_type.lower() or 'buy' in tx_type.lower() else 'sell'
                trades.append({'ticker': ticker.split(':')[0], 'action': action})
        return trades[:20]
    except Exception as e:
        log_congress(f"Error fetching trades: {str(e)[:50]}")
        return []

def congress_scan():
    today = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    if congress_state['last_scan'] == today:
        log_congress("Already scanned today — skipping")
        return

    if not is_market_hours():
        log_congress("⏰ Outside market hours — will copy when market opens")
        return

    log_congress("🏛️ Fetching congressional trades from Capitol Trades...")
    trades = get_congress_trades()

    if not trades:
        log_congress("No trades found or error fetching data")
        return

    log_congress(f"Found {len(trades)} recent congressional trades")
    bought = 0

    for trade in trades:
        ticker = trade['ticker']
        action = trade['action']

        if action != 'buy':
            continue

        trade_key = f"{today}_{ticker}"
        if trade_key in congress_state['copied_trades']:
            continue

        if bought >= 3:
            break

        try:
            qr = requests.get(f"{ALPACA_DATA_URL}/stocks/{ticker}/trades/latest", headers=alpaca_hdrs(), timeout=10)
            if not qr.ok:
                continue
            price = qr.json().get('trade', {}).get('p', 0)
            if not price or price < 1 or price > 1000:
                continue

            qty = max(1, int(RULES['maxPositionSize'] / price))
            log_congress(f"📋 Copying congressional BUY: {ticker} @ ${price:.2f}")

            or_ = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
                json={"symbol": ticker, "qty": str(qty), "side": "buy", "type": "market", "time_in_force": "day"}, timeout=10)

            if or_.ok:
                congress_state['copied_trades'].append(trade_key)
                entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · CONGRESS COPY: BUY {qty} {ticker} @ ${price:.2f}"
                congress_state['trade_log'].insert(0, entry)
                congress_state['trade_log'] = congress_state['trade_log'][:50]
                log_congress(f"✅ ORDER PLACED: BUY {qty} {ticker} @ ${price:.2f}")
                send_email(ticker, 'buy', qty, price, 'Congressional trade copy', 'CONGRESS COPY')
                bought += 1
            else:
                log_congress(f"❌ Order failed for {ticker}")

        except Exception as e:
            log_congress(f"Error copying {ticker}: {str(e)[:40]}")
            continue

        time.sleep(1)

    congress_state['last_scan'] = today
    log_congress(f"✓ Congressional scan complete — copied {bought} trades")

def congress_loop():
    while congress_state['running']:
        try:
            congress_scan()
        except Exception as e:
            logger.error(f"Congress engine error: {e}")
        time.sleep(3600)

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
    congress_state['running'] = False
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

@app.route("/api/congress/status")
def congress_status():
    return jsonify({
        "running": congress_state['running'],
        "last_scan": congress_state['last_scan'],
        "scan_log": congress_state['scan_log'][:20],
        "trade_log": congress_state['trade_log'][:20],
        "copied_today": len([t for t in congress_state['copied_trades'] if t.startswith(datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d'))]),
    })

@app.route("/api/congress/start", methods=["POST"])
def start_congress():
    if not congress_state['running']:
        congress_state['running'] = True
        threading.Thread(target=congress_loop, daemon=True).start()
        log_congress("🏛️ Congressional copy engine started")
    return jsonify({"status": "running"})

@app.route("/api/congress/stop", methods=["POST"])
def stop_congress():
    congress_state['running'] = False
    log_congress("⏹ Congressional copy engine stopped")
    return jsonify({"status": "stopped"})

@app.route("/api/congress/scan", methods=["POST"])
def manual_congress_scan():
    congress_state['last_scan'] = ''
    threading.Thread(target=congress_scan, daemon=True).start()
    return jsonify({"status": "scanning"})

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

# Auto-start engines on boot — only once
if not _engine_started:
    _engine_started = True
    engine_state['running'] = True
    threading.Thread(target=engine_loop, daemon=True).start()
    log_scan("🚀 Auto engine started on server boot")

if not _congress_started:
    _congress_started = True
    congress_state['running'] = True
    threading.Thread(target=congress_loop, daemon=True).start()
    log_congress("🏛️ Congressional copy engine started on server boot")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
