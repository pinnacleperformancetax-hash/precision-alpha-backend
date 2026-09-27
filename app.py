from flask import Flask, request, jsonify
from flask_cors import CORS
from functools import wraps
import os, requests, json, threading, time, logging, re
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)
# NOTE: CORS previously allowed "*" alongside the named Netlify origin, which
# defeats the point of naming an origin at all (any site could call this API
# from a browser). Restricted to just your frontend's real origin.
CORS(app, origins=["https://precision-alpha-ai.netlify.app"])

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

# Shared secret for state-changing routes (placing orders, starting/stopping
# engines, changing settings). Set this in Render's environment variables —
# it is NOT hardcoded here. Until you set it, these routes stay open and a
# warning is logged on every protected request, so nothing breaks before you
# configure it, but you should set API_KEY as soon as possible.
API_KEY = os.environ.get("API_KEY", "")

def require_api_key(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not API_KEY:
            logger.warning(f"⚠️ API_KEY not set — {request.path} is UNPROTECTED")
            return fn(*args, **kwargs)
        supplied = request.headers.get("X-API-Key", "")
        if supplied != API_KEY:
            return jsonify({"error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper

MARKET_SCAN_LIST = ['AAPL','TSLA','NVDA','SPY','QQQ','MSFT','AMD','META','GOOGL','AMZN','NFLX','SOFI','PLTR','RIVN','COIN','SMCI','ARM','UBER','ORCL','JPM','V','DIS','BAC','CRM','PANW']

RULES = {
    'maxDailyLoss': 30, 'maxTrades': 999, 'maxPositionSize': 200,
    'maxLossPerTrade': 9, 'takeProfitTarget': 30,
    'minConfidence': 30, 'maxVolatility': 90, 'minSyncScore': 30, 'maxSharesPerStock': 5, 'takeProfitPct': 15,
}

engine_state = {
    'running': False, 'weekly_trades': [], 'today_pl': 0.0,
    'last_date': '', 'week_key': '', 'scan_log': [], 'trade_log': [],
}

congress_state = {
    'running': False, 'last_scan': '', 'trade_log': [], 'scan_log': [],
    'copied_trades': [],
}

# ---- State persistence ----
# engine_state and congress_state previously lived only in memory, so any
# server restart (a redeploy, a crash, Render's free-tier spin-down after
# inactivity) silently wiped them — including congress_state['copied_trades'],
# the guard that stops congress_scan() from re-buying the same ticker twice in
# one day. On 2026-09-27 this caused GOOGL and AMZN to be bought 4x each in
# one day, one extra buy per redeploy, because the guard kept resetting.
#
# This writes both state dicts to a JSON file on disk after every change and
# reloads them on boot. IMPORTANT LIMITATION: Render's free-tier filesystem is
# ephemeral on a fresh deploy (a new code push wipes local files, not just
# memory) — so this protects against restarts *without* a code change (crash,
# spin-down/wake, manual restart), which is the common case, but a real
# redeploy still starts from blank state. For durability across every kind of
# restart including deploys, state would need to live in an external store
# (Postgres/Redis) rather than a local file — worth doing when the
# multi-tenant rebuild happens.
STATE_FILE = os.environ.get("STATE_FILE_PATH", "/tmp/precision_alpha_state.json")
_state_lock = threading.Lock()

def save_state():
    try:
        with _state_lock:
            with open(STATE_FILE, 'w') as f:
                json.dump({'engine_state': engine_state, 'congress_state': congress_state}, f)
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def load_state():
    try:
        if not os.path.exists(STATE_FILE):
            logger.info("No saved state file found — starting fresh")
            return
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
        engine_state.update(data.get('engine_state', {}))
        congress_state.update(data.get('congress_state', {}))
        logger.info(f"✅ Restored state from {STATE_FILE} — congress copied_trades: {len(congress_state.get('copied_trades', []))}, engine running: {engine_state.get('running')}")
    except Exception as e:
        logger.error(f"Failed to load state, starting fresh: {e}")

load_state()

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
    changed = False
    if engine_state['last_date'] != today:
        engine_state['today_pl'] = 0.0
        engine_state['last_date'] = today
        changed = True
    wk = get_week_key()
    if engine_state['week_key'] != wk:
        engine_state['weekly_trades'] = []
        engine_state['week_key'] = wk
        changed = True
    if changed:
        save_state()

def get_real_today_pl():
    """Get actual today P&L from Alpaca account"""
    try:
        res = requests.get(f"{ALPACA_BASE_URL}/account", headers=alpaca_hdrs(), timeout=10)
        if not res.ok:
            return engine_state['today_pl']
        data = res.json()
        # equity - last_equity = today's P&L
        equity = float(data.get('equity', 0))
        last_equity = float(data.get('last_equity', equity))
        today_pl = equity - last_equity
        return today_pl
    except:
        return engine_state['today_pl']

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

            # Calculate per-share P&L
            qty_pos = abs(int(float(pos.get('qty', 1))))
            avg_entry = float(pos.get('avg_entry_price', 0))
            current_price = float(pos.get('current_price', 0))
            per_share_pl = current_price - avg_entry if avg_entry > 0 else 0
            pct_gain = ((current_price - avg_entry) / avg_entry * 100) if avg_entry > 0 else 0

            if per_share_pl <= -RULES['maxLossPerTrade']:
                should_sell = True
                reason = f"Stop loss: ${per_share_pl:.2f}/share ({pct_gain:.1f}%)"
            elif pct_gain >= RULES['takeProfitPct']:
                should_sell = True
                reason = f"Take profit: +${per_share_pl:.2f}/share (+{pct_gain:.1f}%)"

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
                    save_state()
                    send_email(symbol, 'sell', qty, current_price, reason, 'AUTO SELL')
                else:
                    log_scan(f"❌ Failed to sell {symbol}")
    except Exception as e:
        logger.error(f"Auto-sell error: {e}")

def get_sentiment(symbol):
    """Get sentiment score from recent news headlines using AI"""
    try:
        # Get recent news from Alpaca
        res = requests.get(
            f"https://data.alpaca.markets/v1beta1/news?symbols={symbol}&limit=5",
            headers=alpaca_hdrs(), timeout=10
        )
        if not res.ok:
            return None
        
        news_items = res.json().get('news', [])
        if not news_items:
            return None
        
        # Extract headlines
        headlines = [item.get('headline', '') for item in news_items if item.get('headline')]
        if not headlines:
            return None
        
        # Ask Claude to analyze sentiment
        headlines_text = "\n".join([f"- {h}" for h in headlines[:5]])
        sentiment_prompt = f"""Analyze the sentiment of these recent news headlines for {symbol} stock.
Headlines:
{headlines_text}

Respond ONLY with JSON (no markdown), matching this exact shape:
{{"sentiment": "bullish", "score": 42, "summary": "one sentence"}}
Where "sentiment" is one of: bullish, bearish, neutral. "score" is a number from -100 (very bearish) to 100 (very bullish)."""

        res = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 150, "messages": [{"role": "user", "content": sentiment_prompt}]},
            timeout=15)
        if not res.ok:
            logger.error(f"Sentiment API error for {symbol}: HTTP {res.status_code} — {res.text[:300]}")
            return None
        body = res.json()
        if 'content' not in body:
            logger.error(f"Sentiment API returned no 'content' for {symbol}: {body}")
            return None
        text = body['content'][0]['text'].replace('```json','').replace('```','').strip()
        sentiment_data = json.loads(text)
        sentiment_data['headlines'] = headlines[:3]
        return sentiment_data
    except json.JSONDecodeError as e:
        logger.error(f"Sentiment JSON parse error for {symbol}: {e} — raw text was not valid JSON")
        return None
    except Exception as e:
        logger.error(f"Sentiment error for {symbol}: {e}")
        return None

def get_volume_data(symbol):
    """Get volume data for a symbol"""
    try:
        end = datetime.utcnow().isoformat() + 'Z'
        start = (datetime.utcnow() - timedelta(days=10)).isoformat() + 'Z'
        res = requests.get(
            f"{ALPACA_DATA_URL}/stocks/{symbol}/bars?timeframe=1Day&start={start}&end={end}&limit=10",
            headers=alpaca_hdrs(), timeout=10
        )
        if not res.ok:
            return None
        bars = res.json().get('bars', [])
        if len(bars) < 2:
            return None
        
        # Calculate volume metrics
        recent_vol = bars[-1].get('v', 0)
        avg_vol = sum(b.get('v', 0) for b in bars[:-1]) / max(len(bars) - 1, 1)
        vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 1
        
        # Price momentum (5 day)
        oldest_close = bars[0].get('c', 0)
        newest_close = bars[-1].get('c', 0)
        momentum_5d = ((newest_close - oldest_close) / max(oldest_close, 1)) * 100
        
        # Intraday range
        high = bars[-1].get('h', 0)
        low = bars[-1].get('l', 0)
        close = bars[-1].get('c', 0)
        open_price = bars[-1].get('o', 0)
        
        return {
            'volume': recent_vol,
            'avg_volume': int(avg_vol),
            'volume_ratio': round(vol_ratio, 2),
            'momentum_5d': round(momentum_5d, 2),
            'high': high,
            'low': low,
            'close': close,
            'open': open_price,
            'above_open': close > open_price,
        }
    except Exception as e:
        logger.error(f"Volume data error for {symbol}: {e}")
        return None

def quick_ai_check(symbol, price, price_change):
    """Enhanced AI check with volume and price momentum analysis"""
    # Get volume data
    vol_data = get_volume_data(symbol)
    
    # Build enhanced prompt with volume and momentum data
    vol_info = ""
    if vol_data:
        vol_info = f"""
Volume Analysis:
- Today volume: {vol_data['volume']:,} shares
- Avg volume (9 days): {vol_data['avg_volume']:,} shares  
- Volume ratio: {vol_data['volume_ratio']}x average {'(HIGH VOLUME - strong signal)' if vol_data['volume_ratio'] > 1.5 else '(normal volume)'}
- 5-day price momentum: {vol_data['momentum_5d']:+.2f}%
- Trading above open price: {vol_data['above_open']}
- Day range: ${vol_data['low']:.2f} - ${vol_data['high']:.2f}"""

    # Get sentiment data
    sentiment = get_sentiment(symbol)
    sentiment_info = ""
    if sentiment:
        sentiment_info = f"""
News Sentiment Analysis:
- Overall sentiment: {sentiment.get('sentiment', 'neutral').upper()}
- Sentiment score: {sentiment.get('score', 0)}/100
- Summary: {sentiment.get('summary', 'No summary')}
- Recent headlines: {'; '.join(sentiment.get('headlines', [])[:2])}"""

    prompt = f"""Precision Alpha AI auto-scanner. Evaluate for paper trade.

Stock: {symbol} | Price: ${price:.2f} | 1-day change: ${price_change:.2f} ({(price_change/max(price,1)*100):.1f}%)
{vol_info}
{sentiment_info}

Key factors to consider:
- High volume (>1.5x average) confirms price moves — stronger signal
- Positive 5-day momentum + high volume = strong buy signal
- Bullish news sentiment increases confidence
- Bearish news sentiment reduces confidence
- Low volume moves are less reliable
- Stock trading above open price is bullish

Respond ONLY with JSON (no markdown), matching this exact shape:
{{"confidence": 65, "volatility": 40, "sync": 70, "side": "buy", "reason": "one sentence including volume and sentiment context"}}
Where confidence, volatility, and sync are numbers 0-100, and side is either "buy" or "sell".
Be very aggressive. confidence>30, volatility<90, sync>30 required."""

    res = requests.post("https://api.anthropic.com/v1/messages",
        headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
        json={"model": "claude-haiku-4-5-20251001", "max_tokens": 200, "messages": [{"role": "user", "content": prompt}]},
        timeout=20)
    if not res.ok:
        logger.error(f"AI check API error for {symbol}: HTTP {res.status_code} — {res.text[:300]}")
        raise ValueError(f"AI check failed: HTTP {res.status_code}")
    body = res.json()
    if 'content' not in body:
        logger.error(f"AI check returned no 'content' for {symbol}: {body}")
        raise ValueError("AI check returned no content")
    text = body['content'][0]['text'].replace('```json','').replace('```','').strip()
    result = json.loads(text)
    
    # Volume boost: if volume is 2x+ average and momentum is positive, boost confidence
    if vol_data and vol_data['volume_ratio'] >= 2.0 and vol_data['momentum_5d'] > 0:
        original_conf = result.get('confidence', 0)
        result['confidence'] = min(100, original_conf + 10)
        log_scan(f"📊 {symbol} — volume boost applied ({vol_data['volume_ratio']}x avg vol, conf: {original_conf}→{result['confidence']})")
    
    # Volume penalty: if volume is very low (<0.5x average), reduce confidence
    if vol_data and vol_data['volume_ratio'] < 0.5:
        original_conf = result.get('confidence', 0)
        result['confidence'] = max(0, original_conf - 10)
        log_scan(f"📊 {symbol} — low volume penalty ({vol_data['volume_ratio']}x avg vol, conf: {original_conf}→{result['confidence']})")
    
    # Sentiment boost: bullish news with high score boosts confidence
    if sentiment and sentiment.get('score', 0) >= 50:
        original_conf = result.get('confidence', 0)
        result['confidence'] = min(100, original_conf + 10)
        log_scan(f"📰 {symbol} — bullish sentiment boost (score:{sentiment.get('score')}, conf: {original_conf}→{result['confidence']})")
    
    # Sentiment penalty: bearish news reduces confidence
    elif sentiment and sentiment.get('score', 0) <= -50:
        original_conf = result.get('confidence', 0)
        result['confidence'] = max(0, original_conf - 10)
        log_scan(f"📰 {symbol} — bearish sentiment penalty (score:{sentiment.get('score')}, conf: {original_conf}→{result['confidence']})")
    
    return result

def auto_scan():
    reset_if_needed()
    if not is_market_hours():
        log_scan("⏰ Outside trading hours — scan skipped"); return

    # ALWAYS check and sell positions first — even if daily loss limit hit
    check_and_sell_positions()

    real_pl = get_real_today_pl()
    engine_state['today_pl'] = real_pl
    if real_pl <= -RULES['maxDailyLoss']:
        log_scan(f"🔴 Daily loss limit hit (P&L: ${real_pl:.2f}) — no new buys"); return

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

            # Check current position size — max 5 shares per stock
            try:
                pos_res = requests.get(f"{ALPACA_BASE_URL}/positions/{symbol}", headers=alpaca_hdrs(), timeout=10)
                current_qty = int(float(pos_res.json().get('qty', 0))) if pos_res.ok else 0
            except:
                current_qty = 0

            if current_qty >= RULES['maxSharesPerStock']:
                log_scan(f"⏭ {symbol} — max {RULES['maxSharesPerStock']} shares held, skipping")
                continue

            qty = 1  # Always buy 1 share at a time
            log_scan(f"✅ {symbol} — {side.upper()} signal. Placing... (holding {current_qty}/{RULES['maxSharesPerStock']})")

            or_ = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
                json={"symbol": symbol, "qty": str(qty), "side": side, "type": "market", "time_in_force": "day"}, timeout=10)
            if not or_.ok:
                log_scan(f"❌ {symbol} — order failed"); continue

            engine_state['weekly_trades'].append({'symbol': symbol, 'side': side, 'qty': qty, 'price': price})
            entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · AUTO: {side.upper()} {qty} {symbol} @ ${price:.2f} · {reason}"
            engine_state['trade_log'].insert(0, entry)
            engine_state['trade_log'] = engine_state['trade_log'][:50]
            save_state()
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
    """Fetch recent congressional trades from House Stock Watcher GitHub API"""
    try:
        # Try multiple free sources
        urls = [
            "https://house-stock-watcher-data.s3-us-east-2.amazonaws.com/data/all_transactions.json",
            "https://raw.githubusercontent.com/ratemycongress/congressional-stock-trades/main/data/trades.json",
        ]
        
        data = None
        for url in urls:
            try:
                res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                if res.ok:
                    data = res.json()
                    log_congress(f"Connected to: {url[:50]}...")
                    break
                else:
                    log_congress(f"URL returned {res.status_code}, trying next...")
            except:
                continue
        
        if not data:
            log_congress("All sources failed — using fallback stock list")
            # Fallback: use popular stocks that congress frequently buys
            return [
                {'ticker': 'NVDA', 'action': 'buy'},
                {'ticker': 'MSFT', 'action': 'buy'},
                {'ticker': 'AAPL', 'action': 'buy'},
                {'ticker': 'AMZN', 'action': 'buy'},
                {'ticker': 'GOOGL', 'action': 'buy'},
            ]

        trades = []
        seen = set()
        recent = data[:100] if isinstance(data, list) else []
        for item in recent:
            ticker = item.get('ticker', '').strip().upper()
            tx_type = str(item.get('type', '') or item.get('transaction_type', '')).lower()
            if not ticker or ticker in ('--', 'N/A', '') or len(ticker) > 5:
                continue
            if ticker in seen:
                continue
            seen.add(ticker)
            action = 'buy' if 'purchase' in tx_type or 'buy' in tx_type else 'sell'
            trades.append({'ticker': ticker, 'action': action})
        
        log_congress(f"Found {len(trades)} unique tickers")
        return trades[:20]
    except Exception as e:
        log_congress(f"Error: {str(e)[:50]}")
        return []

def congress_scan():
    today = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    if congress_state['last_scan'] == today:
        log_congress("Already scanned today — skipping")
        return

    if not is_market_hours():
        log_congress("⏰ Outside market hours — will copy when market opens")
        return

    log_congress("🏛️ Fetching congressional trades from Senate Stock Watcher...")
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

        # FIX: enforce the same max-shares-per-stock cap the Auto Engine uses.
        # Without this check, congress_scan() kept re-buying the fallback
        # tickers (NVDA/MSFT/AAPL/AMZN/GOOGL) day after day with no regard
        # for existing position size, since trade_key only blocks the same
        # ticker on the same calendar day — not across days.
        try:
            pos_res = requests.get(f"{ALPACA_BASE_URL}/positions/{ticker}", headers=alpaca_hdrs(), timeout=10)
            current_qty = int(float(pos_res.json().get('qty', 0))) if pos_res.ok else 0
        except:
            current_qty = 0

        if current_qty >= RULES['maxSharesPerStock']:
            log_congress(f"⏭ {ticker} — max {RULES['maxSharesPerStock']} shares held, skipping")
            continue

        try:
            qr = requests.get(f"{ALPACA_DATA_URL}/stocks/{ticker}/trades/latest", headers=alpaca_hdrs(), timeout=10)
            if not qr.ok:
                continue
            price = qr.json().get('trade', {}).get('p', 0)
            if not price or price < 1 or price > 1000:
                continue

            # Cap the buy quantity so we never cross maxSharesPerStock,
            # even when maxPositionSize / price would normally buy more.
            room_left = RULES['maxSharesPerStock'] - current_qty
            qty = max(1, min(room_left, int(RULES['maxPositionSize'] / price)))
            if qty <= 0:
                continue

            log_congress(f"📋 Copying congressional BUY: {ticker} @ ${price:.2f} (holding {current_qty}/{RULES['maxSharesPerStock']})")

            or_ = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
                json={"symbol": ticker, "qty": str(qty), "side": "buy", "type": "market", "time_in_force": "day"}, timeout=10)

            if or_.ok:
                congress_state['copied_trades'].append(trade_key)
                entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · CONGRESS COPY: BUY {qty} {ticker} @ ${price:.2f}"
                congress_state['trade_log'].insert(0, entry)
                congress_state['trade_log'] = congress_state['trade_log'][:50]
                # Save immediately after recording the buy — this is the exact
                # guard that failed on 2026-09-27: if the server restarts
                # before this is persisted, copied_trades reverts to its last
                # saved state and the same ticker can be bought again today.
                save_state()
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
    save_state()
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
@require_api_key
def start_engine():
    if not engine_state['running']:
        engine_state['running'] = True
        save_state()
        threading.Thread(target=engine_loop, daemon=True).start()
        log_scan("🚀 Auto engine started")
    return jsonify({"status": "running"})

@app.route("/api/engine/stop", methods=["POST"])
@require_api_key
def stop_engine():
    engine_state['running'] = False
    save_state()
    log_scan("⏹ Auto engine stopped")
    return jsonify({"status": "stopped"})

@app.route("/api/engine/kill", methods=["POST"])
@require_api_key
def kill_engine():
    engine_state['running'] = False
    congress_state['running'] = False
    save_state()
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
@require_api_key
def start_congress():
    if not congress_state['running']:
        congress_state['running'] = True
        save_state()
        threading.Thread(target=congress_loop, daemon=True).start()
        log_congress("🏛️ Congressional copy engine started")
    return jsonify({"status": "running"})

@app.route("/api/congress/stop", methods=["POST"])
@require_api_key
def stop_congress():
    congress_state['running'] = False
    save_state()
    log_congress("⏹ Congressional copy engine stopped")
    return jsonify({"status": "stopped"})

@app.route("/api/congress/scan", methods=["POST"])
@require_api_key
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
    # Only the POST side (placing an order) needs the key — GET (reading
    # order history) stays open like the other read-only routes.
    if request.method == "POST":
        if API_KEY:
            if request.headers.get("X-API-Key", "") != API_KEY:
                return jsonify({"error": "unauthorized"}), 401
        else:
            logger.warning("⚠️ API_KEY not set — POST /api/orders is UNPROTECTED")
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



@app.route("/api/settings/get")
def get_settings():
    return jsonify(RULES)

@app.route("/api/settings/update", methods=["POST"])
@require_api_key
def update_settings():
    data = request.get_json()
    allowed = ['maxDailyLoss','maxTrades','maxPositionSize','maxLossPerTrade',
               'takeProfitTarget','minConfidence','maxVolatility','minSyncScore',
               'maxSharesPerStock','takeProfitPct']
    updated = {}
    for key in allowed:
        if key in data:
            RULES[key] = float(data[key])
            updated[key] = RULES[key]
    log_scan(f"⚙️ Settings updated: {updated}")
    return jsonify({"status": "updated", "rules": RULES})


# Win Rate Tracker
@app.route("/api/trades/history")
def get_trade_history():
    """Get closed orders and calculate win rate stats"""
    try:
        res = requests.get(
            f"{ALPACA_BASE_URL}/orders?status=closed&limit=100&direction=desc",
            headers=alpaca_hdrs(), timeout=10
        )
        if not res.ok:
            return jsonify({"error": "Failed to fetch orders"}), 500
        
        orders = res.json()
        
        # Filter only filled sell orders
        sells = [o for o in orders if o.get('side') == 'sell' and o.get('status') == 'filled']
        buys = {o.get('symbol'): o for o in orders if o.get('side') == 'buy' and o.get('status') == 'filled'}
        
        trades = []
        wins = 0
        losses = 0
        total_gain = 0
        total_loss = 0
        
        for sell in sells:
            symbol = sell.get('symbol')
            sell_price = float(sell.get('filled_avg_price') or 0)
            qty = float(sell.get('filled_qty') or 0)
            
            # Find matching buy
            buy = buys.get(symbol)
            if not buy:
                continue
                
            buy_price = float(buy.get('filled_avg_price') or 0)
            if not buy_price or not sell_price:
                continue
                
            pl = (sell_price - buy_price) * qty
            pct = ((sell_price - buy_price) / buy_price) * 100
            
            trade = {
                'symbol': symbol,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'qty': qty,
                'pl': round(pl, 2),
                'pct': round(pct, 2),
                'win': pl > 0,
                'date': sell.get('filled_at', '')[:10] if sell.get('filled_at') else '',
            }
            trades.append(trade)
            
            if pl > 0:
                wins += 1
                total_gain += pl
            else:
                losses += 1
                total_loss += abs(pl)
        
        total_trades = wins + losses
        win_rate = round((wins / total_trades * 100), 1) if total_trades > 0 else 0
        avg_gain = round(total_gain / wins, 2) if wins > 0 else 0
        avg_loss = round(total_loss / losses, 2) if losses > 0 else 0
        reward_risk = round(avg_gain / avg_loss, 2) if avg_loss > 0 else 0
        
        return jsonify({
            'trades': trades[:50],
            'stats': {
                'total_trades': total_trades,
                'wins': wins,
                'losses': losses,
                'win_rate': win_rate,
                'avg_gain': avg_gain,
                'avg_loss': avg_loss,
                'reward_risk': reward_risk,
                'total_pl': round(total_gain - total_loss, 2),
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/news/<symbol>")
def get_news(symbol):
    """Get latest news for a stock symbol"""
    try:
        res = requests.get(
            f"https://data.alpaca.markets/v1beta1/news?symbols={symbol}&limit=5",
            headers=alpaca_hdrs(), timeout=10
        )
        if res.ok:
            return jsonify(res.json()), 200
        return jsonify({"news": []}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/news/market")
def get_market_news():
    """Get latest general market news"""
    try:
        res = requests.get(
            f"https://data.alpaca.markets/v1beta1/news?limit=10",
            headers=alpaca_hdrs(), timeout=10
        )
        if res.ok:
            return jsonify(res.json()), 200
        return jsonify({"news": []}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Benchmark state — persists in memory (resets on server restart)
benchmark_state = {
    'real_value': 0.0,
    'history': [],
}

@app.route("/api/benchmark/get")
def get_benchmark():
    return jsonify(benchmark_state)

@app.route("/api/benchmark/update", methods=["POST"])
def update_benchmark():
    data = request.get_json()
    val = float(data.get('real_value', 0))
    benchmark_state['real_value'] = val
    benchmark_state['history'].insert(0, {
        'date': datetime.now(pytz.timezone('America/New_York')).strftime('%m/%d/%Y'),
        'time': datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M:%S %p'),
        'real_value': val,
    })
    benchmark_state['history'] = benchmark_state['history'][:30]
    return jsonify({"status": "saved", "real_value": val})

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
