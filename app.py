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
    'last_weekly_email_week': '',
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

_clock_cache = {'ts': 0.0, 'is_open': None}

def is_market_hours():
    """True only when the market is actually open.

    Previously this only compared the clock to 9:30-16:00 and never checked the
    day of the week, so the engines "traded" all weekend: orders queued while the
    market was closed, then all filled at Monday's open at once. Now it asks
    Alpaca's market clock (handles weekends AND holidays), cached for 60s, and
    falls back to a weekday-aware local check if the clock call fails."""
    now = time.time()
    if _clock_cache['is_open'] is not None and now - _clock_cache['ts'] < 60:
        return _clock_cache['is_open']
    try:
        r = requests.get(f"{ALPACA_BASE_URL}/clock", headers=alpaca_hdrs(), timeout=5)
        if r.ok:
            is_open = bool(r.json().get('is_open'))
            _clock_cache['ts'] = now
            _clock_cache['is_open'] = is_open
            return is_open
    except Exception as e:
        logger.warning(f"Alpaca clock lookup failed, using local fallback: {e}")
    est = datetime.now(pytz.timezone('America/New_York'))
    if est.weekday() >= 5:  # Saturday/Sunday
        return False
    h, m = est.hour, est.minute
    return (h > 9 or (h == 9 and m >= 30)) and h < 16

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

def get_exposure(symbol):
    """Returns (held_qty, pending_buy_qty, pending_sell_qty) for a symbol, or
    None if Alpaca can't be reached (callers must then SKIP the trade).

    The old cap check only looked at filled shares, so orders that were queued
    but not yet filled were invisible: every scan saw "holding 0/5" and bought
    again. held_qty is signed (negative = short)."""
    held = 0
    try:
        r = requests.get(f"{ALPACA_BASE_URL}/positions/{symbol}", headers=alpaca_hdrs(), timeout=10)
        if r.status_code == 404:
            held = 0
        elif r.ok:
            held = int(float(r.json().get('qty', 0)))
        else:
            return None
        o = requests.get(f"{ALPACA_BASE_URL}/orders?status=open&symbols={symbol}&limit=100", headers=alpaca_hdrs(), timeout=10)
        if not o.ok:
            return None
        pending_buy = pending_sell = 0
        for order in o.json():
            q = int(float(order.get('qty') or 0))
            if order.get('side') == 'buy':
                pending_buy += q
            elif order.get('side') == 'sell':
                pending_sell += q
        return held, pending_buy, pending_sell
    except Exception as e:
        logger.error(f"Exposure lookup failed for {symbol}: {e}")
        return None

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

def send_email(subject, body_text, template_params_override=None):
    """Generic email sender via EmailJS. Logs the real response on failure —
    previously this fired the request and never checked whether EmailJS
    actually accepted it, so a rejection (very common for server-side calls:
    many EmailJS accounts have 'Strict Origin Check' enabled, which blocks
    requests with no browser Origin header — exactly what a backend call is)
    failed completely silently."""
    try:
        params = {
            "to_email": ALERT_EMAIL,
            "subject": subject,
            "trade_reason": body_text,
        }
        if template_params_override:
            params.update(template_params_override)
        res = requests.post("https://api.emailjs.com/api/v1.0/email/send", json={
            "service_id": EMAILJS_SERVICE, "template_id": EMAILJS_TEMPLATE, "user_id": EMAILJS_PUBLIC,
            "template_params": params
        }, timeout=10)
        if not res.ok:
            logger.error(f"Email failed: HTTP {res.status_code} — {res.text[:300]}")
        else:
            logger.info("✉️ Email sent successfully")
    except Exception as e:
        logger.error(f"Email failed: {e}")

def build_weekly_summary_text():
    """Compose the Saturday weekly digest from this week's trades."""
    trades = engine_state.get('weekly_trades', [])
    if not trades:
        return "No trades were placed this week."
    lines = [f"Weekly trading summary — {len(trades)} trade(s) this week:\n"]
    by_source = {}
    for t in trades:
        src = t.get('source', 'unknown')
        by_source.setdefault(src, []).append(t)
    for src, items in by_source.items():
        lines.append(f"\n{src.upper()} ({len(items)} trade(s)):")
        for t in items:
            price = t.get('price', 0) or 0
            lines.append(f"  • BUY {t.get('qty')} {t.get('symbol')}" + (f" @ ${price:.2f}" if price else ""))
    real_pl = get_real_today_pl()
    lines.append(f"\nToday's P&L: ${real_pl:.2f}")
    return "\n".join(lines)

def check_and_send_weekly_email():
    """Fires once, on Saturday, summarizing the trading week — replaces the
    old per-trade emails for auto/congress trades, which were both spammy
    and silently failing (see send_email's docstring)."""
    est = datetime.now(pytz.timezone('America/New_York'))
    if est.weekday() != 5:  # Monday=0 ... Saturday=5
        return
    wk = get_week_key()
    if engine_state.get('last_weekly_email_week') == wk:
        return  # already sent this week
    summary = build_weekly_summary_text()
    send_email(f"📊 Precision Alpha: Weekly Trading Summary — {est.strftime('%Y-%m-%d')}", summary)
    engine_state['last_weekly_email_week'] = wk
    save_state()
    log_scan("✉️ Weekly summary email sent")

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
            if float(pos.get('qty', 0)) < 0:
                # Short positions: the stop-loss/take-profit math below assumes a long
                # and would SELL more (doubling the short). Shorting is disabled; cover manually.
                log_scan(f"⚠️ {symbol} — short position ({pos.get('qty')}), skipping auto-sell; cover manually")
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
                    # No per-trade email — see check_and_send_weekly_email(); this
                    # still shows up in the weekly summary via trade_log if desired.
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

    if RULES['maxTrades'] < 999 and len(engine_state['weekly_trades']) >= RULES['maxTrades']:
        log_scan(f"🔴 Weekly trade limit reached ({len(engine_state['weekly_trades'])}/{int(RULES['maxTrades'])}) — no new buys"); return

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

            # Position/exposure check. Counts pending (unfilled) orders, and never
            # opens shorts: this app only buys, and sells only shares it holds.
            exposure = get_exposure(symbol)
            if exposure is None:
                log_scan(f"⏭ {symbol} — couldn't verify position/open orders, skipping"); continue
            held, pending_buy, pending_sell = exposure

            if side == 'sell':
                sellable = held - pending_sell
                if sellable <= 0:
                    log_scan(f"⏭ {symbol} — SELL signal but no long shares to sell (shorting disabled), skipping")
                    continue
                current_qty = held
            else:
                current_qty = max(held, 0) + pending_buy  # effective exposure incl. queued orders
                if held < 0:
                    log_scan(f"⏭ {symbol} — short position open ({held}), skipping buys until it's covered")
                    continue
                if current_qty >= RULES['maxSharesPerStock']:
                    log_scan(f"⏭ {symbol} — max {RULES['maxSharesPerStock']} shares held/pending, skipping")
                    continue

            qty = 1  # Always buy 1 share at a time
            log_scan(f"✅ {symbol} — {side.upper()} signal. Placing... (holding {current_qty}/{RULES['maxSharesPerStock']})")

            or_ = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
                json={"symbol": symbol, "qty": str(qty), "side": side, "type": "market", "time_in_force": "day"}, timeout=10)
            if not or_.ok:
                log_scan(f"❌ {symbol} — order failed"); continue

            engine_state['weekly_trades'].append({'symbol': symbol, 'side': side, 'qty': qty, 'price': price, 'source': 'auto'})
            entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · AUTO: {side.upper()} {qty} {symbol} @ ${price:.2f} · {reason}"
            engine_state['trade_log'].insert(0, entry)
            engine_state['trade_log'] = engine_state['trade_log'][:50]
            save_state()
            log_scan(f"🚀 ORDER PLACED: {side.upper()} {qty} {symbol} @ ${price:.2f}")
            # No per-trade email — see check_and_send_weekly_email().
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

def weekly_email_loop():
    """Runs independently of the trading engines' on/off state, so the
    Saturday summary still gets checked and sent even if you've stopped
    Auto/Congress engines that day. Checks every 30 minutes — cheap, and
    check_and_send_weekly_email() itself no-ops on any day but Saturday, and
    again after it's already sent once that week."""
    while True:
        try: check_and_send_weekly_email()
        except Exception as e: logger.error(f"Weekly email check error: {e}")
        time.sleep(1800)

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

    # Weekly trade limit applies across BOTH engines (and manual trades — see
    # /api/orders) so it can't be bypassed by switching which engine trades.
    if RULES['maxTrades'] < 999 and len(engine_state['weekly_trades']) >= RULES['maxTrades']:
        log_congress(f"🔴 Weekly trade limit reached ({len(engine_state['weekly_trades'])}/{int(RULES['maxTrades'])}) — no new buys")
        congress_state['last_scan'] = today
        save_state()
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

        # Also stop mid-scan if this batch of buys pushes past the weekly limit.
        if RULES['maxTrades'] < 999 and len(engine_state['weekly_trades']) >= RULES['maxTrades']:
            log_congress(f"🔴 Weekly trade limit reached mid-scan ({len(engine_state['weekly_trades'])}/{int(RULES['maxTrades'])}) — stopping")
            break

        # FIX: enforce the same max-shares-per-stock cap the Auto Engine uses.
        # Without this check, congress_scan() kept re-buying the fallback
        # tickers (NVDA/MSFT/AAPL/AMZN/GOOGL) day after day with no regard
        # for existing position size, since trade_key only blocks the same
        # ticker on the same calendar day — not across days.
        exposure = get_exposure(ticker)
        if exposure is None:
            log_congress(f"⏭ {ticker} — couldn't verify position/open orders, skipping")
            continue
        held, pending_buy, _pending_sell = exposure
        if held < 0:
            log_congress(f"⏭ {ticker} — short position open ({held}), skipping")
            continue
        current_qty = held + pending_buy  # effective exposure incl. queued orders

        if current_qty >= RULES['maxSharesPerStock']:
            log_congress(f"⏭ {ticker} — max {RULES['maxSharesPerStock']} shares held/pending, skipping")
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
                # Count congress buys toward the shared weekly trade limit —
                # engine_state['weekly_trades'] is the one counter both
                # engines (and manual trades, see /api/orders) all add to,
                # so the limit can't be bypassed by switching which engine trades.
                engine_state['weekly_trades'].append({'symbol': ticker, 'side': 'buy', 'qty': qty, 'price': price, 'source': 'congress'})
                entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · CONGRESS COPY: BUY {qty} {ticker} @ ${price:.2f}"
                congress_state['trade_log'].insert(0, entry)
                congress_state['trade_log'] = congress_state['trade_log'][:50]
                # Save immediately after recording the buy — this is the exact
                # guard that failed on 2026-09-27: if the server restarts
                # before this is persisted, copied_trades reverts to its last
                # saved state and the same ticker can be bought again today.
                save_state()
                log_congress(f"✅ ORDER PLACED: BUY {qty} {ticker} @ ${price:.2f}")
                # No per-trade email — see check_and_send_weekly_email().
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

# ---- Options: Step 1 (contract lookup only, no order placement yet) ----
# GET /v2/options/contracts is a TRADING API endpoint (same host as /v2/orders
# and /v2/positions), not the market-data host — hence ALPACA_BASE_URL here,
# same as everything else in this file, not ALPACA_DATA_URL.
@app.route("/api/options/contracts/<symbol>")
def get_option_contracts(symbol):
    """Browse available option contracts for an underlying symbol.
    Query params (all optional, passed straight through to Alpaca):
      expiration_date (YYYY-MM-DD), expiration_date_gte, expiration_date_lte,
      type (call/put), strike_price_gte, strike_price_lte, limit, page_token
    Alpaca's default (no expiration filter) only returns contracts expiring
    by the upcoming weekend, so the frontend should let the person pick a
    date range rather than relying on that default for anything useful.
    """
    try:
        params = {"underlying_symbols": symbol.upper()}
        passthrough = ['expiration_date', 'expiration_date_gte', 'expiration_date_lte',
                        'type', 'strike_price_gte', 'strike_price_lte', 'limit', 'page_token', 'status']
        for key in passthrough:
            val = request.args.get(key)
            if val:
                params[key] = val
        res = requests.get(f"{ALPACA_BASE_URL}/options/contracts", headers=alpaca_hdrs(), params=params, timeout=15)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/options/contracts/lookup/<contract_symbol>")
def get_single_option_contract(contract_symbol):
    """Fetch one contract's full details by its OCC symbol (or Alpaca's
    internal contract id) — used once a specific strike/expiration is picked,
    to confirm it's tradable before building an order leg from it."""
    try:
        res = requests.get(f"{ALPACA_BASE_URL}/options/contracts/{contract_symbol}", headers=alpaca_hdrs(), timeout=10)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/options/orders", methods=["POST"])
@require_api_key
def place_option_order():
    """Step 2: single-leg option orders only (buy/sell one call or put).
    Multi-leg (order_class: mleg) spreads are Step 3 — not built yet.

    A single-leg option order uses the SAME /v2/orders endpoint as stock
    orders — Alpaca tells equity and option orders apart by the symbol
    format (a ticker like 'AAPL' vs a 21-char OCC symbol like
    'AAPL260928C00250000'), so no order_class or position_intent is needed
    here, same as a plain stock buy/sell.
    """
    try:
        data = request.get_json() or {}
        contract_symbol = data.get('symbol', '').strip()
        side = str(data.get('side', '')).lower()
        qty = data.get('qty')
        order_type = data.get('type', 'market')
        limit_price = data.get('limit_price')

        if not contract_symbol or len(contract_symbol) < 15:
            return jsonify({"error": "Missing or invalid option contract symbol (expected OCC format)"}), 400
        if side not in ('buy', 'sell'):
            return jsonify({"error": "side must be 'buy' or 'sell'"}), 400
        try:
            qty = int(qty)
            if qty <= 0:
                raise ValueError()
        except (TypeError, ValueError):
            return jsonify({"error": "qty must be a positive integer (number of contracts)"}), 400

        order_payload = {
            "symbol": contract_symbol, "qty": str(qty), "side": side,
            "type": order_type, "time_in_force": "day"
        }
        if order_type == 'limit':
            if not limit_price:
                return jsonify({"error": "limit_price is required for limit orders"}), 400
            order_payload["limit_price"] = str(limit_price)

        res = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(), json=order_payload, timeout=10)
        if res.ok:
            entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · OPTION {side.upper()} {qty}x {contract_symbol}"
            engine_state['trade_log'].insert(0, entry)
            engine_state['trade_log'] = engine_state['trade_log'][:50]
            save_state()
            log_scan(f"🎯 OPTION ORDER PLACED: {side.upper()} {qty}x {contract_symbol}")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
            order_data = request.get_json() or {}
            side = str(order_data.get('side', '')).lower()
            # Weekly trade limit applies to manual buys too — otherwise it's
            # trivially bypassed by just placing trades from the Trade page
            # instead of letting an engine do it. Sells aren't gated: closing
            # a position shouldn't be blocked by a limit meant to cap new exposure.
            reset_if_needed()
            if side == 'buy' and RULES['maxTrades'] < 999 and len(engine_state['weekly_trades']) >= RULES['maxTrades']:
                return jsonify({"message": f"Weekly trade limit reached ({len(engine_state['weekly_trades'])}/{int(RULES['maxTrades'])})"}), 429
            res = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(), json=order_data, timeout=10)
            if res.ok and side == 'buy':
                engine_state['weekly_trades'].append({
                    'symbol': order_data.get('symbol', ''), 'side': 'buy',
                    'qty': order_data.get('qty', ''), 'price': 0, 'source': 'manual'
                })
                save_state()
        else:
            res = requests.get(f"{ALPACA_BASE_URL}/orders?status={request.args.get('status','all')}&limit={request.args.get('limit','50')}", headers=alpaca_hdrs(), timeout=10)
        return jsonify(res.json()), res.status_code
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ai/analyze", methods=["POST"])
@require_api_key
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
@require_api_key
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

# Runs independently of engine on/off state — always checking for Saturday.
threading.Thread(target=weekly_email_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
