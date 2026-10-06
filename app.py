from flask import Flask, request, jsonify
from flask_cors import CORS
from functools import wraps
import os, requests, json, threading, time, logging, re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
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
# Options market data (quotes, greeks, implied volatility) lives under a
# different, older API version than stocks — v1beta1, not v2 — on the same
# data.alpaca.markets host. Confirmed field names: quotes use 'bp'/'ap' for
# bid/ask price, greeks come as {delta, gamma, rho, theta, vega}, and implied
# volatility is a separate top-level 'impliedVolatility' field.
ALPACA_OPTIONS_DATA_URL = "https://data.alpaca.markets/v1beta1"
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
    # Stop-loss as a % below the average entry price, not flat dollars: a $9
    # stop was ~3% on a $300 stock but ~60% on a $15 one, so it protected
    # expensive stocks and barely existed for cheap ones.
    'maxLossPct': 4, 'takeProfitTarget': 30,
    'minConfidence': 30, 'maxVolatility': 90, 'minSyncScore': 30, 'maxSharesPerStock': 5, 'takeProfitPct': 15,
    # Scale-out: tiered profit-taking instead of an all-or-nothing exit at
    # takeProfitPct. Tier 1 and 2 sell a FRACTION of the position's original
    # size (not current size — see engine_state['scale_state']) once each
    # gain threshold is crossed; tier 3 always sells whatever remains. A
    # stop-loss hit (maxLossPct) still exits the FULL position
    # immediately regardless of tiers — partial exits are for locking in
    # gains, not for softening a loss.
    'scaleOutTier1Pct': 5,  'scaleOutTier1Frac': 0.34,
    'scaleOutTier2Pct': 10, 'scaleOutTier2Frac': 0.33,
    'scaleOutTier3Pct': 15,  # sells 100% of whatever's left at this gain
}

engine_state = {
    'running': False, 'weekly_trades': [], 'today_pl': 0.0,
    'last_date': '', 'week_key': '', 'scan_log': [], 'trade_log': [],
    'last_weekly_email_week': '',
    # Active risk level — one of RISK_PROFILES' keys. Persisted so a restart
    # keeps it; RULES itself is rebuilt from this at boot (see below load_state()).
    'risk_level': 'balanced',
    # Per-stock overrides: {'AAPL': 'aggressive'}. A stock not listed here uses
    # the account-wide risk_level above.
    'symbol_risk': {},
    # Plain-language version of the scan_log, for customers rather than
    # debugging. scan_log stays exactly as-is (engineer-readable, e.g.
    # "confluence 3/4 agree [trend:✓, momentum:✗...]") since that detail has
    # been genuinely useful for real debugging. This is a parallel, friendlier
    # feed populated only at genuinely decision-relevant moments (a trade
    # placed, or a meaningful block) — not every intermediate skip reason.
    'customer_feed': [],
    # Per-symbol scale-out tracking: {'SYMBOL': {'origin_qty': int, 'tiers_hit': [bool,bool]}}
    # origin_qty is the largest size the position has reached — tier fractions
    # are computed against it, not the shrinking current qty, so "sell 1/3"
    # means 1/3 of the original position, not 1/3 of what's left after a
    # previous partial sell. Rebases (and un-hits tiers) if scaling in grows
    # the position past its previous origin_qty. Cleared entirely once a
    # position fully closes, so a fresh entry later starts clean.
    'scale_state': {},
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

# ---- Risk levels ----
# Each level overwrites a bundle of RULES together so they stay coherent
# (a tight stop with loose profit targets, or the reverse, is a classic
# way to lose money by accident). 'balanced' is EXACTLY the values the app
# has been running on (except the stop, now a 4% stop instead of a flat
# $9/share — see RULES), so choosing it changes almost nothing.
#
# 'confluence_offset' shifts how many signals must agree: -1 requires one
# fewer (more trades), +1 requires one more (fewer, higher-conviction
# trades). The result never drops below 2 signals in any level.
#
# These numbers are starting points, not backtested constants — same
# caveat as the confluence thresholds and scale-out tiers. Hard rails that
# NO level can turn off: the bear-market block on new buys, the daily loss
# limit (it scales, but always exists), no shorting, and the position cap.
RISK_PROFILES = {
    'conservative': {
        'label': 'Conservative',
        'description': 'Smaller positions, tighter stops, takes profit earlier, and needs more signals to agree before buying.',
        'rules': {
            'maxDailyLoss': 20, 'maxSharesPerStock': 3, 'maxLossPct': 2.5,
            'minConfidence': 50, 'maxVolatility': 70, 'minSyncScore': 50,
            'scaleOutTier1Pct': 3, 'scaleOutTier2Pct': 6, 'scaleOutTier3Pct': 9,
            'takeProfitPct': 9,
        },
        'confluence_offset': 1,
    },
    'balanced': {
        'label': 'Balanced',
        'description': 'The default. Moderate position sizes, stops, and profit targets.',
        'rules': {
            'maxDailyLoss': 30, 'maxSharesPerStock': 5, 'maxLossPct': 4,
            'minConfidence': 30, 'maxVolatility': 90, 'minSyncScore': 30,
            'scaleOutTier1Pct': 5, 'scaleOutTier2Pct': 10, 'scaleOutTier3Pct': 15,
            'takeProfitPct': 15,
        },
        'confluence_offset': 0,
    },
    'aggressive': {
        'label': 'Aggressive',
        'description': 'Larger positions, wider stops, lets winners run further, and acts on fewer confirming signals. More trades, bigger swings both ways.',
        'rules': {
            'maxDailyLoss': 60, 'maxSharesPerStock': 8, 'maxLossPct': 7,
            'minConfidence': 25, 'maxVolatility': 95, 'minSyncScore': 25,
            'scaleOutTier1Pct': 8, 'scaleOutTier2Pct': 16, 'scaleOutTier3Pct': 25,
            'takeProfitPct': 25,
        },
        'confluence_offset': -1,
    },
}

def apply_risk_profile(level):
    """Overwrite RULES with the chosen level's bundle. Returns True if the
    level exists. Deliberately does not log (it runs at import time, before
    log_scan exists) — callers that want a log line add their own."""
    prof = RISK_PROFILES.get(level)
    if not prof:
        return False
    RULES.update(prof['rules'])
    engine_state['risk_level'] = level
    return True

def level_for(symbol):
    """Risk level in effect for one stock: its own override if it has one,
    otherwise the account-wide level."""
    override = engine_state.get('symbol_risk', {}).get(str(symbol).upper())
    return override if override in RISK_PROFILES else engine_state.get('risk_level', 'balanced')

def has_override(symbol):
    return str(symbol).upper() in engine_state.get('symbol_risk', {})

def rules_for(symbol):
    """RULES as they apply to this one stock. Per-stock settings come from the
    stock's override level if it has one. maxDailyLoss is deliberately NOT
    overridden per stock — it's measured across the whole account — and
    neither are maxTrades / maxPositionSize, which aren't part of any level."""
    r = dict(RULES)
    if has_override(symbol):
        prof = RISK_PROFILES.get(level_for(symbol))
        if prof:
            r.update({k: v for k, v in prof['rules'].items() if k != 'maxDailyLoss'})
    return r

def confluence_offset_for(symbol=None):
    prof = RISK_PROFILES.get(level_for(symbol) if symbol else engine_state.get('risk_level', 'balanced'))
    return prof['confluence_offset'] if prof else 0

def level_tag(symbol):
    """' [Aggressive]' for a stock with its own override, else '' — appended to
    log lines so it's obvious why one stock is being treated differently."""
    return f" [{RISK_PROFILES[level_for(symbol)]['label']}]" if has_override(symbol) else ""

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
# RULES isn't persisted (it resets on every deploy), but the chosen level is —
# rebuild RULES from it so a restart or redeploy keeps the user's setting.
if not apply_risk_profile(engine_state.get('risk_level', 'balanced')):
    apply_risk_profile('balanced')

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

def log_customer(msg):
    """Plain-language feed entry — see engine_state['customer_feed'] comment."""
    est = datetime.now(pytz.timezone('America/New_York'))
    entry = f"{est.strftime('%I:%M %p')} — {msg}"
    engine_state['customer_feed'].insert(0, entry)
    engine_state['customer_feed'] = engine_state['customer_feed'][:50]

# ---- Plain-language templates for customer-facing explanations ----
# Each takes the same data the technical log already has and turns it into
# a sentence a non-technical user can actually read and trust, rather than
# engineer shorthand like "confluence 3/4 [trend:✓, momentum:✗...]".
def explain_confluence_pass(symbol, agreeing, total, action_label, regime_label):
    signal_word = "signal" if agreeing == 1 else "signals"
    if action_label == 'SCALING IN':
        return f"📈 {symbol} — Added to the position. {agreeing} of {total} {signal_word} confirmed again, so we're building up gradually instead of all at once."
    return f"🆕 {symbol} — New position opened. {agreeing} of {total} {signal_word} lined up ({regime_label} market)."

def explain_confluence_block(symbol, agreeing, total, required, regime_label):
    signal_word = "signal" if total == 1 else "signals"
    return f"⏸ {symbol} — Skipped for now. Only {agreeing} of {total} {signal_word} confirmed (needed {required}), and conditions are {regime_label.lower()}. Waiting for stronger agreement."

def explain_regime_blocked(symbol, regime_label):
    return f"🛑 {symbol} — Buy signal ignored. The overall market is trending down right now ({regime_label}), so we're not opening new positions until it recovers."

def explain_sell_signal(symbol, qty):
    return f"📉 {symbol} — Sold {qty} share(s). The AI's read on this one turned negative."

def explain_stop_loss(symbol, qty, pct_gain):
    return f"🛑 {symbol} — Closed the position ({qty} shares). It dropped past your loss limit ({pct_gain:.1f}%), so we exited to protect your capital."

def explain_scale_out_tier(symbol, sell_qty, pct_gain, tier_num):
    ordinal = {1: "first", 2: "second"}.get(tier_num, str(tier_num))
    return f"💰 {symbol} — Took some profit. Sold {sell_qty} share(s) after a +{pct_gain:.1f}% gain (the {ordinal} profit-taking step) — locking in gains while letting the rest of the position ride."

def explain_scale_out_final(symbol, qty, pct_gain):
    return f"✅ {symbol} — Closed out the rest of the position ({qty} shares) at +{pct_gain:.1f}% gain. Full profit locked in."

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

def _sell_shares(symbol, qty, current_price, reason, unrealized_pl_for_log):
    """Places the actual sell order and records it. Shared by the stop-loss,
    tiered scale-out, and final full-exit paths below so the order-placement
    and logging logic isn't duplicated three times."""
    log_scan(f"💰 {symbol} — {reason}. Selling {qty} share(s)...")
    sell = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
        json={"symbol": symbol, "qty": str(qty), "side": "sell", "type": "market", "time_in_force": "day"}, timeout=10)
    if sell.ok:
        log_scan(f"✅ SOLD {qty} {symbol} @ ${current_price:.2f} | {reason}")
        entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · AUTO SELL: {qty} {symbol} @ ${current_price:.2f} | {reason}"
        engine_state['trade_log'].insert(0, entry)
        engine_state['trade_log'] = engine_state['trade_log'][:50]
        engine_state['today_pl'] += unrealized_pl_for_log
        save_state()
        return True
    else:
        log_scan(f"❌ Failed to sell {symbol}")
        return False

def check_and_sell_positions():
    """Stop-loss (full exit, unchanged) + tiered scale-out on profit (NEW):
    instead of one all-or-nothing take-profit at takeProfitPct, sells a
    fraction of the position at each of two earlier gain thresholds, then
    exits whatever remains at the final threshold. See RULES's
    scaleOutTier*Pct/Frac comment for the exact thresholds and engine_state's
    'scale_state' comment for how origin_qty/tiers_hit tracking works."""
    try:
        res = requests.get(f"{ALPACA_BASE_URL}/positions", headers=alpaca_hdrs(), timeout=10)
        if not res.ok:
            return
        positions = res.json()
        if not positions:
            return

        held_symbols = set()
        for pos in positions:
            symbol = pos.get('symbol')
            qty = abs(int(float(pos.get('qty', 0))))
            current_price = float(pos.get('current_price', 0))

            if qty == 0:
                continue
            if float(pos.get('qty', 0)) < 0:
                # Short positions: this logic assumes a long and would SELL
                # more (doubling the short). Shorting is disabled; cover manually.
                log_scan(f"⚠️ {symbol} — short position ({pos.get('qty')}), skipping auto-sell; cover manually")
                continue
            held_symbols.add(symbol)

            r = rules_for(symbol)  # this stock's rules (its own level if it has an override)
            avg_entry = float(pos.get('avg_entry_price', 0))
            per_share_pl = current_price - avg_entry if avg_entry > 0 else 0
            pct_gain = ((current_price - avg_entry) / avg_entry * 100) if avg_entry > 0 else 0
            unrealized_pl = float(pos.get('unrealized_pl', 0))

            # --- Stop loss: unchanged, full immediate exit, clears scale tracking ---
            if avg_entry > 0 and pct_gain <= -r['maxLossPct']:
                reason = f"Stop loss: {pct_gain:.1f}% (limit -{r['maxLossPct']}%, ${per_share_pl:.2f}/share){level_tag(symbol)}"
                if _sell_shares(symbol, qty, current_price, reason, unrealized_pl):
                    engine_state['scale_state'].pop(symbol, None)
                    log_customer(explain_stop_loss(symbol, qty, pct_gain))
                    save_state()
                continue

            # --- Tiered scale-out on profit ---
            st = engine_state['scale_state'].setdefault(symbol, {'origin_qty': qty, 'tiers_hit': [False, False]})
            if qty > st['origin_qty']:
                # Position grew (scaled in further) past its previous high —
                # rebase tier fractions to the new size and re-arm both tiers.
                st['origin_qty'] = qty
                st['tiers_hit'] = [False, False]

            tier_defs = [
                (r['scaleOutTier1Pct'], r['scaleOutTier1Frac']),
                (r['scaleOutTier2Pct'], r['scaleOutTier2Frac']),
            ]
            for i, (tier_pct, tier_frac) in enumerate(tier_defs):
                if st['tiers_hit'][i] or pct_gain < tier_pct:
                    continue
                sell_qty = min(qty, max(1, round(st['origin_qty'] * tier_frac)))
                reason = f"Scale-out tier {i+1}: +{pct_gain:.1f}% (selling {int(tier_frac*100)}% of original {st['origin_qty']})"
                if _sell_shares(symbol, sell_qty, current_price, reason, unrealized_pl * (sell_qty / qty)):
                    st['tiers_hit'][i] = True
                    log_customer(explain_scale_out_tier(symbol, sell_qty, pct_gain, i + 1))
                    save_state()
                break  # one tier action per scan — re-evaluate remaining qty next cycle

            # --- Final tier: exit whatever remains ---
            # Re-fetch qty is not needed here since a same-cycle partial sell
            # above already `break`s before reaching this — final-tier check
            # runs on next cycle's fresh position data, seeing the reduced qty.
            if pct_gain >= r['scaleOutTier3Pct']:
                reason = f"Scale-out final: +{pct_gain:.1f}%/share, closing remaining {qty}"
                if _sell_shares(symbol, qty, current_price, reason, unrealized_pl):
                    engine_state['scale_state'].pop(symbol, None)
                    log_customer(explain_scale_out_final(symbol, qty, pct_gain))
                    save_state()

        # Clean up tracking for anything no longer held at all (fully closed
        # by a manual sell, a stop-loss above, or the final tier above).
        stale = [s for s in engine_state['scale_state'] if s not in held_symbols]
        if stale:
            for s in stale:
                engine_state['scale_state'].pop(s, None)
            save_state()
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
    
    return result, vol_data, sentiment

# ---- Confluence trading system: market regime + multi-signal agreement ----
# Previously auto_scan() traded a stock purely on ONE AI confidence score.
# This replaces that with a confluence approach: several independent
# signals must AGREE before a buy fires, and how many must agree scales
# with the overall market's current trend and volatility (the "regime") —
# calmer, trending-up markets need less confirmation; choppy or declining
# markets need more, and a confirmed downtrend blocks new buys outright.
# This does not change congress_scan(), which is unrelated and untouched.

_regime_cache = {'ts': 0.0, 'regime': None}

def _sma(closes, period):
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period

def _rsi(closes, period=14):
    """Standard 14-period RSI from a list of closes, oldest to newest."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)

def get_daily_closes(symbol, days=30):
    """Fetch `days` of daily closes, oldest to newest. Returns [] on failure —
    callers must treat that as 'indicator unavailable', not as zero/neutral.
    Explicitly requests the 'iex' feed: Alpaca's free-tier data plan defaults
    to requiring an explicit feed for some bar queries, and without it some
    requests silently come back empty (200 OK, zero bars) rather than erroring
    — which is exactly what was happening here: every confluence trend/
    momentum signal, and the whole market-regime check, were silently
    unavailable on 2026-10-02 because of this, leaving only sentiment+ai as
    the only two signals ever scored, which trivially passed 2/2 every time."""
    try:
        end = datetime.utcnow().isoformat() + 'Z'
        start = (datetime.utcnow() - timedelta(days=days + 5)).isoformat() + 'Z'  # pad for weekends/holidays
        res = requests.get(
            f"{ALPACA_DATA_URL}/stocks/{symbol}/bars",
            params={'timeframe': '1Day', 'start': start, 'end': end, 'limit': days + 10, 'feed': 'iex'},
            headers=alpaca_hdrs(), timeout=10
        )
        if not res.ok:
            logger.error(f"get_daily_closes HTTP error for {symbol}: {res.status_code} — {res.text[:300]}")
            return []
        bars = res.json().get('bars', [])
        if not bars:
            logger.warning(f"get_daily_closes returned 0 bars for {symbol} (HTTP {res.status_code}, body: {res.text[:200]})")
        return [b['c'] for b in bars]
    except Exception as e:
        logger.error(f"get_daily_closes error for {symbol}: {e}")
        return []

def get_market_regime():
    """SPY's own trend + volatility, defining how strict the confluence bar
    is this scan cycle. Cached for 5 minutes (matches the scan interval) so
    it's computed once per cycle, not once per stock."""
    now = time.time()
    if _regime_cache['regime'] is not None and now - _regime_cache['ts'] < 300:
        return _regime_cache['regime']

    closes = get_daily_closes('SPY', days=30)
    if len(closes) < 21:
        regime = {'trend': 'unknown', 'volatility': 'unknown', 'label': 'Unknown (insufficient SPY data)'}
        _regime_cache['ts'] = now
        _regime_cache['regime'] = regime
        return regime

    sma20 = _sma(closes, 20)
    price = closes[-1]
    trend = 'bull' if price > sma20 else 'bear'

    # Daily % change volatility over the last ~14 days
    recent = closes[-15:]
    pct_changes = [abs((recent[i] - recent[i-1]) / recent[i-1]) for i in range(1, len(recent)) if recent[i-1]]
    avg_daily_move = (sum(pct_changes) / len(pct_changes) * 100) if pct_changes else 0
    if avg_daily_move < 0.7:
        volatility = 'low'
    elif avg_daily_move < 1.5:
        volatility = 'normal'
    else:
        volatility = 'high'

    label = f"{'Bull' if trend=='bull' else 'Bear'}, {volatility.capitalize()} Vol"
    regime = {
        'trend': trend, 'volatility': volatility, 'label': label,
        'spy_price': round(price, 2), 'spy_sma20': round(sma20, 2),
        'avg_daily_move_pct': round(avg_daily_move, 2),
    }
    _regime_cache['ts'] = now
    _regime_cache['regime'] = regime
    return regime

def confluence_threshold_for_regime(regime, symbol=None):
    """How many of the (up to 5) confluence signals must agree, and out of
    how many, given the current regime. Returns (required, out_of) or
    (None, None) if new buys are blocked entirely this regime.
    These specific thresholds are a starting point, not backtested constants
    — tune them once you've watched this run against real market days."""
    if regime['trend'] == 'bear':
        return None, None  # no new buys while SPY is below its own 20-day trend — applies at EVERY risk level
    if regime['trend'] == 'bull' and regime['volatility'] == 'low':
        required, out_of = 3, 5
    elif regime['trend'] == 'bull':  # normal or high vol
        required, out_of = 4, 5
    else:
        required, out_of = 4, 5  # unknown/neutral — be conservative
    # Risk level shifts the bar by +/-1 signal, but never below 2 (one lone
    # signal agreeing is not confluence) and never above the number of
    # signals that exist.
    required = max(2, min(out_of, required + confluence_offset_for(symbol)))
    return required, out_of

def evaluate_confluence(symbol, price, ai_result, vol_data, sentiment):
    """Scores up to 5 independent signals for one stock. Returns
    (signals_agreeing, signals_total, detail_list) — detail_list is for
    logging, so a blocked/allowed decision is always explainable."""
    closes = get_daily_closes(symbol, days=30)
    signals = []

    sma20 = _sma(closes, 20) if closes else None
    if sma20 is not None:
        signals.append(('trend', price > sma20))
    # else: trend signal omitted entirely (not counted for or against) —
    # an unavailable indicator should never silently count as bearish.

    rsi = _rsi(closes) if closes else None
    if rsi is not None:
        signals.append(('momentum', 40 <= rsi <= 70))

    if vol_data and vol_data.get('volume_ratio') is not None:
        signals.append(('volume', vol_data['volume_ratio'] > 1.2))

    if sentiment and sentiment.get('score') is not None:
        signals.append(('sentiment', sentiment['score'] >= 20))

    if ai_result:
        r = rules_for(symbol)
        ai_bullish = (
            ai_result.get('side') == 'buy'
            and ai_result.get('confidence', 0) >= r['minConfidence']
            and ai_result.get('volatility', 100) <= r['maxVolatility']
            and ai_result.get('sync', 0) >= r['minSyncScore']
        )
        signals.append(('ai', ai_bullish))

    agreeing = sum(1 for _, ok in signals if ok)
    return agreeing, len(signals), signals

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

    market_regime = get_market_regime()
    log_scan(f"📊 Market regime: {market_regime['label']}" + (
        f" (SPY ${market_regime['spy_price']} vs 20d SMA ${market_regime['spy_sma20']})"
        if market_regime.get('spy_price') else ""
    ))

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
                ai, vol_data, sentiment = quick_ai_check(symbol, price, price_change)
            except: continue

            conf, vol, sync = ai.get('confidence',0), ai.get('volatility',100), ai.get('sync',0)
            side, reason = ai.get('side','buy'), ai.get('reason','')

            r = rules_for(symbol)
            if conf < r['minConfidence'] or vol > r['maxVolatility'] or sync < r['minSyncScore']:
                log_scan(f"⚫ {symbol} — blocked (C:{conf} V:{vol} S:{sync})"); continue

            # Confluence gate: only applies to NEW BUYS. A sell signal is
            # risk-REDUCING (closing exposure you already hold), so it isn't
            # held to the same bar as opening a new position — it still has
            # to clear the basic AI-quality check above, plus the "did we
            # actually hold shares" check below.
            if side == 'buy':
                required, out_of = confluence_threshold_for_regime(market_regime, symbol)
                if required is None:
                    log_scan(f"⛔ {symbol} — buy signal ignored: market regime is {market_regime['label']}, new buys paused")
                    log_customer(explain_regime_blocked(symbol, market_regime['label']))
                    continue
                agreeing, total, detail = evaluate_confluence(symbol, price, ai, vol_data, sentiment)
                if total == 0:
                    log_scan(f"⏭ {symbol} — no confluence signals available, skipping"); continue
                # Threshold is defined against a target denominator (out_of),
                # scaled down if fewer signals were actually available this run
                # (e.g. trend omitted for a newly-listed stock with thin history).
                effective_required = min(required, total)
                if agreeing < effective_required:
                    detail_str = ', '.join(f"{name}:{'✓' if ok else '✗'}" for name, ok in detail)
                    log_scan(f"⚫ {symbol} — confluence {agreeing}/{total} (need {effective_required}) [{detail_str}]")
                    log_customer(explain_confluence_block(symbol, agreeing, total, effective_required, market_regime['label']))
                    continue
                detail_str = ', '.join(f"{name}:{'✓' if ok else '✗'}" for name, ok in detail)
                log_scan(f"✅ {symbol} — confluence {agreeing}/{total} agree [{detail_str}]")
                # Customer-facing entry logged below once we know new-vs-scaling-in,
                # so it's not duplicated here — see action_label a few lines down.

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
                if current_qty >= r['maxSharesPerStock']:
                    log_scan(f"⏭ {symbol} — max {r['maxSharesPerStock']} shares held/pending, skipping{level_tag(symbol)}")
                    continue

            qty = 1  # Always buy 1 share at a time
            # A buy on a symbol already holding shares IS scaling in — it's
            # only allowed to reach here because the SAME confluence bar that
            # gates a brand-new position agreed again on this later scan (see
            # the confluence gate above, which applies uniformly to every
            # buy regardless of current_qty). maxSharesPerStock is the
            # ultimate cap on how far scaling in can go — no separate
            # "max adds per day" counter, since that cap already bounds it.
            is_scale_in = side == 'buy' and current_qty > 0
            action_label = 'SCALING IN' if is_scale_in else ('NEW POSITION' if side == 'buy' else 'SELL')
            log_scan(f"✅ {symbol} — {side.upper()} signal ({action_label}). Placing... (holding {current_qty}/{r['maxSharesPerStock']}){level_tag(symbol)}")

            or_ = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(),
                json={"symbol": symbol, "qty": str(qty), "side": side, "type": "market", "time_in_force": "day"}, timeout=10)
            if not or_.ok:
                log_scan(f"❌ {symbol} — order failed"); continue

            engine_state['weekly_trades'].append({'symbol': symbol, 'side': side, 'qty': qty, 'price': price, 'source': 'auto'})
            entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · AUTO {action_label}: {side.upper()} {qty} {symbol} @ ${price:.2f} · {reason}"
            engine_state['trade_log'].insert(0, entry)
            engine_state['trade_log'] = engine_state['trade_log'][:50]
            if side == 'buy':
                log_customer(explain_confluence_pass(symbol, agreeing, total, action_label, market_regime['label']))
            else:
                log_customer(explain_sell_signal(symbol, qty))
            save_state()
            log_scan(f"🚀 ORDER PLACED ({action_label}): {side.upper()} {qty} {symbol} @ ${price:.2f}")
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

# ---- Congress data: poll every source in parallel, use the best one ----
# Sources race side by side (total wait = the slowest one, capped), but the
# winner is picked on DATA QUALITY, not speed: the scan runs once a day, so a
# fresher, fuller dataset matters far more than a faster response. Every
# source's result is logged and kept in congress_state['last_race'] so you can
# see which ones are alive and how they compare.
CONGRESS_SOURCES = [
    {'name': 'House Stock Watcher (S3)', 'kind': 'legacy',
     'url': "https://house-stock-watcher-data.s3-us-east-2.amazonaws.com/data/all_transactions.json"},
    {'name': 'GitHub trades.json', 'kind': 'legacy',
     'url': "https://raw.githubusercontent.com/ratemycongress/congressional-stock-trades/main/data/trades.json"},
    # House + Senate STOCK Act filings, newest transaction first, last 3 months.
    # Keyless use is capped (30 requests/day per IP — Render IPs are shared, so
    # set BARGO_API_KEY on Render for a reliable limit). Terms require a visible
    # credit linking to Bargo wherever the data is DISPLAYED to users.
    {'name': 'Bargo API (House+Senate)', 'kind': 'bargo',
     'url': "https://www.bargo.ai/free-apis/congress/v1/trades?limit=100"},
]

def _parse_date(s):
    """Best-effort 'YYYY-MM-DD' from ISO or MM/DD/YYYY; '' if unparseable."""
    if not s:
        return ''
    s = str(s).strip()[:10]
    for fmt in ('%Y-%m-%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return ''

def _action_from_type(tx):
    t = str(tx or '').lower()
    if 'exchange' in t:
        return None
    if 'purchase' in t or 'buy' in t:
        return 'buy'
    if 'sale' in t or 'sell' in t:
        return 'sell'
    return None  # unknown type: skip rather than guess a direction

def _normalize_congress_rows(rows):
    out = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get('ticker') or '').strip().upper()
        if not ticker or ticker in ('--', 'N/A') or len(ticker) > 5:
            continue
        action = _action_from_type(item.get('type') or item.get('transaction_type'))
        if not action:
            continue
        date = max(_parse_date(item.get('transaction_date')), _parse_date(item.get('disclosure_date')))
        out.append({'ticker': ticker, 'action': action, 'date': date})
    # Newest first. If no row has a date this is a no-op (stable sort), which
    # preserves the old "first 100 as delivered" behaviour for undated files.
    out.sort(key=lambda r: r['date'], reverse=True)
    return out[:100]

def _fetch_congress_source(src):
    started = time.time()
    result = {'name': src['name'], 'ok': False, 'rows': [], 'newest': '', 'error': '', 'ms': 0}
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        key = os.environ.get('BARGO_API_KEY')
        if src['kind'] == 'bargo' and key:
            headers['X-Api-Key'] = key
        res = requests.get(src['url'], headers=headers, timeout=15)
        result['ms'] = int((time.time() - started) * 1000)
        if not res.ok:
            result['error'] = f"HTTP {res.status_code}"
            if res.status_code == 429 and src['kind'] == 'bargo':
                result['error'] += " (daily limit reached — set BARGO_API_KEY on Render)"
            elif res.status_code == 401 and src['kind'] == 'bargo':
                result['error'] += " (BARGO_API_KEY rejected)"
            return result
        data = res.json()
        rows = data.get('trades') if isinstance(data, dict) else data
        if not isinstance(rows, list):
            result['error'] = 'unexpected response shape'
            return result
        norm = _normalize_congress_rows(rows)
        result['rows'] = norm
        result['newest'] = max((r['date'] for r in norm), default='')
        result['ok'] = bool(norm)
        if not norm:
            result['error'] = 'no usable trades in response'
    except Exception as e:
        result['ms'] = int((time.time() - started) * 1000)
        result['error'] = f"{type(e).__name__}: {str(e)[:60]}"
    return result

def race_congress_sources():
    ex = ThreadPoolExecutor(max_workers=len(CONGRESS_SOURCES))
    futures = [(s, ex.submit(_fetch_congress_source, s)) for s in CONGRESS_SOURCES]
    results = []
    for s, f in futures:
        try:
            results.append(f.result(timeout=25))
        except Exception as e:
            results.append({'name': s['name'], 'ok': False, 'rows': [], 'newest': '', 'ms': 25000,
                            'error': f"timed out ({type(e).__name__})"})
    ex.shutdown(wait=False)  # don't block on a straggler
    ok = [r for r in results if r['ok']]
    # Best = freshest data first, then breadth (distinct tickers).
    winner = max(ok, key=lambda r: (r['newest'], len({x['ticker'] for x in r['rows']}))) if ok else None
    return results, winner

def get_congress_trades():
    """Recent congressional trades from whichever source wins the race."""
    try:
        results, winner = race_congress_sources()
        for r in results:
            if r['ok']:
                log_congress(f"📡 {r['name']}: {len(r['rows'])} trades, newest {r['newest'] or 'n/a'}, {r['ms']}ms")
            else:
                log_congress(f"❌ {r['name']}: {r['error']} ({r['ms']}ms)")
        congress_state['last_race'] = {
            'time': datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %I:%M %p'),
            'winner': winner['name'] if winner else None,
            'sources': [{'name': r['name'], 'ok': r['ok'], 'trades': len(r['rows']),
                         'newest': r['newest'], 'ms': r['ms'], 'error': r['error']} for r in results],
        }
        if not winner:
            log_congress("All sources failed — using fallback stock list")
            # Fallback: use popular stocks that congress frequently buys
            return [
                {'ticker': 'NVDA', 'action': 'buy'},
                {'ticker': 'MSFT', 'action': 'buy'},
                {'ticker': 'AAPL', 'action': 'buy'},
                {'ticker': 'AMZN', 'action': 'buy'},
                {'ticker': 'GOOGL', 'action': 'buy'},
            ]
        log_congress(f"🏁 Using {winner['name']} (freshest data: {winner['newest'] or 'undated'})")
        trades, seen = [], set()
        for row in winner['rows']:
            if row['ticker'] in seen:
                continue
            seen.add(row['ticker'])
            trades.append({'ticker': row['ticker'], 'action': row['action']})
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

    log_congress("🏛️ Polling all congressional data sources in parallel...")
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

        cap = rules_for(ticker)['maxSharesPerStock']
        if current_qty >= cap:
            log_congress(f"⏭ {ticker} — max {cap} shares held/pending, skipping{level_tag(ticker)}")
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
            room_left = cap - current_qty
            qty = max(1, min(room_left, int(RULES['maxPositionSize'] / price)))
            if qty <= 0:
                continue

            log_congress(f"📋 Copying congressional BUY: {ticker} @ ${price:.2f} (holding {current_qty}/{cap})")

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
        "customer_feed": engine_state['customer_feed'][:30],
        "is_market_hours": is_market_hours(),
    })

@app.route("/api/congress/status")
def congress_status():
    return jsonify({
        "running": congress_state['running'],
        "last_scan": congress_state['last_scan'],
        "scan_log": congress_state['scan_log'][:20],
        "trade_log": congress_state['trade_log'][:20],
        "last_race": congress_state.get('last_race'),
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

@app.route("/api/options/quote/<contract_symbol>")
def get_option_live_quote(contract_symbol):
    """Live bid/ask/mid for ONE option contract — used by the Risk/Reward
    display to price a single order or a spread's net debit/credit before
    you submit. Same v1beta1 snapshots endpoint the Edge Scanner uses,
    just for one symbol instead of a whole scan."""
    try:
        res = requests.get(f"{ALPACA_OPTIONS_DATA_URL}/options/snapshots",
                            headers=alpaca_hdrs(),
                            params={'symbols': contract_symbol}, timeout=10)
        if not res.ok:
            return jsonify({"error": f"HTTP {res.status_code}"}), res.status_code
        snap = res.json().get('snapshots', {}).get(contract_symbol)
        if not snap:
            return jsonify({"error": "No quote available for this contract"}), 404
        quote = snap.get('latestQuote') or {}
        bid, ask = quote.get('bp'), quote.get('ap')
        try:
            bid = float(bid) if bid is not None else None
            ask = float(ask) if ask is not None else None
        except (TypeError, ValueError):
            bid = ask = None
        mid = round((bid + ask) / 2, 4) if bid is not None and ask is not None else None
        return jsonify({"symbol": contract_symbol, "bid": bid, "ask": ask, "mid": mid})
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

@app.route("/api/options/orders/multi-leg", methods=["POST"])
@require_api_key
def place_multi_leg_option_order():
    """Step 3: multi-leg spreads (e.g. buy one call, sell another — a
    vertical spread) submitted as ONE atomic order via order_class: 'mleg'.

    Unlike single-leg orders, Alpaca requires this on a distinct payload
    shape: a top-level qty (how many of the whole spread combo to trade,
    usually 1) plus a 'legs' array where each leg carries its OWN symbol,
    side, ratio_qty (how that leg scales relative to the top-level qty —
    almost always 1 for a simple 2-leg spread), and position_intent
    ('buy_to_open'/'sell_to_open' for a new spread). All legs fill together
    or the whole order is rejected — there's no partial-spread state.

    Expected request body:
      { "legs": [ {"symbol": "...", "side": "buy"}, {"symbol": "...", "side": "sell"} ],
        "qty": 1, "type": "market", "limit_price": optional }
    """
    try:
        data = request.get_json() or {}
        legs_in = data.get('legs', [])
        qty = data.get('qty', 1)
        order_type = data.get('type', 'market')
        limit_price = data.get('limit_price')

        if not isinstance(legs_in, list) or len(legs_in) < 2 or len(legs_in) > 4:
            return jsonify({"error": "legs must be a list of 2-4 contracts"}), 400
        try:
            qty = int(qty)
            if qty <= 0:
                raise ValueError()
        except (TypeError, ValueError):
            return jsonify({"error": "qty must be a positive integer"}), 400

        legs_payload = []
        for i, leg in enumerate(legs_in):
            symbol = str(leg.get('symbol', '')).strip()
            side = str(leg.get('side', '')).lower()
            ratio_qty = leg.get('ratio_qty', 1)
            if not symbol or len(symbol) < 15:
                return jsonify({"error": f"Leg {i+1}: missing or invalid OCC contract symbol"}), 400
            if side not in ('buy', 'sell'):
                return jsonify({"error": f"Leg {i+1}: side must be 'buy' or 'sell'"}), 400
            try:
                ratio_qty = int(ratio_qty)
                if ratio_qty <= 0:
                    raise ValueError()
            except (TypeError, ValueError):
                return jsonify({"error": f"Leg {i+1}: ratio_qty must be a positive integer"}), 400
            legs_payload.append({
                "symbol": symbol, "side": side, "ratio_qty": str(ratio_qty),
                "position_intent": "buy_to_open" if side == "buy" else "sell_to_open",
            })

        order_payload = {
            "order_class": "mleg", "qty": str(qty), "type": order_type,
            "time_in_force": "day", "legs": legs_payload,
        }
        if order_type == 'limit':
            if not limit_price:
                return jsonify({"error": "limit_price is required for limit orders"}), 400
            order_payload["limit_price"] = str(limit_price)

        res = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_hdrs(), json=order_payload, timeout=10)
        if res.ok:
            leg_desc = ' / '.join(f"{l['side'].upper()} {l['symbol']}" for l in legs_payload)
            entry = f"{datetime.now(pytz.timezone('America/New_York')).strftime('%I:%M %p')} · SPREAD ({qty}x): {leg_desc}"
            engine_state['trade_log'].insert(0, entry)
            engine_state['trade_log'] = engine_state['trade_log'][:50]
            save_state()
            log_scan(f"🎯🎯 MULTI-LEG ORDER PLACED: {leg_desc}")
        else:
            logger.error(f"Multi-leg order rejected: HTTP {res.status_code} — {res.text[:300]}")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---- Options: Edge Score scanner ----
# This is a DETERMINISTIC heuristic, not AI-driven and not investment advice.
# It scores near-the-money contracts on three things, each 0-100:
#   - liquidity_score: tighter bid/ask spread = more reliably tradable
#   - delta_score: rewards contracts near 0.45 |delta| — meaningful leverage
#     to the underlying's move without being so deep ITM/OTM it's basically
#     just the stock (high delta) or basically a lottery ticket (low delta)
#   - iv_score: rewards moderate implied volatility — very high IV means
#     you're paying a lot for the option's time value; very low IV means
#     little upside is priced in either way
# The 0.45 delta and 35% IV "sweet spots" below are reasonable starting
# points, not backtested constants — tune them once you've watched this
# against real market behavior for a while.
# NOTE: Alpaca's free/indicative options feed does not expose per-contract
# daily volume (it's null), so "unusual volume today" isn't something this
# can score on the free tier — spread + delta + IV are what's available.
def score_option_contract(bid, ask, delta, iv):
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None  # no usable quote — exclude rather than guess
    mid = (bid + ask) / 2
    spread_pct = (ask - bid) / mid if mid > 0 else 1.0
    liquidity_score = max(0, 100 - spread_pct * 100 * 5)

    if delta is None:
        delta_score = 40  # unknown — treat as below-average confidence, not zero
    else:
        delta_score = max(0, 100 - abs(abs(delta) - 0.45) * 200)

    if iv is None:
        iv_score = 50
    else:
        iv_score = max(0, 100 - abs(iv - 0.35) * 150)

    edge_score = round(liquidity_score * 0.4 + delta_score * 0.35 + iv_score * 0.25)
    return {
        'edge_score': edge_score, 'mid_price': round(mid, 2),
        'spread_pct': round(spread_pct * 100, 1),
        'delta': round(delta, 3) if delta is not None else None,
        'iv': round(iv * 100, 1) if iv is not None else None,
    }

def build_option_reason(scored, contract_type):
    parts = []
    if scored['spread_pct'] < 5:
        parts.append("tight spread")
    elif scored['spread_pct'] > 15:
        parts.append("wide spread — costly to enter/exit")
    if scored['delta'] is not None:
        d = abs(scored['delta'])
        if 0.35 <= d <= 0.55:
            parts.append(f"delta {scored['delta']} in a solid leverage range")
        elif d > 0.55:
            parts.append(f"delta {scored['delta']} — trades close to the stock itself")
        else:
            parts.append(f"delta {scored['delta']} — cheap but low odds of finishing in the money")
    if scored['iv'] is not None:
        if scored['iv'] > 60:
            parts.append(f"IV {scored['iv']}% is elevated — you're paying up for it")
        elif scored['iv'] < 20:
            parts.append(f"IV {scored['iv']}% is low — cheaper premium")
    return "; ".join(parts) if parts else "Limited data available for this contract."

@app.route("/api/options/edge-scan")
def options_edge_scan():
    """Scans near-the-money contracts across a list of underlyings and
    returns the top-scoring candidates by Edge Score. See score_option_contract
    for exactly what's being measured and its limitations."""
    try:
        symbols_param = request.args.get('symbols', '')
        symbols = [s.strip().upper() for s in symbols_param.split(',') if s.strip()] or MARKET_SCAN_LIST[:8]
        min_days = int(request.args.get('min_days', 7))
        max_days = int(request.args.get('max_days', 45))
        moneyness_pct = float(request.args.get('moneyness_pct', 15))
        top_n = int(request.args.get('top_n', 8))
        opt_type = request.args.get('type', '')  # '', 'call', or 'put'

        today = datetime.utcnow().date()
        exp_gte = (today + timedelta(days=min_days)).isoformat()
        exp_lte = (today + timedelta(days=max_days)).isoformat()

        all_candidates = []
        skipped = []

        for symbol in symbols:
            try:
                pr = requests.get(f"{ALPACA_DATA_URL}/stocks/{symbol}/trades/latest", headers=alpaca_hdrs(), timeout=10)
                price = pr.json().get('trade', {}).get('p', 0) if pr.ok else 0
                if not price:
                    skipped.append(f"{symbol}: no live price"); continue

                strike_low = round(price * (1 - moneyness_pct / 100), 2)
                strike_high = round(price * (1 + moneyness_pct / 100), 2)
                params = {
                    'underlying_symbols': symbol, 'expiration_date_gte': exp_gte,
                    'expiration_date_lte': exp_lte, 'strike_price_gte': strike_low,
                    'strike_price_lte': strike_high, 'limit': 90, 'status': 'active',
                }
                if opt_type:
                    params['type'] = opt_type
                cr = requests.get(f"{ALPACA_BASE_URL}/options/contracts", headers=alpaca_hdrs(), params=params, timeout=15)
                if not cr.ok:
                    skipped.append(f"{symbol}: contracts lookup failed"); continue
                contracts = cr.json().get('option_contracts', [])
                if not contracts:
                    skipped.append(f"{symbol}: no contracts in range"); continue

                contract_info = {c['symbol']: c for c in contracts}
                contract_symbols = list(contract_info.keys())[:90]

                sr = requests.get(f"{ALPACA_OPTIONS_DATA_URL}/options/snapshots",
                                   headers=alpaca_hdrs(),
                                   params={'symbols': ','.join(contract_symbols), 'limit': len(contract_symbols)},
                                   timeout=15)
                if not sr.ok:
                    skipped.append(f"{symbol}: snapshot lookup failed ({sr.status_code})"); continue
                snapshots = sr.json().get('snapshots', {})

                for csym, snap in snapshots.items():
                    info = contract_info.get(csym)
                    if not info:
                        continue
                    quote = snap.get('latestQuote') or {}
                    greeks = snap.get('greeks') or {}
                    bid, ask = quote.get('bp'), quote.get('ap')
                    delta = greeks.get('delta')
                    iv = snap.get('impliedVolatility')
                    try:
                        bid = float(bid) if bid is not None else None
                        ask = float(ask) if ask is not None else None
                        delta = float(delta) if delta is not None else None
                        iv = float(iv) if iv is not None else None
                    except (TypeError, ValueError):
                        continue

                    scored = score_option_contract(bid, ask, delta, iv)
                    if scored is None:
                        continue
                    all_candidates.append({
                        'underlying': symbol, 'contract_symbol': csym,
                        'type': info.get('type'), 'strike_price': info.get('strike_price'),
                        'expiration_date': info.get('expiration_date'),
                        'underlying_price': price,
                        **scored,
                        'reason': build_option_reason(scored, info.get('type')),
                    })
            except Exception as e:
                skipped.append(f"{symbol}: {str(e)[:60]}")
                continue

        all_candidates.sort(key=lambda c: c['edge_score'], reverse=True)
        return jsonify({
            'candidates': all_candidates[:top_n],
            'scanned_symbols': symbols,
            'skipped': skipped,
            'total_candidates_found': len(all_candidates),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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



@app.route("/api/risk/get")
def get_risk_level():
    """Current level plus every level's description and the exact rule values
    it sets, so a UI can show what choosing each one actually changes."""
    return jsonify({
        "level": engine_state.get('risk_level', 'balanced'),
        "symbol_overrides": dict(engine_state.get('symbol_risk', {})),
        "scan_list": MARKET_SCAN_LIST,
        "profiles": {
            k: {"label": v['label'], "description": v['description'],
                "rules": v['rules'], "confluence_offset": v['confluence_offset']}
            for k, v in RISK_PROFILES.items()
        },
    })

@app.route("/api/risk/set", methods=["POST"])
@require_api_key
def set_risk_level():
    data = request.get_json() or {}
    level = str(data.get('level', '')).lower()
    if level not in RISK_PROFILES:
        return jsonify({"error": f"level must be one of: {', '.join(RISK_PROFILES)}"}), 400
    previous = engine_state.get('risk_level', 'balanced')
    apply_risk_profile(level)
    save_state()
    log_scan(f"🎚️ Risk level changed: {previous} → {level} "
             f"(max {RULES['maxSharesPerStock']} shares/stock, stop {RULES['maxLossPct']}%, daily loss limit ${RULES['maxDailyLoss']})")
    log_customer(f"🎚️ Risk level set to {RISK_PROFILES[level]['label']}. {RISK_PROFILES[level]['description']}")
    return jsonify({"level": level, "rules": {k: RULES[k] for k in RISK_PROFILES[level]['rules']}})

@app.route("/api/risk/symbol", methods=["POST"])
@require_api_key
def set_symbol_risk_level():
    """Give one stock its own risk level, or clear it with level='default'.
    Takes effect immediately for that stock's open position and future buys.
    Everything account-wide (daily loss limit, bear-market buy block, no
    shorting, weekly trade cap) is unaffected."""
    data = request.get_json() or {}
    symbol = str(data.get('symbol', '')).strip().upper()
    level = str(data.get('level', '')).strip().lower()
    if not re.fullmatch(r'[A-Z]{1,6}(\.[A-Z])?', symbol):
        return jsonify({"error": "symbol must be a ticker like AAPL"}), 400
    overrides = engine_state.setdefault('symbol_risk', {})
    if level in ('default', 'account', ''):
        removed = overrides.pop(symbol, None)
        save_state()
        if removed:
            log_scan(f"🎚️ {symbol} risk override removed (was {removed}) — now follows the account level")
            log_customer(f"🎚️ {symbol} now follows your account-wide risk level again.")
        return jsonify({"symbol": symbol, "level": engine_state.get('risk_level', 'balanced'), "override": None})
    if level not in RISK_PROFILES:
        return jsonify({"error": f"level must be one of: {', '.join(RISK_PROFILES)}, or 'default'"}), 400
    if symbol not in overrides and len(overrides) >= 25:
        return jsonify({"error": "max 25 per-stock overrides"}), 400
    previous = overrides.get(symbol)
    overrides[symbol] = level
    save_state()
    prof = RISK_PROFILES[level]
    log_scan(f"🎚️ {symbol} risk override: {previous or 'account level'} → {level} "
             f"(stop {prof['rules']['maxLossPct']}%, max {prof['rules']['maxSharesPerStock']} shares)")
    log_customer(f"🎚️ {symbol} set to {prof['label']}. {prof['description']}")
    return jsonify({"symbol": symbol, "level": level, "override": level})

@app.route("/api/settings/get")
def get_settings():
    return jsonify(RULES)

@app.route("/api/settings/update", methods=["POST"])
@require_api_key
def update_settings():
    data = request.get_json()
    allowed = ['maxDailyLoss','maxTrades','maxPositionSize','maxLossPct',
               'takeProfitTarget','minConfidence','maxVolatility','minSyncScore',
               'maxSharesPerStock','takeProfitPct',
               'scaleOutTier1Pct','scaleOutTier1Frac','scaleOutTier2Pct','scaleOutTier2Frac','scaleOutTier3Pct']
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
