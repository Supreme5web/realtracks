import os
import json
import time
import asyncio
import threading

import requests
import websockets

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
WSOL_MINT = "So11111111111111111111111111111111111111112"

# ---------------------------------------------------------------------------
# Config (env vars)
# ---------------------------------------------------------------------------
MIN_MARKET_CAP_USD = float(os.environ.get("MIN_MARKET_CAP_USD", "11000"))
MIN_VOLUME_USD = float(os.environ.get("MIN_VOLUME_USD", "1000"))
# How long we'll keep watching a token's trades before giving up on it if it
# never qualifies. Keeps memory bounded and stops paying for metered trade
# events on dead tokens.
MAX_TRACK_AGE_SECONDS = float(os.environ.get("MAX_TRACK_AGE_SECONDS", "1800"))  # 30 min
# How long to wait for the metadata URI fetch before giving up on a token.
METADATA_FETCH_TIMEOUT_SECONDS = 8

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
# mint -> {name, symbol, created_ts, v_sol, cumulative_sol_volume, alerted}
tracked_tokens = {}
tracked_lock = threading.Lock()

watcher_status = {
    "connected": False,
    "tokens_seen": 0,
    "tokens_tracking": 0,
    "alerts_sent": 0,
    "last_error": None,
}

_sol_price_cache = {"price": None, "ts": 0}
_sol_price_lock = threading.Lock()
SOL_PRICE_CACHE_TTL_SECONDS = 60


def get_sol_price_usd():
    """
    SOL/USD via Jupiter's free public price API - no API key, no dependency
    on Codex/QuickNode/Helius, so this feature can't burn the quotas you've
    already exhausted elsewhere. Cached briefly since we don't need
    sub-minute precision for market cap thresholds.
    """
    now = time.time()
    with _sol_price_lock:
        if _sol_price_cache["price"] is not None and now - _sol_price_cache["ts"] < SOL_PRICE_CACHE_TTL_SECONDS:
            return _sol_price_cache["price"]
    try:
        resp = requests.get(
            "https://api.jup.ag/price/v2",
            params={"ids": WSOL_MINT},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        price = float(data["data"][WSOL_MINT]["price"])
        with _sol_price_lock:
            _sol_price_cache["price"] = price
            _sol_price_cache["ts"] = now
        return price
    except Exception as e:
        print(f"[PumpAlert] SOL price lookup failed: {e}", flush=True)
        with _sol_price_lock:
            return _sol_price_cache["price"]  # stale-but-usable, or None


def fetch_twitter_link(uri):
    """
    Fetch the token's off-chain metadata JSON (the `uri` from the create
    event) and return its twitter link, or None. Never raises - a failed or
    missing fetch is treated as "no twitter", since Twitter is a compulsory
    filter and we'd rather under-alert than alert on an unverified token.
    """
    if not uri:
        return None
    try:
        resp = requests.get(uri, timeout=METADATA_FETCH_TIMEOUT_SECONDS)
        if not resp.ok:
            return None
        data = resp.json()
        # pump.fun metadata JSON commonly uses one of these keys depending on
        # which uploader/creator flow was used.
        for key in ("twitter", "twitter_url", "x", "x_url"):
            val = data.get(key)
            if val:
                return val
        return None
    except Exception:
        return None


def send_telegram_alert(name, symbol, mint, market_cap_usd):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_ids = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()]
    if not token or not chat_ids:
        print("[PumpAlert] Can't send alert - TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set", flush=True)
        return

    text = (
        f"\U0001F680 *{name}* (${symbol})\n"
        f"*MC:* ${market_cap_usd:,.0f}\n"
        f"*CA:* `{mint}`"
    )
    for chat_id in chat_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception as e:
            print(f"[PumpAlert] Telegram send failed for {chat_id}: {e}", flush=True)


# ---------------------------------------------------------------------------
# WebSocket handling
# ---------------------------------------------------------------------------

async def _check_socials_and_maybe_track(ws, msg):
    """
    Runs for every new pump.fun mint. Fetches metadata off the free
    subscribeNewToken event first - only if it has a Twitter link do we pay
    to subscribe to its (metered) trade stream. This is the main cost
    control: most pump.fun launches have no Twitter and get dropped for
    free, before we ever spend a metered event on them.
    """
    mint = msg.get("mint")
    if not mint:
        return

    with tracked_lock:
        watcher_status["tokens_seen"] += 1

    name = msg.get("name") or "Unknown"
    symbol = msg.get("symbol") or "N/A"
    uri = msg.get("uri")
    v_sol = msg.get("vSolInBondingCurve") or 0.0

    loop = asyncio.get_event_loop()
    twitter = await loop.run_in_executor(None, fetch_twitter_link, uri)
    if not twitter:
        return  # dropped for free, never subscribed to trades

    with tracked_lock:
        tracked_tokens[mint] = {
            "name": name,
            "symbol": symbol,
            "created_ts": time.time(),
            "v_sol": v_sol,
            "cumulative_sol_volume": 0.0,
            "alerted": False,
        }
        watcher_status["tokens_tracking"] = len(tracked_tokens)

    await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
    print(f"[PumpAlert] Tracking {symbol} ({mint}) - has Twitter", flush=True)


async def _handle_trade(ws, msg):
    mint = msg.get("mint")
    with tracked_lock:
        entry = tracked_tokens.get(mint)
        if not entry or entry["alerted"]:
            return
        new_v_sol = msg.get("vSolInBondingCurve")
        market_cap_sol = msg.get("marketCapSol")
        if new_v_sol is None or market_cap_sol is None:
            return
        # Trade size = change in the bonding curve's virtual SOL reserve
        # since the last trade we saw for this mint.
        trade_sol_amount = abs(new_v_sol - entry["v_sol"])
        entry["v_sol"] = new_v_sol
        entry["cumulative_sol_volume"] += trade_sol_amount
        cumulative_sol_volume = entry["cumulative_sol_volume"]
        name, symbol = entry["name"], entry["symbol"]

    sol_price = get_sol_price_usd()
    if not sol_price:
        return  # can't evaluate USD thresholds without a price

    market_cap_usd = market_cap_sol * sol_price
    volume_usd = cumulative_sol_volume * sol_price

    if market_cap_usd >= MIN_MARKET_CAP_USD and volume_usd >= MIN_VOLUME_USD:
        with tracked_lock:
            entry = tracked_tokens.get(mint)
            if not entry or entry["alerted"]:
                return
            entry["alerted"] = True
            watcher_status["alerts_sent"] += 1

        print(f"[PumpAlert] {symbol} ({mint}) qualified - MC ${market_cap_usd:,.0f}, vol ${volume_usd:,.0f}", flush=True)
        send_telegram_alert(name, symbol, mint, market_cap_usd)
        await ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))


async def _cleanup_stale_tokens(ws):
    """Periodically drops/unsubscribes tokens that never qualified in time,
    so we stop paying for their trade events and don't leak memory."""
    while True:
        await asyncio.sleep(60)
        now = time.time()
        stale_mints = []
        with tracked_lock:
            for mint, entry in list(tracked_tokens.items()):
                if entry["alerted"] or now - entry["created_ts"] > MAX_TRACK_AGE_SECONDS:
                    stale_mints.append(mint)
                    del tracked_tokens[mint]
            watcher_status["tokens_tracking"] = len(tracked_tokens)
        for mint in stale_mints:
            try:
                await ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))
            except Exception:
                pass  # connection may already be closing; reconnect loop will resubscribe as needed


async def _handle_message(ws, raw_message):
    try:
        msg = json.loads(raw_message)
    except json.JSONDecodeError:
        return

    tx_type = msg.get("txType")
    if tx_type == "create":
        asyncio.create_task(_check_socials_and_maybe_track(ws, msg))
    elif tx_type in ("buy", "sell"):
        await _handle_trade(ws, msg)


async def _run_watcher_forever():
    api_key = os.environ.get("PUMPPORTAL_API_KEY", "")
    if not api_key:
        raise ValueError(
            "PUMPPORTAL_API_KEY env var is not set. Get one (and fund the linked "
            "wallet with at least 0.02 SOL) at https://pumpportal.fun/trading-api/setup "
            "- subscribeTokenTrade is metered at 0.01 SOL per 10,000 events."
        )

    uri = f"{PUMPPORTAL_WS_URL}?api-key={api_key}"
    backoff = 5

    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                watcher_status["connected"] = True
                watcher_status["last_error"] = None
                backoff = 5
                print("[PumpAlert] Connected to PumpPortal", flush=True)

                await ws.send(json.dumps({"method": "subscribeNewToken"}))

                # Re-subscribe to any tokens we were already tracking (e.g. after
                # a reconnect) so we don't silently stop watching them.
                with tracked_lock:
                    still_tracking = [m for m, e in tracked_tokens.items() if not e["alerted"]]
                if still_tracking:
                    await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": still_tracking}))

                cleanup_task = asyncio.create_task(_cleanup_stale_tokens(ws))
                try:
                    async for raw_message in ws:
                        await _handle_message(ws, raw_message)
                finally:
                    cleanup_task.cancel()

        except Exception as e:
            watcher_status["connected"] = False
            watcher_status["last_error"] = str(e)
            print(f"[PumpAlert] Connection error: {e} - reconnecting in {backoff}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def start_watcher_background():
    """Entry point for app.py - runs the asyncio watcher forever in this thread."""
    try:
        asyncio.run(_run_watcher_forever())
    except Exception as e:
        watcher_status["connected"] = False
        watcher_status["last_error"] = str(e)
        print(f"[PumpAlert] Watcher crashed: {e}", flush=True)
