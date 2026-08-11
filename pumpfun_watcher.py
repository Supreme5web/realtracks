import os
import json
import time
import asyncio
import threading
from datetime import datetime, timezone

import requests
import websockets

# ---------------------------------------------------------------------------
# Config (env vars)
# ---------------------------------------------------------------------------
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Prefer Helius if a key is provided - much higher rate limits and a far
# more stable websocket than the public RPC. Falls back to public endpoints
# (or explicit SOLANA_WS_URL/SOLANA_RPC_URL overrides) if no key is set.
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
if HELIUS_API_KEY:
    _default_ws = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    _default_rpc = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
else:
    _default_ws = "wss://api.mainnet-beta.solana.com"
    _default_rpc = "https://api.mainnet-beta.solana.com"

SOLANA_WS_URL = os.environ.get("SOLANA_WS_URL", _default_ws)
SOLANA_RPC_URL = os.environ.get("SOLANA_RPC_URL", _default_rpc)

# Used only for enriching the outgoing alert (buys/sells, volume, age,
# price, socials) - never touches detection/tracking. Sign up at
# solanatracker.io for a key. If unset, alerts just fall back to the
# original name/CA/MC-only format.
SOLANA_TRACKER_API_KEY = os.environ.get("SOLANA_TRACKER_API_KEY", "")
SOLANA_TRACKER_BASE_URL = "https://data.solanatracker.io"

MIN_MARKET_CAP_USD = float(os.environ.get("MIN_MARKET_CAP_USD", "12000"))
# Minimum 24h volume (from DexScreener) required to actually send the alert.
MIN_ALERT_VOLUME_USD = float(os.environ.get("MIN_ALERT_VOLUME_USD", "14000"))
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
# How long we'll keep polling a mint for before giving up if it never hits
# the mcap target (or never shows up on DexScreener at all).
MAX_TRACK_AGE_SECONDS = float(os.environ.get("MAX_TRACK_AGE_SECONDS", "600"))  # 10 min

# Throttling to stay under free-tier rate limits.
RPC_MIN_INTERVAL_SECONDS = float(os.environ.get("RPC_MIN_INTERVAL_SECONDS", "0.25"))  # ~4 req/s
DEXSCREENER_MIN_INTERVAL_SECONDS = float(os.environ.get("DEXSCREENER_MIN_INTERVAL_SECONDS", "0.25"))
DEXSCREENER_BATCH_SIZE = 30  # DexScreener's /tokens/ endpoint accepts up to 30 comma-separated addresses

# --- /stats config -----------------------------------------------------
# How often we re-check called coins' mcap to see if they've hit 2x their
# call mcap yet. Separate (slower) interval from the detection poll loop
# since this can run for up to ~24h per coin and doesn't need to be fast.
STATS_TRACK_INTERVAL_SECONDS = float(os.environ.get("STATS_TRACK_INTERVAL_SECONDS", "30"))
HITRATE_MULTIPLIER = 2.0  # a "hit" = mcap reached >= 2x the mcap at call time
STATS_DAY_TZ = timezone.utc  # the day boundary for /stats resets at 00:00 UTC

# Log line Anchor emits for the pump.fun "create" instruction. logsSubscribe
# is already filtered to txs that mention the program, so this line is what
# actually distinguishes "new mint" from every buy/sell/other tx on it.
CREATE_LOG_MARKER = "Program log: Instruction: Create"

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
# mint -> {created_ts, alerted, name, symbol, misses, volume_usd}
tracked_tokens = {}
tracked_lock = threading.Lock()

watcher_status = {
    "connected": False,
    "tokens_seen": 0,
    "tokens_tracking": 0,
    "alerts_sent": 0,
    "last_error": None,
}

_rpc_last_call = {"ts": 0.0}
_rpc_lock = threading.Lock()

_dex_last_call = {"ts": 0.0}
_dex_lock = threading.Lock()

# Cache for Solana Tracker results to reduce API calls
_tracker_cache = {}
_tracker_cache_lock = threading.Lock()
TRACKER_CACHE_TTL = 300  # 5 minutes

# /stats tracking - one bucket for "today" (UTC), reset automatically the
# first time it's touched after midnight UTC.
# mint -> {name, symbol, initial_mcap, hit_2x}
daily_stats_lock = threading.Lock()
daily_stats = {
    "date": datetime.now(STATS_DAY_TZ).date(),
    "called": {},
}


def _throttle(lock, last_call, min_interval):
    with lock:
        wait = min_interval - (time.time() - last_call["ts"])
        if wait > 0:
            time.sleep(wait)
        last_call["ts"] = time.time()


def rpc_call(method, params, max_retries=4):
    """POST a JSON-RPC call to SOLANA_RPC_URL, throttled and retried on
    429/5xx so we don't get banned by the free public endpoint."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    backoff = 1.0
    for attempt in range(max_retries):
        _throttle(_rpc_lock, _rpc_last_call, RPC_MIN_INTERVAL_SECONDS)
        try:
            resp = requests.post(SOLANA_RPC_URL, json=payload, timeout=15)
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                return None
            return data.get("result")
        except Exception as e:
            print(f"[PumpAlert] RPC call {method} failed: {e}", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 20)
    return None


def get_mint_from_signature(signature, retries=4, delay=1.0):
    """Fetch the tx behind a create-log signature and pull the mint address
    out of it. The pump.fun `create` instruction's account order (per its
    Anchor IDL) puts the new mint at index 0. Retries a few times since the
    tx isn't always fetchable the instant we get the log notification.
    """
    for attempt in range(retries):
        result = rpc_call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed",
                },
            ],
        )
        if result:
            try:
                instructions = result["transaction"]["message"]["instructions"]
                for ix in instructions:
                    if ix.get("programId") == PUMP_FUN_PROGRAM_ID:
                        accounts = ix.get("accounts")
                        if accounts:
                            return accounts[0]
            except (KeyError, TypeError, IndexError):
                pass
            return None  # tx fetched fine but no matching instruction found
        time.sleep(delay)
    return None


def fetch_market_caps(mints):
    """Batch-query DexScreener for a list of mints, return {mint: {mcap, name, symbol, price_usd, volume_usd}}.
    Returns an empty dict (never raises) on failure - callers just retry next poll cycle."""
    if not mints:
        return {}
    _throttle(_dex_lock, _dex_last_call, DEXSCREENER_MIN_INTERVAL_SECONDS)
    url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(mints)}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 429:
            print("[PumpAlert] DexScreener rate limited, backing off", flush=True)
            time.sleep(2)
            return {}
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[PumpAlert] DexScreener fetch failed: {e}", flush=True)
        return {}

    pairs = data.get("pairs") or []
    out = {}
    for pair in pairs:
        base = pair.get("baseToken") or {}
        addr = base.get("address")
        if not addr:
            continue
        mcap = pair.get("marketCap") or pair.get("fdv")
        if mcap is None:
            continue
        # Prefer the first pair we see per mint; if a token has migrated to
        # Raydium it may have multiple pairs, but any one gives a usable mcap.
        if addr not in out:
            # DexScreener returns volume as a nested object keyed by
            # timeframe: {"h24": ..., "h6": ..., "h1": ..., "m5": ...}.
            # There is no top-level "volume24h" and no "usd"/"value" key.
            volume = pair.get("volume")
            if isinstance(volume, dict):
                volume = volume.get("h24")
            
            out[addr] = {
                "mcap": float(mcap),
                "name": base.get("name") or "Unknown",
                "symbol": base.get("symbol") or "N/A",
                "price_usd": pair.get("priceUsd"),
                "volume_usd": float(volume) if volume is not None else None,
            }
    return out


def fetch_solana_tracker_info(mint):
    """Best-effort lookup of buys/sells, age, and socials for
    a mint via the Solana Tracker API. Called once, at alert time - never
    part of the detection/polling loop. Returns None on any failure (missing
    key, timeout, bad response, etc) so a slow/broken lookup never blocks
    the core alert from going out."""
    
    # Check cache first
    with _tracker_cache_lock:
        cached = _tracker_cache.get(mint)
        if cached and (time.time() - cached["ts"]) < TRACKER_CACHE_TTL:
            return cached["data"]
    
    if not SOLANA_TRACKER_API_KEY:
        return None
    
    try:
        resp = requests.get(
            f"{SOLANA_TRACKER_BASE_URL}/tokens/{mint}",
            headers={"x-api-key": SOLANA_TRACKER_API_KEY},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[PumpAlert] Solana Tracker fetch failed for {mint}: {e}", flush=True)
        return None

    token_info = data.get("token") or {}
    pools = data.get("pools") or []
    pool = pools[0] if pools else {}
    txns = pool.get("txns") or {}
    price = pool.get("price") or {}

    age_seconds = None
    created_at = pool.get("createdAt")
    if created_at:
        try:
            age_seconds = max(0, time.time() - (float(created_at) / 1000))
        except (TypeError, ValueError):
            age_seconds = None

    result = {
        "buys": txns.get("buys"),
        "sells": txns.get("sells"),
        "volume_usd": None,  # We use DexScreener for volume now
        "price_usd": price.get("usd"),
        "age_seconds": age_seconds,
        "twitter": token_info.get("twitter"),
        "telegram": token_info.get("telegram"),
        "website": token_info.get("website"),
    }
    
    # Cache the result
    with _tracker_cache_lock:
        _tracker_cache[mint] = {
            "data": result,
            "ts": time.time()
        }
    
    return result


def _format_age(age_seconds):
    if age_seconds is None:
        return None
    age_seconds = int(age_seconds)
    if age_seconds < 60:
        return f"{age_seconds}s"
    minutes = age_seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def _format_price(price_usd):
    if price_usd is None:
        return None
    try:
        price_usd = float(price_usd)
    except (TypeError, ValueError):
        return None
    if price_usd < 0.01:
        return f"${price_usd:.8f}".rstrip("0").rstrip(".")
    return f"${price_usd:,.4f}"


def send_telegram_alert(name, symbol, mint, market_cap_usd, volume_usd):
    """Returns True if the alert was actually sent (used to log the call
    for /stats), False if it was skipped for any reason."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_ids = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()]
    if not token or not chat_ids:
        print("[PumpAlert] Can't send alert - TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set", flush=True)
        return False

    # Volume filter - using DexScreener data we already have
    if volume_usd is not None and float(volume_usd) < MIN_ALERT_VOLUME_USD:
        print(
            f"[PumpAlert] Skipping alert for {mint} - volume ${float(volume_usd):,.0f} "
            f"below ${MIN_ALERT_VOLUME_USD:,.0f} minimum",
            flush=True,
        )
        return False
    
    # If volume is None (shouldn't happen with DexScreener), skip to avoid spam
    if volume_usd is None:
        print(f"[PumpAlert] Skipping {mint} - volume data unavailable", flush=True)
        return False

    extra = fetch_solana_tracker_info(mint)

    # Skip coins with no socials at all. If the lookup itself failed (no API
    # key, timeout, bad response) we can't know either way, so we fail open
    # and still send the alert rather than silently dropping it.
    if extra is not None:
        has_socials = bool(extra.get("twitter") or extra.get("telegram") or extra.get("website"))
        if not has_socials:
            print(f"[PumpAlert] Skipping alert for {mint} - no socials found", flush=True)
            return False

    lines = [
        f"\U0001F680 *{name}* (${symbol})",
        f"*MC:* ${market_cap_usd:,.0f}",
    ]

    if extra:
        price_str = _format_price(extra.get("price_usd"))
        if price_str:
            lines.append(f"*Price:* {price_str}")

        age_str = _format_age(extra.get("age_seconds"))
        if age_str:
            lines.append(f"*Age:* {age_str}")

        buys, sells = extra.get("buys"), extra.get("sells")
        if buys is not None and sells is not None:
            lines.append(f"*Buys/Sells:* {buys}/{sells}")

    # Always show volume from DexScreener
    if volume_usd is not None:
        lines.append(f"*Volume:* ${float(volume_usd):,.0f}")

    if extra:
        socials = []
        if extra.get("twitter"):
            socials.append(f"[Twitter]({extra['twitter']})")
        if extra.get("telegram"):
            socials.append(f"[Telegram]({extra['telegram']})")
        if extra.get("website"):
            socials.append(f"[Website]({extra['website']})")
        if socials:
            lines.append(" | ".join(socials))

    lines.append(f"*CA:* `{mint}`")
    lines.append(
        f"[DexScreener](https://dexscreener.com/solana/{mint}) | "
        f"[pump.fun](https://pump.fun/{mint})"
    )

    text = "\n".join(lines)

    for chat_id in chat_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
        except Exception as e:
            print(f"[PumpAlert] Telegram send failed for {chat_id}: {e}", flush=True)

    return True


# ---------------------------------------------------------------------------
# /stats - daily call count + hitrate (>= 2x call mcap before day resets)
# ---------------------------------------------------------------------------

def _reset_if_new_day_unlocked():
    """Must be called while holding daily_stats_lock."""
    today = datetime.now(STATS_DAY_TZ).date()
    if daily_stats["date"] != today:
        daily_stats["date"] = today
        daily_stats["called"] = {}


def _record_call(mint, name, symbol, mcap_usd):
    """Logs a coin as "called" for today's /stats count, right after an
    alert is actually sent (not just queued/considered)."""
    with daily_stats_lock:
        _reset_if_new_day_unlocked()
        daily_stats["called"][mint] = {
            "name": name,
            "symbol": symbol,
            "initial_mcap": mcap_usd,
            "hit_2x": False,
        }


def _format_stats_message():
    with daily_stats_lock:
        _reset_if_new_day_unlocked()
        date_label = (
            f"{daily_stats['date'].day}/"
            f"{daily_stats['date'].strftime('%b').upper()}/"
            f"{daily_stats['date'].year}"
        )
        total = len(daily_stats["called"])
        hits = sum(1 for e in daily_stats["called"].values() if e["hit_2x"])

    hitrate = (hits / total * 100) if total else 0

    return (
        f"COINS CALLED {date_label}\n\n"
        f"{total} COINS\n\n"
        f"Hitrate: {hitrate:.0f}%"
    )


def _stats_tracking_loop():
    """Runs independently of the detection poll loop. Periodically re-checks
    every not-yet-2x'd coin called today against DexScreener and marks it a
    hit once its mcap reaches HITRATE_MULTIPLIER x its call-time mcap. Stops
    caring about a coin once the day rolls over (00:00 UTC), per design."""
    while True:
        time.sleep(STATS_TRACK_INTERVAL_SECONDS)

        with daily_stats_lock:
            _reset_if_new_day_unlocked()
            pending = [
                mint for mint, e in daily_stats["called"].items() if not e["hit_2x"]
            ]

        if not pending:
            continue

        for i in range(0, len(pending), DEXSCREENER_BATCH_SIZE):
            batch = pending[i:i + DEXSCREENER_BATCH_SIZE]
            results = fetch_market_caps(batch)

            with daily_stats_lock:
                _reset_if_new_day_unlocked()  # day may have flipped mid-loop
                for mint in batch:
                    entry = daily_stats["called"].get(mint)
                    if not entry or entry["hit_2x"]:
                        continue
                    info = results.get(mint)
                    if not info:
                        continue
                    if info["mcap"] >= entry["initial_mcap"] * HITRATE_MULTIPLIER:
                        entry["hit_2x"] = True
                        print(
                            f"[PumpAlert] {entry['symbol']} ({mint}) hit "
                            f"{HITRATE_MULTIPLIER}x call mcap",
                            flush=True,
                        )


def _commands_loop():
    """Long-polls Telegram getUpdates for incoming commands (currently just
    /stats) and replies in the chat it was sent from. Only responds to chats
    listed in TELEGRAM_CHAT_ID, so randoms can't probe a public bot."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    allowed_chat_ids = {
        c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()
    }
    if not token or not allowed_chat_ids:
        return  # nothing to do without credentials - alerts loop already logs this

    offset = None
    while True:
        try:
            params = {"timeout": 30}
            if offset is not None:
                params["offset"] = offset
            resp = requests.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params=params,
                timeout=35,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[PumpAlert] getUpdates failed: {e}", flush=True)
            time.sleep(5)
            continue

        for update in data.get("result", []):
            offset = update["update_id"] + 1
            message = update.get("message") or update.get("channel_post") or {}
            text = (message.get("text") or "").strip()
            chat_id = (message.get("chat") or {}).get("id")
            if not chat_id or not text:
                continue
            if str(chat_id) not in allowed_chat_ids:
                continue

            command = text.split()[0].split("@")[0].lower()
            if command == "/stats":
                try:
                    requests.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id, "text": _format_stats_message()},
                        timeout=10,
                    )
                except Exception as e:
                    print(f"[PumpAlert] Failed to send /stats reply: {e}", flush=True)


# ---------------------------------------------------------------------------
# New-mint detection (websocket)
# ---------------------------------------------------------------------------

def _handle_create_signature(signature):
    """Runs in a worker thread: resolve the mint for a create-tx signature
    and add it to the tracked set. Kept off the asyncio loop since it does
    blocking HTTP calls."""
    mint = get_mint_from_signature(signature)
    if not mint:
        print(f"[PumpAlert] Could not resolve mint for signature {signature}", flush=True)
        return

    with tracked_lock:
        if mint in tracked_tokens:
            return
        tracked_tokens[mint] = {
            "created_ts": time.time(),
            "alerted": False,
            "name": None,
            "symbol": None,
            "volume_usd": None,
        }
        watcher_status["tokens_tracking"] = len(tracked_tokens)
    print(f"[PumpAlert] New mint tracked: {mint}", flush=True)


async def _handle_logs_notification(msg, executor):
    try:
        value = msg["params"]["result"]["value"]
    except (KeyError, TypeError):
        return

    if value.get("err") is not None:
        return  # failed tx, ignore

    logs = value.get("logs") or []
    if not any(CREATE_LOG_MARKER in line for line in logs):
        return  # not a create instruction - most mentions are buys/sells, skip them

    signature = value.get("signature")
    if not signature:
        return

    with tracked_lock:
        watcher_status["tokens_seen"] += 1

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _handle_create_signature, signature)


# Backoff schedule (seconds) used only after an HTTP 429 (rate limited)
# websocket rejection: 1m, 2m, 4m, 8m, 15m max.
RATE_LIMIT_BACKOFF_SCHEDULE = [60, 120, 240, 480, 900]


async def _run_watcher_forever():
    backoff = 5
    rate_limit_backoff_idx = 0
    executor = None
    import concurrent.futures

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)

    while True:
        try:
            async with websockets.connect(SOLANA_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                sub_request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [PUMP_FUN_PROGRAM_ID]},
                        {"commitment": "confirmed"},
                    ],
                }
                await ws.send(json.dumps(sub_request))

                watcher_status["connected"] = True
                watcher_status["last_error"] = None
                backoff = 5
                rate_limit_backoff_idx = 0
                print("[PumpAlert] Connected to Solana RPC websocket, subscribed to pump.fun logs", flush=True)

                async for raw_message in ws:
                    try:
                        msg = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("method") == "logsNotification":
                        await _handle_logs_notification(msg, executor)

        except Exception as e:
            watcher_status["connected"] = False
            watcher_status["last_error"] = str(e)

            if "429" in str(e):
                wait = RATE_LIMIT_BACKOFF_SCHEDULE[min(rate_limit_backoff_idx, len(RATE_LIMIT_BACKOFF_SCHEDULE) - 1)]
                rate_limit_backoff_idx += 1
                print(f"[PumpAlert] Rate limited (429) - backing off {wait}s", flush=True)
                await asyncio.sleep(wait)
                continue

            print(f"[PumpAlert] Connection error: {e} - reconnecting in {backoff}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Market cap polling (plain thread, independent of the websocket loop)
# ---------------------------------------------------------------------------

def _poll_loop():
    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        now = time.time()

        with tracked_lock:
            pending = [
                mint for mint, e in tracked_tokens.items() if not e["alerted"]
            ]

        if not pending:
            continue

        for i in range(0, len(pending), DEXSCREENER_BATCH_SIZE):
            batch = pending[i:i + DEXSCREENER_BATCH_SIZE]
            results = fetch_market_caps(batch)

            for mint in batch:
                info = results.get(mint)
                with tracked_lock:
                    entry = tracked_tokens.get(mint)
                    if not entry or entry["alerted"]:
                        continue
                    if info:
                        entry["name"] = info["name"]
                        entry["symbol"] = info["symbol"]
                        entry["volume_usd"] = info.get("volume_usd")

                if not info:
                    continue

                # Check market cap threshold
                if info["mcap"] >= MIN_MARKET_CAP_USD:
                    with tracked_lock:
                        entry = tracked_tokens.get(mint)
                        if not entry or entry["alerted"]:
                            continue
                        entry["alerted"] = True

                    print(
                        f"[PumpAlert] {info['symbol']} ({mint}) qualified - MC ${info['mcap']:,.0f}, Volume ${info.get('volume_usd', 0):,.0f}",
                        flush=True,
                    )
                    sent = send_telegram_alert(
                        info["name"], 
                        info["symbol"], 
                        mint, 
                        info["mcap"],
                        info.get("volume_usd")
                    )
                    # Only count/record it as a "call" if it actually went
                    # out - send_telegram_alert can still skip internally
                    # (volume/socials filters) even after the mcap gate.
                    if sent:
                        watcher_status["alerts_sent"] += 1
                        _record_call(mint, info["name"], info["symbol"], info["mcap"])

        # Sweep stale/alerted entries out of the tracked dict.
        with tracked_lock:
            for mint, entry in list(tracked_tokens.items()):
                if entry["alerted"] or now - entry["created_ts"] > MAX_TRACK_AGE_SECONDS:
                    del tracked_tokens[mint]
            watcher_status["tokens_tracking"] = len(tracked_tokens)


# ---------------------------------------------------------------------------
# Entry point for app.py
# ---------------------------------------------------------------------------

def start_watcher_background():
    """Starts the mcap poll loop, /stats 2x-tracking loop, and the Telegram
    command listener each in their own thread, then runs the websocket
    listener forever on this thread."""
    threading.Thread(target=_poll_loop, daemon=True).start()
    threading.Thread(target=_stats_tracking_loop, daemon=True).start()
    threading.Thread(target=_commands_loop, daemon=True).start()
    try:
        asyncio.run(_run_watcher_forever())
    except Exception as e:
        watcher_status["connected"] = False
        watcher_status["last_error"] = str(e)
        print(f"[PumpAlert] Watcher crashed: {e}", flush=True)