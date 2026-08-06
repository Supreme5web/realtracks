import os
import json
import time
import asyncio
import threading

import requests
import websockets

# ---------------------------------------------------------------------------
# Config (env vars)
# ---------------------------------------------------------------------------
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Public RPC by default. Free public endpoints are heavily rate-limited and
# sometimes drop long-lived websocket connections - swap in a free-tier
# Helius/QuickNode/Shyft RPC+WS pair via env vars if you see frequent
# reconnects or 429s.
SOLANA_WS_URL = os.environ.get("SOLANA_WS_URL", "wss://api.mainnet-beta.solana.com")
SOLANA_RPC_URL = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

MIN_MARKET_CAP_USD = float(os.environ.get("MIN_MARKET_CAP_USD", "12000"))
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
# How long we'll keep polling a mint for before giving up if it never hits
# the mcap target (or never shows up on DexScreener at all).
MAX_TRACK_AGE_SECONDS = float(os.environ.get("MAX_TRACK_AGE_SECONDS", "1800"))  # 30 min

# Throttling to stay under free-tier rate limits.
RPC_MIN_INTERVAL_SECONDS = float(os.environ.get("RPC_MIN_INTERVAL_SECONDS", "0.25"))  # ~4 req/s
DEXSCREENER_MIN_INTERVAL_SECONDS = float(os.environ.get("DEXSCREENER_MIN_INTERVAL_SECONDS", "0.25"))
DEXSCREENER_BATCH_SIZE = 30  # DexScreener's /tokens/ endpoint accepts up to 30 comma-separated addresses

# Log line Anchor emits for the pump.fun "create" instruction. logsSubscribe
# is already filtered to txs that mention the program, so this line is what
# actually distinguishes "new mint" from every buy/sell/other tx on it.
CREATE_LOG_MARKER = "Program log: Instruction: Create"

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
# mint -> {created_ts, alerted, name, symbol, misses}
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
    """Batch-query DexScreener for a list of mints, return {mint: {mcap, name, symbol, price_usd}}.
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
            out[addr] = {
                "mcap": float(mcap),
                "name": base.get("name") or "Unknown",
                "symbol": base.get("symbol") or "N/A",
                "price_usd": pair.get("priceUsd"),
            }
    return out


def send_telegram_alert(name, symbol, mint, market_cap_usd):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_ids = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()]
    if not token or not chat_ids:
        print("[PumpAlert] Can't send alert - TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set", flush=True)
        return

    text = (
        f"\U0001F680 *{name}* (${symbol})\n"
        f"*MC:* ${market_cap_usd:,.0f}\n"
        f"*CA:* `{mint}`\n"
        f"[DexScreener](https://dexscreener.com/solana/{mint}) | "
        f"[pump.fun](https://pump.fun/{mint})"
    )
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


async def _run_watcher_forever():
    backoff = 5
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

                if not info:
                    continue

                if info["mcap"] >= MIN_MARKET_CAP_USD:
                    with tracked_lock:
                        entry = tracked_tokens.get(mint)
                        if not entry or entry["alerted"]:
                            continue
                        entry["alerted"] = True
                        watcher_status["alerts_sent"] += 1

                    print(
                        f"[PumpAlert] {info['symbol']} ({mint}) qualified - MC ${info['mcap']:,.0f}",
                        flush=True,
                    )
                    send_telegram_alert(info["name"], info["symbol"], mint, info["mcap"])

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
    """Starts the mcap poll loop in its own thread, then runs the websocket
    listener forever on this thread."""
    threading.Thread(target=_poll_loop, daemon=True).start()
    try:
        asyncio.run(_run_watcher_forever())
    except Exception as e:
        watcher_status["connected"] = False
        watcher_status["last_error"] = str(e)
        print(f"[PumpAlert] Watcher crashed: {e}", flush=True)
