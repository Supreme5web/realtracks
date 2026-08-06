import os
import json
import time
import hashlib
import asyncio
import threading

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

MIN_MARKET_CAP_USD = float(os.environ.get("MIN_MARKET_CAP_USD", "12000"))
MAX_MARKET_CAP_USD = float(os.environ.get("MAX_MARKET_CAP_USD", "60000"))
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
# How long we'll keep polling a mint for before giving up if it never hits
# the mcap target (or never shows up on DexScreener at all).
MAX_TRACK_AGE_SECONDS = float(os.environ.get("MAX_TRACK_AGE_SECONDS", "600"))  # 10 min

# Rug filter: skip alerting if the top 10 non-bonding-curve holders control
# this much or more of total supply. The bonding curve's own token account
# is excluded from this calc since it legitimately holds most of the unsold
# supply pre-migration - that's the mechanism, not a red flag.
MAX_TOP10_HOLDER_PCT = float(os.environ.get("MAX_TOP10_HOLDER_PCT", "60"))
# Don't re-run the (2 extra RPC calls) holder check more than this often per
# mint - it only matters for mints currently sitting in the alert window.
HOLDER_CHECK_COOLDOWN_SECONDS = float(os.environ.get("HOLDER_CHECK_COOLDOWN_SECONDS", "15"))

# Throttling to stay under free-tier rate limits.
RPC_MIN_INTERVAL_SECONDS = float(os.environ.get("RPC_MIN_INTERVAL_SECONDS", "0.25"))  # ~4 req/s
DEXSCREENER_MIN_INTERVAL_SECONDS = float(os.environ.get("DEXSCREENER_MIN_INTERVAL_SECONDS", "0.25"))
DEXSCREENER_BATCH_SIZE = 30  # DexScreener's /tokens/ endpoint accepts up to 30 comma-separated addresses

# Log line Anchor emits for the pump.fun "create" instruction. logsSubscribe
# is already filtered to txs that mention the program, so this line is what
# actually distinguishes "new mint" from every buy/sell/other tx on it. It's
# only a cheap pre-filter though - the log line alone doesn't prove *which*
# program in the tx emitted it, since sniper/launcher bots (Bloom, BullX,
# Trojan, etc.) commonly CPI into pump.fun from a wrapper program. The real
# check happens in get_mint_from_signature via the instruction discriminator.
CREATE_LOG_MARKER = "Program log: Instruction: Create"

# Anchor instruction discriminator = first 8 bytes of sha256("global:<ix_name>").
# This is what actually identifies pump.fun's own `create` instruction,
# regardless of whether it's a top-level instruction or reached via CPI.
CREATE_IX_DISCRIMINATOR = list(hashlib.sha256(b"global:create").digest()[:8])

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX = {c: i for i, c in enumerate(_B58_ALPHABET)}


def b58decode(s):
    """Minimal base58 decoder (Bitcoin/Solana alphabet) - avoids depending
    on the external `base58` package, which isn't worth the risk of another
    missed-dependency deploy failure for ~15 lines of code."""
    if not s:
        return b""
    num = 0
    for ch in s:
        num = num * 58 + _B58_INDEX[ch]
    combined = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""
    n_leading_zeros = len(s) - len(s.lstrip("1"))
    return b"\x00" * n_leading_zeros + combined

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


def _is_create_ix(ix):
    """True if this parsed instruction is pump.fun's own `create` call,
    verified by its Anchor discriminator - not just by programId, since a
    wrapper/sniper-bot program invoking pump.fun via CPI would otherwise be
    mistaken for it (and give the wrong accounts entirely)."""
    if ix.get("programId") != PUMP_FUN_PROGRAM_ID:
        return False
    data = ix.get("data")
    if not data:
        return False
    try:
        raw = b58decode(data)
    except Exception:
        return False
    return list(raw[:8]) == CREATE_IX_DISCRIMINATOR


def get_mint_from_signature(signature, retries=4, delay=1.0):
    """Fetch the tx behind a create-log signature and pull the mint address
    (and bonding-curve token account) out of it. Per pump.fun's Anchor IDL,
    the `create` instruction's accounts are ordered:
    [mint, mint_authority, bonding_curve, associated_bonding_curve, global, ...]
    so index 0 is the new mint and index 3 is the ATA that holds the unsold
    supply pre-migration (needed later to exclude it from holder-% checks).
    Checks both top-level and inner (CPI) instructions, since many creates
    are routed through a launcher/sniper-bot program rather than calling
    pump.fun directly. Retries a few times since the tx isn't always
    fetchable the instant we get the log notification.

    Returns (mint, bonding_curve_ata) or (None, None).
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
                candidates = list(result["transaction"]["message"]["instructions"])
                for inner in (result.get("meta") or {}).get("innerInstructions") or []:
                    candidates.extend(inner.get("instructions") or [])
            except (KeyError, TypeError):
                candidates = []

            for ix in candidates:
                if _is_create_ix(ix):
                    accounts = ix.get("accounts")
                    if accounts:
                        mint = accounts[0]
                        bonding_curve_ata = accounts[3] if len(accounts) > 3 else None
                        return mint, bonding_curve_ata
            return None, None  # tx fetched fine but no matching create instruction found
        time.sleep(delay)
    return None, None


def get_top10_holder_pct(mint, exclude_account):
    """Returns the % of total supply held by the top 10 token accounts,
    excluding `exclude_account` (the bonding curve's own ATA). Returns None
    if either RPC call fails - callers should treat that as "unknown, try
    again later" rather than a rejection."""
    largest = rpc_call("getTokenLargestAccounts", [mint])
    supply = rpc_call("getTokenSupply", [mint])
    if not largest or not supply:
        return None
    try:
        accounts = largest.get("value") or []
        total = int(supply["value"]["amount"])
        if total <= 0:
            return None
        filtered = [a for a in accounts if a.get("address") != exclude_account]
        filtered.sort(key=lambda a: int(a.get("amount", 0)), reverse=True)
        top10_sum = sum(int(a.get("amount", 0)) for a in filtered[:10])
        return (top10_sum / total) * 100
    except (KeyError, TypeError, ValueError):
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


def send_telegram_alert(name, symbol, mint, market_cap_usd, top10_pct=None):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_ids = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()]
    if not token or not chat_ids:
        print("[PumpAlert] Can't send alert - TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set", flush=True)
        return

    holder_line = f"*Top 10 holders:* {top10_pct:.1f}%\n" if top10_pct is not None else ""
    text = (
        f"\U0001F680 *{name}* (${symbol})\n"
        f"*MC:* ${market_cap_usd:,.0f}\n"
        f"{holder_line}"
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
    mint, bonding_curve_ata = get_mint_from_signature(signature)
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
            "bonding_curve_ata": bonding_curve_ata,
            "last_holder_check_ts": 0.0,
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

                mcap = info["mcap"]

                if mcap >= MAX_MARKET_CAP_USD:
                    # Missed the window going up - drop it, no alert.
                    with tracked_lock:
                        entry = tracked_tokens.get(mint)
                        if entry and not entry["alerted"]:
                            entry["alerted"] = True  # reuse flag to stop future checks/sweep it out
                    continue

                if mcap < MIN_MARKET_CAP_USD:
                    continue  # not in the window yet, keep polling

                # In the [MIN, MAX) window - check holder concentration
                # before alerting, but don't hammer RPC every 5s poll.
                with tracked_lock:
                    entry = tracked_tokens.get(mint)
                    if not entry or entry["alerted"]:
                        continue
                    if now - entry["last_holder_check_ts"] < HOLDER_CHECK_COOLDOWN_SECONDS:
                        continue
                    entry["last_holder_check_ts"] = now
                    bonding_curve_ata = entry.get("bonding_curve_ata")

                top10_pct = get_top10_holder_pct(mint, bonding_curve_ata)

                if top10_pct is None:
                    print(f"[PumpAlert] {info['symbol']} ({mint}) holder check inconclusive, will retry", flush=True)
                    continue

                if top10_pct >= MAX_TOP10_HOLDER_PCT:
                    print(
                        f"[PumpAlert] {info['symbol']} ({mint}) skipped - top10 holders {top10_pct:.1f}% "
                        f"(>= {MAX_TOP10_HOLDER_PCT}%)",
                        flush=True,
                    )
                    continue  # stays tracked, will re-check next cooldown window

                with tracked_lock:
                    entry = tracked_tokens.get(mint)
                    if not entry or entry["alerted"]:
                        continue
                    entry["alerted"] = True
                    watcher_status["alerts_sent"] += 1

                print(
                    f"[PumpAlert] {info['symbol']} ({mint}) qualified - MC ${mcap:,.0f}, "
                    f"top10 {top10_pct:.1f}%",
                    flush=True,
                )
                send_telegram_alert(info["name"], info["symbol"], mint, mcap, top10_pct)

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
