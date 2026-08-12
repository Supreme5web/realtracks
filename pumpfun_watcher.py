import os
import json
import time
import asyncio
import threading
from io import BytesIO
from datetime import datetime, timezone

import requests
import websockets

try:
    from PIL import Image
except ImportError:
    Image = None

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

# /stats tracking - one bucket for "today" (UTC), reset automatically the
# first time it's touched after midnight UTC. This is the in-memory
# fallback, used whenever Supabase isn't configured (see below) - it works
# fine but resets on every restart/redeploy/idle spin-down.
# mint -> {name, symbol, initial_mcap, hit_2x}
daily_stats_lock = threading.Lock()
daily_stats = {
    "date": datetime.now(STATS_DAY_TZ).date(),
    "called": {},
}

# Optional Supabase-backed persistence for /stats, so counts/hitrate survive
# restarts, redeploys, and Render free-tier idle spin-downs (all of which
# wipe plain in-memory state and even local disk, since Render's free
# filesystem is ephemeral too). Set SUPABASE_URL + SUPABASE_KEY to enable;
# leave unset and the bot falls back to the in-memory tracking above.
#
# Expected table (run once in the Supabase SQL editor):
#
#   create table pumpfun_calls (
#     call_date date not null,
#     mint text not null,
#     name text,
#     symbol text,
#     initial_mcap numeric,
#     hit_2x boolean not null default false,
#     called_at timestamptz not null default now(),
#     primary key (call_date, mint)
#   );
#
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
STATS_TABLE = os.environ.get("SUPABASE_STATS_TABLE", "pumpfun_calls")

_supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("[PumpAlert] /stats persistence: Supabase", flush=True)
    except Exception as e:
        print(f"[PumpAlert] Supabase init failed, falling back to in-memory /stats: {e}", flush=True)
else:
    print(
        "[PumpAlert] /stats persistence: in-memory only "
        "(set SUPABASE_URL/SUPABASE_KEY to persist across restarts)",
        flush=True,
    )


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


def _find_pumpfun_mint_in_instructions(instructions):
    """Scans a flat list of parsed instructions for one invoking the pump.fun
    program and returns its first account (the mint), or None."""
    for ix in instructions or []:
        if ix.get("programId") == PUMP_FUN_PROGRAM_ID:
            accounts = ix.get("accounts")
            if accounts:
                return accounts[0]
    return None


def get_mint_from_signature(signature, retries=4, delay=1.0):
    """Fetch the tx behind a create-log signature and pull the mint address
    out of it. The pump.fun `create`/`create_v2` instruction's account order
    (per its Anchor IDL) puts the new mint at index 0. Retries a few times
    since the tx isn't always fetchable the instant we get the log
    notification.

    Checks BOTH the transaction's top-level instructions AND its inner
    instructions (meta.innerInstructions) - a growing share of creates go
    through sniper/bundler tools that CPI into the pump.fun program from
    their own top-level instruction, in which case the pump.fun call only
    shows up nested inside innerInstructions. Missing this silently drops
    exactly the fast/competitive launches most likely to actually pump.
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
                top_level = result["transaction"]["message"]["instructions"]
                mint = _find_pumpfun_mint_in_instructions(top_level)
                if mint:
                    return mint

                inner_groups = (result.get("meta") or {}).get("innerInstructions") or []
                for group in inner_groups:
                    mint = _find_pumpfun_mint_in_instructions(group.get("instructions"))
                    if mint:
                        return mint
            except (KeyError, TypeError, IndexError):
                pass
            return None  # tx fetched fine but no matching instruction found
        time.sleep(delay)
    return None


def fetch_market_caps(mints):
    """Batch-query DexScreener for a list of mints, return {mint: {mcap, name, symbol, price_usd, volume_usd, image_url}}.
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

            info = pair.get("info") or {}

            # info.socials items are {"platform": "twitter", "handle": "..."}
            # per DexScreener's actual schema (NOT "type"/"url" - that was
            # wrong in an earlier version of this code). In practice
            # "handle" is the full profile URL, not a bare username.
            socials = {}
            for s in info.get("socials") or []:
                s_platform = (s.get("platform") or "").lower()
                s_handle = s.get("handle")
                if s_platform and s_handle and s_platform not in socials:
                    socials[s_platform] = s_handle
            websites = [w.get("url") for w in (info.get("websites") or []) if w.get("url")]

            out[addr] = {
                "mcap": float(mcap),
                "name": base.get("name") or "Unknown",
                "symbol": base.get("symbol") or "N/A",
                "price_usd": pair.get("priceUsd"),
                "volume_usd": float(volume) if volume is not None else None,
                "image_url": info.get("imageUrl"),
                "socials": socials,
                "website": websites[0] if websites else None,
            }
    return out


def fetch_pumpfun_metadata(mint):
    """pump.fun's own (undocumented but widely used) coin endpoint - has
    image_uri/twitter/telegram/website from the instant the token is
    created, since it's the same data pump.fun's own site reads. Used as
    a fallback/supplement when DexScreener hasn't indexed the pair's info
    block yet (which, for a coin that just hit our mcap threshold, is
    common - DexScreener's own indexing lags behind by anywhere from a
    couple minutes to never for low-effort mints).
    Returns {"image_url", "socials", "website"} or None on any failure."""
    url = f"https://frontend-api-v3.pump.fun/coins/{mint}"
    try:
        resp = requests.get(
            url,
            timeout=8,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json",
            },
        )
        if not resp.ok:
            print(f"[PumpAlert] pump.fun metadata fetch for {mint} returned {resp.status_code}", flush=True)
            return None
        data = resp.json()
    except Exception as e:
        print(f"[PumpAlert] pump.fun metadata fetch failed for {mint}: {e}", flush=True)
        return None

    socials = {}
    if data.get("twitter"):
        socials["twitter"] = data["twitter"]
    if data.get("telegram"):
        socials["telegram"] = data["telegram"]

    return {
        "image_url": data.get("image_uri") or None,
        "socials": socials,
        "website": data.get("website") or None,
    }


def _crop_to_16_9(image_bytes):
    """Center-crops raw image bytes to a 16:9 aspect ratio and returns JPEG
    bytes. Returns None on any failure (corrupt image, unsupported format,
    Pillow not installed, etc) so a bad image never blocks the alert."""
    if Image is None:
        return None
    try:
        img = Image.open(BytesIO(image_bytes))
        img = img.convert("RGB")
        w, h = img.size
        target_ratio = 16 / 9
        current_ratio = w / h

        if current_ratio > target_ratio:
            new_w = int(h * target_ratio)
            left = (w - new_w) // 2
            img = img.crop((left, 0, left + new_w, h))
        else:
            new_h = int(w / target_ratio)
            top = (h - new_h) // 2
            img = img.crop((0, top, w, top + new_h))

        out = BytesIO()
        img.save(out, format="JPEG", quality=90)
        return out.getvalue()
    except Exception as e:
        print(f"[PumpAlert] Image crop failed: {e}", flush=True)
        return None


SOCIAL_BUTTON_LABELS = {
    "twitter": "\U0001F426 Twitter/X",
    "x": "\U0001F426 Twitter/X",
    "telegram": "\u2708\ufe0f Telegram",
    "discord": "\U0001F3AE Discord",
}


def _build_reply_markup(mint):
    """Just the chart button now - socials/website are plain text links in
    the message itself instead of buttons."""
    return {
        "inline_keyboard": [
            [{"text": "Check Live Chart", "url": f"https://dexscreener.com/solana/{mint}"}]
        ]
    }


def send_telegram_alert(name, symbol, mint, market_cap_usd, volume_usd, image_url=None, socials=None, website=None):
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

    # Skip if mcap has outrun volume - i.e. price is up on thin trading,
    # not real activity (classic rug/manipulated-curve signature).
    if volume_usd is not None and market_cap_usd > float(volume_usd):
        print(
            f"[PumpAlert] Skipping alert for {mint} - mcap ${market_cap_usd:,.0f} "
            f"> volume ${float(volume_usd):,.0f}",
            flush=True,
        )
        return False

    # If volume is None (shouldn't happen with DexScreener), skip to avoid spam
    if volume_usd is None:
        print(f"[PumpAlert] Skipping {mint} - volume data unavailable", flush=True)
        return False

    lines = [
        f"*{name}* [{symbol}]",
        "",
        f"\U0001F4B0 *Market Cap:* ${market_cap_usd:,.0f}",
    ]

    # Always show volume from DexScreener
    if volume_usd is not None:
        lines.append(f"\U0001F4CA *Volume:* ${float(volume_usd):,.0f}")

    # Socials/website as embedded (clickable) Markdown links rather than
    # raw pasted URLs or buttons.
    social_lines = []
    for s_type, label in SOCIAL_BUTTON_LABELS.items():
        url = (socials or {}).get(s_type)
        if url:
            social_lines.append(f"[{label}]({url})")
    if website:
        social_lines.append(f"[\U0001F310 Website]({website})")

    # SOCIAL_BUTTON_LABELS has both "twitter" and "x" mapped to the same
    # label - dedupe so the same link doesn't show up twice.
    seen = set()
    deduped_social_lines = []
    for line in social_lines:
        if line not in seen:
            deduped_social_lines.append(line)
            seen.add(line)

    if deduped_social_lines:
        lines.append("")
        lines.extend(deduped_social_lines)

    lines.append("")
    lines.append(f"`{mint}`")

    text = "\n".join(lines)

    reply_markup = _build_reply_markup(mint)

    # Fetch + center-crop the token image to 16:9. Best-effort - any failure
    # here just falls back to a text-only alert rather than blocking it, but
    # we log *why* every time so a persistent failure is diagnosable from
    # the Render logs instead of just silently never sending photos.
    photo_bytes = None
    if not image_url:
        print(f"[PumpAlert] No image_url from DexScreener for {mint} - sending text-only", flush=True)
    else:
        try:
            # dd.dexscreener.com (DexScreener's image CDN) commonly 403s
            # requests that don't look like a browser - a bare User-Agent
            # header is enough to get past that.
            img_resp = requests.get(
                image_url,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            )
            img_resp.raise_for_status()
            photo_bytes = _crop_to_16_9(img_resp.content)
            if photo_bytes is None:
                print(
                    f"[PumpAlert] Image fetched for {mint} but crop failed "
                    f"(Pillow available: {Image is not None}) - sending text-only",
                    flush=True,
                )
        except Exception as e:
            print(f"[PumpAlert] Failed to fetch token image for {mint} from {image_url}: {e}", flush=True)

    for chat_id in chat_ids:
        try:
            if photo_bytes:
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendPhoto",
                    data={
                        "chat_id": chat_id,
                        "caption": text,
                        "parse_mode": "Markdown",
                        "reply_markup": json.dumps(reply_markup),
                    },
                    files={"photo": ("token.jpg", photo_bytes, "image/jpeg")},
                    timeout=15,
                )
                if not resp.ok:
                    print(
                        f"[PumpAlert] Telegram sendPhoto failed for {chat_id} "
                        f"({resp.status_code}): {resp.text[:300]}",
                        flush=True,
                    )
            else:
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": True,
                        "reply_markup": reply_markup,
                    },
                    timeout=10,
                )
                if not resp.ok:
                    print(
                        f"[PumpAlert] Telegram sendMessage failed for {chat_id} "
                        f"({resp.status_code}): {resp.text[:300]}",
                        flush=True,
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
    today = datetime.now(STATS_DAY_TZ).date()

    if _supabase is not None:
        try:
            _supabase.table(STATS_TABLE).upsert(
                {
                    "call_date": today.isoformat(),
                    "mint": mint,
                    "name": name,
                    "symbol": symbol,
                    "initial_mcap": mcap_usd,
                    "hit_2x": False,
                }
            ).execute()
            return
        except Exception as e:
            print(f"[PumpAlert] Supabase upsert failed for {mint}: {e}", flush=True)
            # fall through to in-memory so we don't lose the record entirely

    with daily_stats_lock:
        _reset_if_new_day_unlocked()
        daily_stats["called"][mint] = {
            "name": name,
            "symbol": symbol,
            "initial_mcap": mcap_usd,
            "hit_2x": False,
        }


def _format_stats_message():
    today = datetime.now(STATS_DAY_TZ).date()

    if _supabase is not None:
        try:
            resp = (
                _supabase.table(STATS_TABLE)
                .select("hit_2x")
                .eq("call_date", today.isoformat())
                .execute()
            )
            rows = resp.data or []
            total = len(rows)
            hits = sum(1 for r in rows if r.get("hit_2x"))
        except Exception as e:
            print(f"[PumpAlert] Supabase read failed for /stats: {e}", flush=True)
            total, hits = 0, 0
    else:
        with daily_stats_lock:
            _reset_if_new_day_unlocked()
            total = len(daily_stats["called"])
            hits = sum(1 for e in daily_stats["called"].values() if e["hit_2x"])

    date_label = f"{today.day}/{today.strftime('%b').upper()}/{today.year}"
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
        today = datetime.now(STATS_DAY_TZ).date()

        if _supabase is not None:
            try:
                resp = (
                    _supabase.table(STATS_TABLE)
                    .select("mint,initial_mcap")
                    .eq("call_date", today.isoformat())
                    .eq("hit_2x", False)
                    .execute()
                )
                pending_map = {r["mint"]: r["initial_mcap"] for r in (resp.data or [])}
            except Exception as e:
                print(f"[PumpAlert] Supabase read failed in stats tracking loop: {e}", flush=True)
                continue
        else:
            with daily_stats_lock:
                _reset_if_new_day_unlocked()
                pending_map = {
                    mint: e["initial_mcap"]
                    for mint, e in daily_stats["called"].items()
                    if not e["hit_2x"]
                }

        pending = list(pending_map.keys())
        if not pending:
            continue

        for i in range(0, len(pending), DEXSCREENER_BATCH_SIZE):
            batch = pending[i:i + DEXSCREENER_BATCH_SIZE]
            results = fetch_market_caps(batch)

            hit_mints = [
                mint for mint in batch
                if results.get(mint)
                and results[mint]["mcap"] >= pending_map[mint] * HITRATE_MULTIPLIER
            ]
            if not hit_mints:
                continue

            if _supabase is not None:
                for mint in hit_mints:
                    try:
                        _supabase.table(STATS_TABLE).update({"hit_2x": True}).eq(
                            "call_date", today.isoformat()
                        ).eq("mint", mint).execute()
                    except Exception as e:
                        print(f"[PumpAlert] Supabase update failed for {mint}: {e}", flush=True)
            else:
                with daily_stats_lock:
                    _reset_if_new_day_unlocked()
                    for mint in hit_mints:
                        entry = daily_stats["called"].get(mint)
                        if entry:
                            entry["hit_2x"] = True

            for mint in hit_mints:
                print(
                    f"[PumpAlert] {mint} hit {HITRATE_MULTIPLIER}x call mcap",
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

                    image_url = info.get("image_url")
                    socials = info.get("socials") or {}
                    website = info.get("website")

                    # DexScreener's info block (image/socials/website) is
                    # frequently not indexed yet this early - fall back to
                    # pump.fun's own API to fill in whatever's missing.
                    if not image_url or not socials or not website:
                        pf_meta = fetch_pumpfun_metadata(mint)
                        if pf_meta:
                            image_url = image_url or pf_meta.get("image_url")
                            website = website or pf_meta.get("website")
                            merged_socials = dict(pf_meta.get("socials") or {})
                            merged_socials.update(socials)  # DexScreener wins on conflict
                            socials = merged_socials

                    sent = send_telegram_alert(
                        info["name"], 
                        info["symbol"], 
                        mint, 
                        info["mcap"],
                        info.get("volume_usd"),
                        image_url,
                        socials,
                        website,
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