import os
import json
import time
import asyncio
import statistics
import threading
from io import BytesIO
from datetime import datetime, timedelta, timezone

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

# Prefer Ankr if a key is provided - much higher rate limits and a far
# more stable websocket than the public RPC. Falls back to PublicNode's
# free mainnet endpoint (no key required) if no Ankr key is set.
#
# IMPORTANT: pump.fun only exists on Solana MAINNET. Make sure any RPC
# endpoint you use is a mainnet one, NOT devnet/testnet - devnet will
# connect fine and report "healthy" but will never see any real pump.fun
# activity, since it doesn't exist there. Ankr's mainnet Solana endpoint
# is locked behind a paid plan on the free tier, so PublicNode (free,
# no key) is the default here instead. Set ANKR_API_KEY if/when you
# upgrade, or set SOLANA_RPC_URL / SOLANA_WS_URL directly to point at
# any other provider (Chainstack, Syndica, etc).
ANKR_API_KEY = os.environ.get("ANKR_API_KEY", "")
if ANKR_API_KEY:
    _default_ws = f"wss://rpc.ankr.com/solana/ws/{ANKR_API_KEY}"
    _default_rpc = f"https://rpc.ankr.com/solana/{ANKR_API_KEY}"
else:
    _default_ws = "wss://solana-rpc.publicnode.com"
    _default_rpc = "https://solana-rpc.publicnode.com"

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

# /stats timeframe selector - the inline keyboard shown under /stats lets
# the user flip between these rolling windows (hours). Default matches the
# original behavior (24h).
STATS_TIMEFRAME_OPTIONS_HOURS = [1, 4, 6, 12, 24]
STATS_DEFAULT_TIMEFRAME_HOURS = 24

# Multiples of the call-time market cap we track "time to X" for, so /stats
# and /ask can tell you not just THAT a coin ran, but how fast it typically
# does, so you know how long to actually wait around for an entry/exit.
MILESTONE_MULTIPLIERS = [1.5, 2.0, 3.0, 5.0, 10.0]

# --- AI insights (/ask) config ------------------------------------------
# Optional - lets you ask a plain-English question about this bot's own
# call history (e.g. "what volume should I look for before buying?") and
# get an answer grounded in the /stats data above, via Gemini 3.5
# Flash-Lite. Leave GEMINI_API_KEY unset and /ask just tells you it's not
# configured; everything else in the bot works the same either way.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

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
# mint -> {name, symbol, initial_mcap, ath_mcap}
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
#     ath_mcap numeric,
#     initial_volume numeric,
#     ath_volume numeric,
#     ath_reached_at timestamptz,
#     call_hour_utc int,
#     milestones jsonb not null default '{}'::jsonb,
#     called_at timestamptz not null default now(),
#     primary key (call_date, mint)
#   );
#
# If you already have this table from before, just add whichever of these
# columns are missing - old ones can stay, they're simply no longer (or
# still) written to:
#
#   alter table pumpfun_calls add column if not exists ath_mcap numeric;
#   alter table pumpfun_calls add column if not exists initial_volume numeric;
#   alter table pumpfun_calls add column if not exists ath_volume numeric;
#   alter table pumpfun_calls add column if not exists ath_reached_at timestamptz;
#   alter table pumpfun_calls add column if not exists call_hour_utc int;
#   alter table pumpfun_calls add column if not exists milestones jsonb not null default '{}'::jsonb;
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


def _escape_md(s):
    """Escapes Telegram legacy-Markdown special chars in untrusted text
    (coin names/symbols come straight from token metadata, which isn't
    controlled by us) so a stray "_", "*", "[", or "]" doesn't break
    parsing and silently drop the whole message."""
    if not s:
        return s
    for ch in ("_", "*", "[", "]"):
        s = s.replace(ch, f"\\{ch}")
    return s


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
        f"\U0001F680 *{_escape_md(name)}* [{_escape_md(symbol)}]",
        "\u2500" * 18,
        f"\U0001F4B0 *Market Cap:*  ${market_cap_usd:,.0f}",
    ]

    # Always show volume from DexScreener
    if volume_usd is not None:
        lines.append(f"\U0001F4CA *Volume:*  ${float(volume_usd):,.0f}")

    # Socials/website as embedded (clickable) Markdown links, on one row
    # separated by bullets rather than stacked lines - keeps the card short.
    social_links = []
    for s_type, label in SOCIAL_BUTTON_LABELS.items():
        url = (socials or {}).get(s_type)
        if url:
            social_links.append(f"[{label}]({url})")
    if website:
        social_links.append(f"[\U0001F310 Website]({website})")

    # SOCIAL_BUTTON_LABELS has both "twitter" and "x" mapped to the same
    # label - dedupe so the same link doesn't show up twice.
    seen = set()
    deduped_social_links = []
    for link in social_links:
        if link not in seen:
            deduped_social_links.append(link)
            seen.add(link)

    if deduped_social_links:
        lines.append("\u2500" * 18)
        lines.append("   \u2022   ".join(deduped_social_links))

    lines.append("\u2500" * 18)
    lines.append(f"\U0001F4CB *Contract*\n`{mint}`")

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


def _record_call(mint, name, symbol, mcap_usd, volume_usd=None):
    """Logs a coin as "called" for today's /stats count, right after an
    alert is actually sent (not just queued/considered). Seeds the row with
    initial mcap/volume and the UTC hour of the call, so the tracking loop
    below can later fill in ath_mcap/ath_volume and time-to-milestone."""
    today = datetime.now(STATS_DAY_TZ).date()
    now = datetime.now(timezone.utc)
    hour_utc = now.hour

    if _supabase is not None:
        try:
            _supabase.table(STATS_TABLE).upsert(
                {
                    "call_date": today.isoformat(),
                    "mint": mint,
                    "name": name,
                    "symbol": symbol,
                    "initial_mcap": mcap_usd,
                    "ath_mcap": mcap_usd,
                    "initial_volume": volume_usd,
                    "ath_volume": volume_usd,
                    "call_hour_utc": hour_utc,
                    "milestones": {},
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
            "ath_mcap": mcap_usd,
            "initial_volume": volume_usd,
            "ath_volume": volume_usd,
            "call_hour_utc": hour_utc,
            "milestones": {},
            "called_at": time.time(),
        }


def _format_recent_message():
    """Shows the last 10 coins called (most recent first) with the multiple
    each has hit so far (ath_mcap / initial_mcap), independent of the
    /stats day boundary. With Supabase persistence this looks across all
    days; the in-memory fallback can only see today's calls, since that
    state resets at 00:00 UTC same as /stats."""
    if _supabase is not None:
        try:
            resp = (
                _supabase.table(STATS_TABLE)
                .select("name,symbol,initial_mcap,ath_mcap,called_at")
                .order("called_at", desc=True)
                .limit(10)
                .execute()
            )
            rows = resp.data or []
        except Exception as e:
            print(f"[PumpAlert] Supabase read failed for /recent: {e}", flush=True)
            rows = []
    else:
        with daily_stats_lock:
            _reset_if_new_day_unlocked()
            rows = list(daily_stats["called"].values())[-10:][::-1]

    if not rows:
        return "\U0001F4CB *LAST 10 CALLS*\n" + "\u2500" * 18 + "\nNo calls yet."

    lines = ["\U0001F4CB *LAST 10 CALLS*", "\u2500" * 18]
    for i, r in enumerate(rows, start=1):
        name = _escape_md(r.get("name") or "Unknown")
        symbol = _escape_md(r.get("symbol") or "N/A")
        initial = r.get("initial_mcap")
        ath = r.get("ath_mcap")
        if initial and ath and initial > 0:
            mult_str = f"*{ath / initial:.1f}x*"
        else:
            mult_str = "N/A"
        lines.append(f"{i}. {name} [{symbol}] \u2014 {mult_str}")

    return "\n".join(lines)


# Plain numbered emojis (1..10) used for the /stats top-10 list, so it
# reads as a straightforward ranked list rather than a podium.
NUMBER_EMOJIS = [
    "1\uFE0F\u20E3", "2\uFE0F\u20E3", "3\uFE0F\u20E3", "4\uFE0F\u20E3", "5\uFE0F\u20E3",
    "6\uFE0F\u20E3", "7\uFE0F\u20E3", "8\uFE0F\u20E3", "9\uFE0F\u20E3", "\U0001F51F",
]


def _build_stats_keyboard(selected_hours):
    """Inline keyboard of timeframe buttons shown under /stats. The
    currently-selected window gets a checkmark so it's obvious which one
    is active; tapping another posts a `stats_tf:<hours>` callback that
    _commands_loop uses to re-render the same message in place."""
    row = []
    for hours in STATS_TIMEFRAME_OPTIONS_HOURS:
        label = f"{hours}H"
        if hours == selected_hours:
            label = f"\u2705 {label}"
        row.append({"text": label, "callback_data": f"stats_tf:{hours}"})
    return {"inline_keyboard": [row]}


def _gather_rows(hours):
    """Fetches all the fields needed for /stats and /ask, scoped to a
    rolling `hours`-wide window."""
    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=hours)

    if _supabase is not None:
        try:
            resp = (
                _supabase.table(STATS_TABLE)
                .select(
                    "name,symbol,initial_mcap,ath_mcap,initial_volume,ath_volume,"
                    "call_hour_utc,milestones,called_at"
                )
                .gte("called_at", cutoff_dt.isoformat())
                .execute()
            )
            return resp.data or []
        except Exception as e:
            print(f"[PumpAlert] Supabase read failed: {e}", flush=True)
            return []

    # In-memory fallback only ever holds "today" (it's cleared at 00:00 UTC
    # - see _reset_if_new_day_unlocked), so a window request that reaches
    # back across the day boundary just won't see calls from before the
    # reset. Acceptable trade-off for the no-Supabase fallback; Supabase
    # mode above isn't affected by this.
    cutoff_ts = time.time() - hours * 3600
    with daily_stats_lock:
        _reset_if_new_day_unlocked()
        return [
            r for r in daily_stats["called"].values()
            if (r.get("called_at") or 0) >= cutoff_ts
        ]


def _format_stats_message(hours=STATS_DEFAULT_TIMEFRAME_HOURS):
    """Builds the /stats text for a rolling `hours`-wide window (1/4/6/12/24h,
    selected via the inline keyboard). Total calls, hitrate, milestone
    hit-rates/timing, and the top-10 list are all scoped to that window."""
    rows = _gather_rows(hours)
    total = len(rows)

    # Multiple reached = all-time-high mcap / mcap at call time. Skip any
    # row with no usable initial_mcap (shouldn't happen, but division safety).
    multiples = []
    for r in rows:
        initial = r.get("initial_mcap")
        ath = r.get("ath_mcap")
        if initial and ath and initial > 0:
            multiples.append((ath / initial, r))

    hits = sum(1 for mult, _ in multiples if mult >= HITRATE_MULTIPLIER)
    hitrate = (hits / total * 100) if total else 0

    window_label = f"Last {hours}H"

    lines = [
        f"\U0001F4CA *COINS CALLED* \u2014 {window_label}",
        "\u2500" * 18,
        f"\U0001F3AF *Total Calls:*  {total}",
        f"\u2705 *Hitrate (\u2265{HITRATE_MULTIPLIER:g}x):*  {hitrate:.0f}%",
    ]

    milestone_line = _format_milestone_line(rows, total)
    if milestone_line:
        lines.append(milestone_line)

    best_hour_line = _format_best_hour_line(rows)
    if best_hour_line:
        lines.append(best_hour_line)

    top10 = sorted(multiples, key=lambda pair: pair[0], reverse=True)[:10]
    if top10:
        lines.append("\u2500" * 18)
        lines.append("\U0001F3C6 *Top 10 by ATH*")
        for number_emoji, (mult, r) in zip(NUMBER_EMOJIS, top10):
            name = _escape_md(r.get("name") or "Unknown")
            symbol = _escape_md(r.get("symbol") or "N/A")
            lines.append(f"{number_emoji} {name} [{symbol}] \u2014 *{mult:.1f}x*")

    return "\n".join(lines)


def _format_milestone_line(rows, total):
    """One compact line summarizing how many calls hit 2x and 5x, and the
    median time it took - the two numbers most useful for deciding how
    long to actually wait around after a call before giving up on it."""
    if not total:
        return None
    times_2x, times_5x = [], []
    hits_2x = hits_5x = 0
    for r in rows:
        milestones = r.get("milestones") or {}
        if "2.0" in milestones:
            hits_2x += 1
            times_2x.append(milestones["2.0"])
        if "5.0" in milestones:
            hits_5x += 1
            times_5x.append(milestones["5.0"])
    if not times_2x and not times_5x:
        return None
    parts = []
    if times_2x:
        parts.append(
            f"2x: {hits_2x}/{total} ({hits_2x/total*100:.0f}%), median {_format_duration(statistics.median(times_2x))}"
        )
    if times_5x:
        parts.append(
            f"5x: {hits_5x}/{total} ({hits_5x/total*100:.0f}%), median {_format_duration(statistics.median(times_5x))}"
        )
    return f"\u23F1 *Speed to hit:*  " + "  \u2022  ".join(parts)


def _format_best_hour_line(rows, min_samples=3):
    """One line naming the UTC hour with the best 2x hit rate, so you know
    when this bot's calls have historically performed best. Requires at
    least `min_samples` calls in an hour to avoid a single lucky/unlucky
    call skewing the result."""
    buckets = {}
    for r in rows:
        hour = r.get("call_hour_utc")
        if hour is None:
            continue
        milestones = r.get("milestones") or {}
        b = buckets.setdefault(hour, {"total": 0, "hits": 0})
        b["total"] += 1
        if "2.0" in milestones:
            b["hits"] += 1
    qualifying = {h: v for h, v in buckets.items() if v["total"] >= min_samples}
    if not qualifying:
        return None
    best_hour, v = max(qualifying.items(), key=lambda kv: kv[1]["hits"] / kv[1]["total"])
    rate = v["hits"] / v["total"] * 100
    return f"\U0001F550 *Best hour (UTC):*  {best_hour:02d}:00\u2013{best_hour:02d}:59 \u2014 {v['hits']}/{v['total']} ({rate:.0f}%) hit 2x"


def _format_duration(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def get_insights_digest(hours=STATS_DEFAULT_TIMEFRAME_HOURS):
    """Builds a plain-text analytical digest (not Markdown-formatted, no
    emoji) of call performance over the given window - this is what gets
    handed to Gemini as grounding context for /ask, and is also usable
    standalone if you want the raw numbers. Covers: volume of coins that
    ran vs didn't, hit rates + timing per multiplier, and best call hour."""
    rows = _gather_rows(hours)
    total = len(rows)
    if not total:
        return f"No calls in the last {hours}h - nothing to analyze yet."

    lines = [f"Window: last {hours}h. Total calls: {total}."]

    for m in MILESTONE_MULTIPLIERS:
        key = str(m)
        times = [r["milestones"][key] for r in rows if key in (r.get("milestones") or {})]
        hits = len(times)
        rate = hits / total * 100
        if times:
            lines.append(
                f"- Reached {m}x: {hits}/{total} ({rate:.0f}%). "
                f"Median time: {_format_duration(statistics.median(times))}, "
                f"avg: {_format_duration(statistics.mean(times))}."
            )
        else:
            lines.append(f"- Reached {m}x: {hits}/{total} ({rate:.0f}%).")

    winners_vol, losers_vol = [], []
    for r in rows:
        vol = r.get("initial_volume")
        if vol is None:
            continue
        if "2.0" in (r.get("milestones") or {}):
            winners_vol.append(vol)
        else:
            losers_vol.append(vol)
    if winners_vol and losers_vol:
        lines.append(
            f"- Avg volume at call time: ${statistics.mean(winners_vol):,.0f} for coins that reached 2x, "
            f"vs ${statistics.mean(losers_vol):,.0f} for coins that didn't (yet)."
        )

    hour_buckets = {}
    for r in rows:
        hour = r.get("call_hour_utc")
        if hour is None:
            continue
        b = hour_buckets.setdefault(hour, {"total": 0, "hits": 0})
        b["total"] += 1
        if "2.0" in (r.get("milestones") or {}):
            b["hits"] += 1
    qualifying = {h: v for h, v in hour_buckets.items() if v["total"] >= 3}
    if qualifying:
        best_hour, v = max(qualifying.items(), key=lambda kv: kv[1]["hits"] / kv[1]["total"])
        lines.append(
            f"- Best call hour (UTC, min 3 samples): {best_hour:02d}:00-{best_hour:02d}:59, "
            f"{v['hits']}/{v['total']} ({v['hits']/v['total']*100:.0f}%) reached 2x."
        )

    ranked = []
    for r in rows:
        initial, ath = r.get("initial_mcap"), r.get("ath_mcap")
        if initial and ath and initial > 0:
            ranked.append((ath / initial, r))
    ranked.sort(key=lambda x: x[0], reverse=True)
    if ranked:
        lines.append("Top runners (peak vs call market cap):")
        for ratio, r in ranked[:5]:
            lines.append(
                f"  * {r.get('name') or 'Unknown'} ({r.get('symbol') or 'N/A'}): {ratio:.1f}x "
                f"- call MC ${r.get('initial_mcap', 0):,.0f} -> peak MC ${r.get('ath_mcap', 0):,.0f}"
            )

    return "\n".join(lines)


_GEMINI_SYSTEM_PREAMBLE = (
    "You are a trading data analyst for a Solana pump.fun call/alert bot. "
    "You'll be given a plain-text digest of how this specific bot's past "
    "calls actually performed (hit rates per multiple, time-to-multiple, "
    "volume patterns, best call hour, top runners). Answer the user's "
    "question using ONLY that data. Be concise and concrete - cite real "
    "numbers from the digest instead of generic trading advice. If the "
    "digest doesn't have enough samples to answer confidently, say so "
    "plainly instead of guessing. Don't pad every answer with a financial "
    "disclaimer - only mention it if directly relevant."
)


def ask_gemini(question, hours=STATS_DEFAULT_TIMEFRAME_HOURS):
    """Sends the current insights digest + the user's question to Gemini
    3.5 Flash-Lite and returns its answer as a string. Never raises -
    returns a plain-text error message on any failure."""
    if not GEMINI_API_KEY:
        return "GEMINI_API_KEY isn't set, so I can't ask the AI anything yet - add it in Render's env vars."

    digest = get_insights_digest(hours)
    prompt = f"{_GEMINI_SYSTEM_PREAMBLE}\n\n--- CALL HISTORY DIGEST ---\n{digest}\n\n--- QUESTION ---\n{question}"

    try:
        resp = requests.post(
            GEMINI_URL,
            params={"key": GEMINI_API_KEY},
            json={
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 700},
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        candidates = data.get("candidates") or []
        if not candidates:
            return "Gemini didn't return an answer - it may have blocked the response. Try rephrasing."
        parts = candidates[0].get("content", {}).get("parts", [])
        text = "".join(p.get("text", "") for p in parts).strip()
        return text or "Gemini returned an empty response - try rephrasing the question."
    except Exception as e:
        print(f"[PumpAlert] Gemini request failed: {e}", flush=True)
        return f"Couldn't reach Gemini right now ({e}). Try again in a bit."


def _parse_called_at(value):
    """called_at comes back as a float epoch (in-memory) or an ISO8601
    string (Supabase). Normalizes either to an epoch float."""
    if isinstance(value, (int, float)):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return time.time()


def _stats_tracking_loop():
    """Runs independently of the detection poll loop. Periodically re-checks
    EVERY coin called today against DexScreener (not just ones that haven't
    hit the hitrate multiplier yet - that was the old design). For each
    coin it:
      - ratchets ath_mcap/ath_volume up whenever the current mcap exceeds
        the stored ath, recording ath_reached_at too, so ath_mcap ends up
        being the coin's true all-time-high for the day (used by the
        hitrate % and top-10 list in /stats), and ath_volume tells you how
        much volume was actually flowing at that peak.
      - fills in milestones (time-to-1.5x/2x/3x/5x/10x the call mcap) the
        first time each multiple is crossed, so /stats and /ask can tell
        you how long a typical run actually takes.
    Stops caring about a coin once the day rolls over (00:00 UTC), per
    design."""
    while True:
        time.sleep(STATS_TRACK_INTERVAL_SECONDS)
        today = datetime.now(STATS_DAY_TZ).date()

        if _supabase is not None:
            try:
                resp = (
                    _supabase.table(STATS_TABLE)
                    .select("mint,initial_mcap,ath_mcap,ath_volume,called_at,milestones")
                    .eq("call_date", today.isoformat())
                    .execute()
                )
                row_map = {r["mint"]: r for r in (resp.data or [])}
            except Exception as e:
                print(f"[PumpAlert] Supabase read failed in stats tracking loop: {e}", flush=True)
                continue
        else:
            with daily_stats_lock:
                _reset_if_new_day_unlocked()
                row_map = {
                    mint: dict(e) for mint, e in daily_stats["called"].items()
                }

        mints = list(row_map.keys())
        if not mints:
            continue

        for i in range(0, len(mints), DEXSCREENER_BATCH_SIZE):
            batch = mints[i:i + DEXSCREENER_BATCH_SIZE]
            results = fetch_market_caps(batch)

            updates = {}  # mint -> dict of fields to persist
            for mint in batch:
                info = results.get(mint)
                if not info:
                    continue
                row = row_map.get(mint) or {}
                current_ath = row.get("ath_mcap") or 0
                current_mcap = info["mcap"]
                current_volume = info.get("volume_usd")

                fields = {}
                if current_mcap > current_ath:
                    fields["ath_mcap"] = current_mcap
                    fields["ath_volume"] = current_volume
                    fields["ath_reached_at"] = datetime.now(timezone.utc).isoformat()

                initial_mcap = row.get("initial_mcap") or 0
                if initial_mcap > 0:
                    milestones = dict(row.get("milestones") or {})
                    called_at = _parse_called_at(row.get("called_at"))
                    ratio = current_mcap / initial_mcap
                    new_milestone = False
                    for m in MILESTONE_MULTIPLIERS:
                        key = str(m)
                        if ratio >= m and key not in milestones:
                            milestones[key] = round(time.time() - called_at, 1)
                            new_milestone = True
                    if new_milestone:
                        fields["milestones"] = milestones

                if fields:
                    updates[mint] = fields

            if not updates:
                continue

            if _supabase is not None:
                for mint, fields in updates.items():
                    try:
                        _supabase.table(STATS_TABLE).update(fields).eq(
                            "call_date", today.isoformat()
                        ).eq("mint", mint).execute()
                    except Exception as e:
                        print(f"[PumpAlert] Supabase update failed for {mint}: {e}", flush=True)
            else:
                with daily_stats_lock:
                    _reset_if_new_day_unlocked()
                    for mint, fields in updates.items():
                        entry = daily_stats["called"].get(mint)
                        if entry:
                            entry.update(fields)


def _commands_loop():
    """Long-polls Telegram getUpdates for incoming commands (/stats and
    /recent) plus the /stats timeframe-button taps (callback queries), and
    replies in the chat it was sent from. Only responds to chats listed in
    TELEGRAM_CHAT_ID, so randoms can't probe a public bot."""
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

            callback_query = update.get("callback_query")
            if callback_query:
                _handle_stats_callback(token, callback_query, allowed_chat_ids)
                continue

            message = update.get("message") or update.get("channel_post") or {}
            text = (message.get("text") or "").strip()
            chat_id = (message.get("chat") or {}).get("id")
            if not chat_id or not text:
                continue
            if str(chat_id) not in allowed_chat_ids:
                continue

            command = text.split()[0].split("@")[0].lower()
            if command == "/stats":
                reply_text = _format_stats_message(STATS_DEFAULT_TIMEFRAME_HOURS)
                try:
                    resp = requests.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={
                            "chat_id": chat_id,
                            "text": reply_text,
                            "parse_mode": "Markdown",
                            "reply_markup": _build_stats_keyboard(STATS_DEFAULT_TIMEFRAME_HOURS),
                        },
                        timeout=10,
                    )
                    if not resp.ok:
                        print(
                            f"[PumpAlert] /stats sendMessage failed for {chat_id} "
                            f"({resp.status_code}): {resp.text[:300]}",
                            flush=True,
                        )
                except Exception as e:
                    print(f"[PumpAlert] Failed to send /stats reply: {e}", flush=True)
            elif command == "/recent":
                reply_text = _format_recent_message()
                try:
                    resp = requests.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={
                            "chat_id": chat_id,
                            "text": reply_text,
                            "parse_mode": "Markdown",
                        },
                        timeout=10,
                    )
                    if not resp.ok:
                        print(
                            f"[PumpAlert] /recent sendMessage failed for {chat_id} "
                            f"({resp.status_code}): {resp.text[:300]}",
                            flush=True,
                        )
                except Exception as e:
                    print(f"[PumpAlert] Failed to send /recent reply: {e}", flush=True)
            elif command == "/ask":
                question = text[len(command):].strip()
                if not question:
                    _send_plain_message(token, chat_id, "Usage: /ask <your question> - e.g. /ask what volume should I look for?")
                else:
                    _send_plain_message(token, chat_id, ask_gemini(question))
            elif not command.startswith("/"):
                # Not a recognized slash-command - treat any plain-text
                # message as a question for the AI, so you can just type
                # naturally instead of remembering the /ask prefix.
                _send_plain_message(token, chat_id, ask_gemini(text))


def _send_plain_message(token, chat_id, text):
    """Small helper for the AI-reply paths (/ask + free-text questions) -
    plain text, no Markdown, since Gemini's output isn't Telegram-escaped
    and could contain characters that break Markdown parsing."""
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=15,
        )
        if not resp.ok:
            print(
                f"[PumpAlert] sendMessage failed for {chat_id} ({resp.status_code}): {resp.text[:300]}",
                flush=True,
            )
    except Exception as e:
        print(f"[PumpAlert] Failed to send message to {chat_id}: {e}", flush=True)


def _handle_stats_callback(token, callback_query, allowed_chat_ids):
    """Handles a tap on one of the /stats timeframe buttons: re-renders the
    same message in place with the new window's data, swaps the checkmark
    to the newly-selected button, and acks the tap so Telegram stops
    showing the button's loading spinner."""
    callback_id = callback_query.get("id")
    data = callback_query.get("data") or ""
    message = callback_query.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")

    if not data.startswith("stats_tf:") or not chat_id or not message_id:
        # Not a button we recognize / stale message - still ack it so the
        # tap doesn't spin forever on the user's end.
        if callback_id:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                    json={"callback_query_id": callback_id},
                    timeout=10,
                )
            except Exception:
                pass
        return

    if str(chat_id) not in allowed_chat_ids:
        return

    try:
        hours = int(data.split(":", 1)[1])
    except (IndexError, ValueError):
        hours = STATS_DEFAULT_TIMEFRAME_HOURS
    if hours not in STATS_TIMEFRAME_OPTIONS_HOURS:
        hours = STATS_DEFAULT_TIMEFRAME_HOURS

    reply_text = _format_stats_message(hours)

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "text": reply_text,
                "parse_mode": "Markdown",
                "reply_markup": _build_stats_keyboard(hours),
            },
            timeout=10,
        )
        if not resp.ok and "message is not modified" not in resp.text:
            # Telegram 400s harmlessly if the tapped button was already the
            # active one (text/markup unchanged) - not worth logging.
            print(
                f"[PumpAlert] stats_tf editMessageText failed for {chat_id} "
                f"({resp.status_code}): {resp.text[:300]}",
                flush=True,
            )
    except Exception as e:
        print(f"[PumpAlert] Failed to edit /stats message for {chat_id}: {e}", flush=True)

    if callback_id:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                json={"callback_query_id": callback_id},
                timeout=10,
            )
        except Exception as e:
            print(f"[PumpAlert] answerCallbackQuery failed: {e}", flush=True)


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
                        _record_call(mint, info["name"], info["symbol"], info["mcap"], info.get("volume_usd"))

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