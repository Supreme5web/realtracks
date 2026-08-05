import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import click

# global dict for tracking last processed slot for the wallets being tracked
last_processed_slots = {}

# Weighted-average cost basis per (wallet, token), used to compute realized
# PnL on sells. Lives only in memory - resets if the process restarts, so
# PnL on a token won't be accurate until we've seen at least one buy of it
# since the last deploy.
_cost_basis = {}
_cost_basis_lock = threading.Lock()

# Cached SOL/USD price, refreshed at most once every 60s so we don't hit
# the price API on every single alert.
_sol_price_cache = {"price": None, "ts": 0}
_sol_price_lock = threading.Lock()
SOL_PRICE_CACHE_TTL_SECONDS = 300  # 5 min - CoinGecko's free tier is IP-rate-limited and
# Render's free-tier IPs are shared across many apps, so a short TTL just means more 429s.


def format_compact_number(value):
    """Format a number as a compact string, e.g. 1234567 -> '1.23M'."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_000_000_000:
        return f"{sign}{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{sign}{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{sign}{value / 1_000:.2f}K"
    return f"{sign}{value:.2f}"


def format_amount(value):
    """Format a raw token/SOL amount with thousands separators, e.g. 3803702.61 -> '3,803,702.61'."""
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "N/A"


def format_sol(value):
    """Format a SOL amount, trimming trailing zeros, e.g. 4.9000 -> '4.9'."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"
    s = f"{value:.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


def format_usd(value, signed=False):
    """Format a USD amount, e.g. 678.64 -> '$678.64', or with signed=True: 8977.69 -> '$+8,977.69'."""
    if value is None:
        return "N/A"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if signed:
        prefix = "+" if value >= 0 else "-"
        return f"${prefix}{abs(value):,.2f}"
    return f"${value:,.2f}"


def format_signed_pct(value):
    if value is None:
        return "N/A"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"
    prefix = "+" if value >= 0 else ""
    return f"{prefix}{value:.2f}%"


def escape_markdown(text):
    """
    Escape Telegram legacy-Markdown special characters in untrusted text
    (token names/tickers come from Codex and can contain '_', '*', '[', etc.,
    which otherwise breaks Telegram's parser and causes the whole message
    to be rejected with a 400 Bad Request).
    """
    if not text:
        return text
    text = str(text)
    for ch in ("_", "*", "`", "[", "]"):
        text = text.replace(ch, f"\\{ch}")
    return text


def get_sol_price_usd():
    """Current SOL/USD price via CoinGecko, cached for 60s. Returns None on failure."""
    now = time.time()
    with _sol_price_lock:
        if _sol_price_cache["price"] is not None and now - _sol_price_cache["ts"] < SOL_PRICE_CACHE_TTL_SECONDS:
            return _sol_price_cache["price"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana", "vs_currencies": "usd"},
            timeout=10,
        )
        resp.raise_for_status()
        price = resp.json()["solana"]["usd"]
        with _sol_price_lock:
            _sol_price_cache["price"] = price
            _sol_price_cache["ts"] = now
        return price
    except Exception as e:
        click.echo(f"SOL price lookup failed: {e}")
        with _sol_price_lock:
            return _sol_price_cache["price"]  # may still be None, or a stale-but-usable value


def record_buy(wallet_address, token_mint, units, cost_usd):
    """Add to the weighted-average cost basis for this wallet/token."""
    if units <= 0 or cost_usd is None:
        return
    key = f"{wallet_address}:{token_mint}"
    with _cost_basis_lock:
        entry = _cost_basis.setdefault(key, {"units": 0.0, "cost_usd": 0.0})
        entry["units"] += units
        entry["cost_usd"] += cost_usd


def record_sell(wallet_address, token_mint, units_sold, proceeds_usd):
    """
    Realize PnL on a sell using weighted-average cost basis.
    Returns (realized_pnl_usd, pnl_pct), or (None, None) if we have no
    buy history for this wallet/token (e.g. the buy happened before this
    process started, or the service has restarted since).
    """
    if units_sold <= 0:
        return None, None
    key = f"{wallet_address}:{token_mint}"
    with _cost_basis_lock:
        entry = _cost_basis.get(key)
        if not entry or entry["units"] <= 0:
            return None, None
        avg_cost_per_unit = entry["cost_usd"] / entry["units"]
        units_sold_capped = min(units_sold, entry["units"])  # can't realize more than we tracked
        cost_basis_sold = avg_cost_per_unit * units_sold_capped
        entry["units"] -= units_sold_capped
        entry["cost_usd"] -= cost_basis_sold
        if entry["units"] <= 1e-9:
            entry["units"] = 0.0
            entry["cost_usd"] = 0.0

    if proceeds_usd is None or cost_basis_sold <= 0:
        return None, None
    realized_pnl = proceeds_usd - cost_basis_sold
    pnl_pct = (realized_pnl / cost_basis_sold) * 100
    return realized_pnl, pnl_pct


# Trading bots to link out to on each alert, with your referral codes baked
# into the URL. Rendered as inline buttons (3 per row) under each Telegram
# buy/sell alert, keyed off the token's contract address (CA).
TRADING_BOTS = [
    {"label": "AXI", "build_url": lambda ca: f"https://axiom.trade/t/{ca}/@supremee5?chain=sol"},
    {"label": "TRO", "build_url": lambda ca: f"https://t.me/menelaus_trojanbot?start=d-supremeesol-{ca}"},
    {"label": "BONK", "build_url": lambda ca: f"https://t.me/bonkbot_bot?start=ref_ggydi_ca_{ca}"},
    {"label": "MAE", "build_url": lambda ca: f"https://t.me/maestro?start={ca}-hollydodson"},
    {"label": "GMGN", "build_url": lambda ca: f"https://gmgn.ai/sol/token/supremee5_{ca}"},
    {"label": "COVE", "build_url": lambda ca: f"https://t.me/cove_trading_bot?start=ref_supremeesol-{ca}"},
]


def build_trading_bot_keyboard(token_mint):
    """Telegram inline keyboard of quick-trade buttons (your referral links) for a token, 3 per row."""
    buttons = [{"text": bot["label"], "url": bot["build_url"](token_mint)} for bot in TRADING_BOTS]
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    return {"inline_keyboard": rows}


def build_trading_bot_links_text(token_mint):
    """Plain markdown links for the same trading bots, for use in Discord embeds
    (incoming webhooks can't reliably render interactive buttons like Telegram can)."""
    return " | ".join(f"[{bot['label']}]({bot['build_url'](token_mint)})" for bot in TRADING_BOTS)


# Mints that show up as intermediate routing hops in multi-hop swaps (e.g. a
# Jupiter route that goes Token -> USDC -> Token) rather than as a token the
# person actually chose to buy or sell. We never alert on these directly.
IGNORED_MINTS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "So11111111111111111111111111111111111111112",  # Wrapped SOL
}

WSOL_MINT = "So11111111111111111111111111111111111111112"


def extract_sol_amount(transaction, wallet_address):
    """
    The SOL leg of a swap doesn't always land as a plain native balance change
    on the wallet's own address. Many aggregator routes (Jupiter etc.) wrap/
    unwrap SOL through the wallet's *associated WSOL token account* - a
    different pubkey than the wallet's main address - to move through the
    route. When that happens, the wallet's own nativeBalanceChange only
    reflects the network/priority fee it paid (a few thousand to a few
    hundred-thousand lamports), while the real traded amount shows up as a
    tokenBalanceChanges entry for the WSOL mint, tagged with `userAccount`
    equal to the wallet (even though the token *account* itself has its own,
    different pubkey).

    We take whichever of the two is larger, since a fee-sized native change
    should never win over a real WSOL-routed trade amount.
    """
    native_change = 0.0
    wsol_change = 0.0
    for account in transaction.get("accountData", []):
        if account.get("account") == wallet_address:
            native_change = abs(account.get("nativeBalanceChange", 0)) / 1e9
        for tbc in account.get("tokenBalanceChanges", []) or []:
            if tbc.get("userAccount") == wallet_address and tbc.get("mint") == WSOL_MINT:
                raw = tbc.get("rawTokenAmount", {}) or {}
                try:
                    amt = abs(int(raw.get("tokenAmount", 0))) / (10 ** int(raw.get("decimals", 9)))
                except (TypeError, ValueError):
                    amt = 0.0
                wsol_change += amt
    return round(max(native_change, wsol_change), 4)

CODEX_GRAPHQL_URL = "https://graph.codex.io/graphql"
SOLANA_NETWORK_ID = 1399811149

CODEX_TOKEN_QUERY = """
query GetTokenInfo($tokens: [String]) {
  filterTokens(tokens: $tokens, limit: 1) {
    results {
      marketCap
      circulatingMarketCap
      priceUSD
      token {
        name
        symbol
      }
    }
  }
}
"""


def get_token_info(token_mint, codex_api_key):
    """
    Look up market cap / price / name / ticker for a token via the Codex.io API.
    Never raises - returns safe defaults on any failure (missing key, bad
    response, no match, network error) so a single bad lookup can't crash
    the whole monitoring loop.
    """
    defaults = {"market_cap": 0, "name": "Unknown", "ticker": "N/A", "price_usd": 0}

    if not codex_api_key:
        return defaults

    try:
        token_id = f"{token_mint}:{SOLANA_NETWORK_ID}"
        resp = requests.post(
            CODEX_GRAPHQL_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": codex_api_key,
            },
            json={"query": CODEX_TOKEN_QUERY, "variables": {"tokens": [token_id]}},
            timeout=10,
        )
        if not resp.ok:
            click.echo(f"Codex lookup failed for {token_mint}: HTTP {resp.status_code}")
            return defaults

        data = resp.json()
        if data.get("errors"):
            click.echo(f"Codex lookup returned errors for {token_mint}: {data['errors']}")
            return defaults

        results = (data.get("data") or {}).get("filterTokens", {}).get("results") or []
        if not results:
            return defaults

        result = results[0]
        token = result.get("token") or {}
        # marketCap is fully-diluted; fall back to circulatingMarketCap if unset
        market_cap = result.get("marketCap") or result.get("circulatingMarketCap") or 0
        price_usd = result.get("priceUSD") or 0

        return {
            "market_cap": market_cap,
            "name": token.get("name") or "Unknown",
            "ticker": token.get("symbol") or "N/A",
            "price_usd": price_usd,
        }
    except Exception as e:
        click.echo(f"Codex lookup failed for {token_mint}: {e}")
        return defaults


def build_buy_message(wallet_name, wallet_address, token_info, token_amount, sol_amount,
                       usd_value, token_mint):
    safe_wallet_name = escape_markdown(wallet_name)
    safe_ticker = escape_markdown(token_info["ticker"])
    cap = format_compact_number(token_info["market_cap"])
    token_link = f"https://solscan.io/token/{token_mint}"

    return (
        f"\U0001F7E2 *BUY ALERT*\n\n"
        f"*{safe_wallet_name}* swapped {format_sol(sol_amount)} SOL \u2192 "
        f"{format_amount(token_amount)} ({format_usd(usd_value)}) "
        f"[#{safe_ticker}]({token_link}) at {cap} MC\n\n"
        f"CA: `{token_mint}`\n"
        f"Wallet: `{wallet_address}`"
    )


def build_sell_message(wallet_name, wallet_address, token_info, token_amount, sol_amount,
                        usd_value, token_mint, pnl_usd, pnl_pct):
    safe_wallet_name = escape_markdown(wallet_name)
    safe_ticker = escape_markdown(token_info["ticker"])
    cap = format_compact_number(token_info["market_cap"])
    token_link = f"https://solscan.io/token/{token_mint}"

    lines = [
        f"\U0001F534 *SELL ALERT*",
        "",
        f"*{safe_wallet_name}* swapped {format_amount(token_amount)} ({format_usd(usd_value)}) "
        f"[#{safe_ticker}]({token_link}) \u2192 {format_sol(sol_amount)} SOL at {cap} MC",
        "",
    ]
    if pnl_usd is not None and pnl_pct is not None:
        lines.append(f"PnL: {format_usd(pnl_usd, signed=True)} ({format_signed_pct(pnl_pct)})")
    lines.append(f"CA: `{token_mint}`")
    lines.append(f"Wallet: `{wallet_address}`")
    return "\n".join(lines)


def send_telegram_notification(bot_token, chat_id, message, reply_markup=None):
    """Send a message to a Telegram chat via the Bot API, optionally with an inline keyboard."""
    if not bot_token or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        response = requests.post(url, json=payload, timeout=10)
        if not response.ok:
            # Telegram puts the real reason (e.g. "can't parse entities") in the
            # JSON body, not the status line, so log that instead of just the code.
            try:
                detail = response.json().get("description", response.text)
            except ValueError:
                detail = response.text
            click.echo(f"Telegram API error {response.status_code}: {detail}")
        response.raise_for_status()
    except requests.RequestException:
        # Avoid ever logging the bot token, which requests.RequestException's
        # str(e) includes as part of the request URL.
        click.echo("Error sending Telegram notification (request failed)")


def send_discord_notification(webhook_url, wallet_name, wallet_address, action, token_mint,
                               token_amount, sol_amount, usd_value, coin_cap, coin_name,
                               coin_ticker, pnl_usd=None, pnl_pct=None):
    """Send a Discord notification with a formatted embed. (Optional, only used if a Discord webhook is configured.)"""
    if not webhook_url:
        return
    try:
        embed_color = 65280 if action == "BOUGHT" else 16711680  # Green for BOUGHT, Red for SOLD
        fields = [
            {"name": "**Token**", "value": f'${coin_ticker} - {coin_name}', "inline": True},
            {"name": "**Market Cap**", "value": f'${format_compact_number(coin_cap)}', "inline": False},
            {"name": "**Amount**", "value": f"{format_amount(token_amount)} ({format_usd(usd_value)})", "inline": False},
            {"name": "**SOL**", "value": f"{format_sol(sol_amount)} SOL", "inline": False},
        ]
        if pnl_usd is not None and pnl_pct is not None:
            fields.append({"name": "**PnL**", "value": f"{format_usd(pnl_usd, signed=True)} ({format_signed_pct(pnl_pct)})", "inline": False})
        fields.append({"name": "**Token Mint**", "value": token_mint, "inline": False})
        fields.append({"name": "**Wallet**", "value": wallet_address, "inline": False})
        fields.append({"name": "**Trade**", "value": build_trading_bot_links_text(token_mint), "inline": False})

        embed = {
            "embeds": [
                {
                    "title": f"{wallet_name} - **{action}**",
                    "color": embed_color,
                    "fields": fields,
                    "footer": {"text": "Transaction Notification"},
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                }
            ]
        }
        response = requests.post(webhook_url, json=embed, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        click.echo(f"Error sending Discord notification: {e}")


def notify(alerts_config, action, wallet_name, wallet_address, token_info, token_mint,
           token_amount, sol_amount, signature=None):
    """
    Compute USD value / holdings % / PnL for a buy or sell, update the cost-basis
    tracker, and fan the alert out to whichever channels are configured.
    """
    telegram_bot_token = alerts_config.get("telegram_bot_token")
    telegram_chat_ids = alerts_config.get("telegram_chat_ids") or []
    discord_webhook = alerts_config.get("discord_webhook")

    sol_price = get_sol_price_usd()
    # USD value of the token leg, shown next to the token amount in the alert.
    try:
        usd_value = token_amount * float(token_info.get("price_usd") or 0)
    except (TypeError, ValueError):
        usd_value = None
    if not usd_value:
        usd_value = None

    # USD value of the SOL leg - what was actually paid/received - used for cost basis / PnL,
    # since it reflects the real trade rather than a possibly-stale token price snapshot.
    sol_usd_value = (sol_amount * sol_price) if sol_price is not None else None

    # Sanity check: the token-price-derived USD value and the SOL-leg-derived USD value
    # should roughly agree. When they don't, something is off (an off-chain price feed
    # that's stale/wrong for a low-liquidity token, a bundled/multi-hop tx whose net SOL
    # balance change doesn't equal the swap's gross value, etc.) - log it with the
    # signature so a specific mismatched trade can actually be looked up on Solscan later,
    # rather than only ever surfacing as an unexplained-looking number in the alert.
    if usd_value and sol_usd_value and min(usd_value, sol_usd_value) > 0:
        ratio = max(usd_value, sol_usd_value) / min(usd_value, sol_usd_value)
        if ratio >= 3:
            sig_note = f" sig={signature}" if signature else ""
            click.echo(
                f"[SolTracker] USD/SOL value mismatch for {wallet_name} ({token_mint}): "
                f"token-price ${usd_value:,.2f} vs sol-leg ${sol_usd_value:,.2f}{sig_note}"
            )

    pnl_usd = None
    pnl_pct = None

    if action == "BOUGHT":
        if sol_usd_value is not None:
            record_buy(wallet_address, token_mint, token_amount, sol_usd_value)
    elif action == "SOLD":
        pnl_usd, pnl_pct = record_sell(wallet_address, token_mint, token_amount, sol_usd_value)

    if telegram_bot_token and telegram_chat_ids:
        if action == "BOUGHT":
            message = build_buy_message(
                wallet_name, wallet_address, token_info, token_amount, sol_amount,
                usd_value, token_mint,
            )
        else:
            message = build_sell_message(
                wallet_name, wallet_address, token_info, token_amount, sol_amount,
                usd_value, token_mint, pnl_usd, pnl_pct,
            )
        keyboard = build_trading_bot_keyboard(token_mint)
        for chat_id in telegram_chat_ids:
            send_telegram_notification(telegram_bot_token, chat_id, message, reply_markup=keyboard)

    if discord_webhook:
        send_discord_notification(
            discord_webhook, wallet_name, wallet_address, action, token_mint,
            token_amount, sol_amount, usd_value, token_info["market_cap"], token_info["name"],
            token_info["ticker"], pnl_usd, pnl_pct,
        )


def execute_monitoring(wallet, helius_key, codex_api_key, alerts_config):
    wallet_name = wallet["name"]
    wallet_address = wallet["address"]

    if wallet_address not in last_processed_slots:
        last_processed_slots[wallet_address] = None

    while True:
        # NOTE: api.helius.xyz has been unreliable / is being phased out for this endpoint.
        # Helius's current docs only list mainnet.helius-rpc.com as the server for
        # GET /v0/addresses/{address}/transactions.
        base_url = f"https://mainnet.helius-rpc.com/v0/addresses/{wallet_address}/transactions"
        params = {
            "api-key": helius_key,
            # Current API takes singular "type" (requests serializes the list as
            # repeated type=TRANSFER&type=SWAP). The old "types" param is not recognized.
            "type": ["TRANSFER", "SWAP"],
            "limit": 5,
        }

        try:
            response = requests.get(base_url, params=params, timeout=15)
            response.raise_for_status()
            transactions = response.json()

            if transactions:
                last_processed_slots[wallet_address] = process_transactions(
                    wallet, transactions, codex_api_key, alerts_config, last_processed_slots[wallet_address]
                )
            else:
                click.echo(f"No new transactions for {wallet_name} ({wallet_address}).")

            time.sleep(30)
        except requests.exceptions.RequestException as e:
            # Include the response body (if any) so a non-200 is actionable in logs
            # instead of just "500 Server Error: Internal Server Error for url: ...".
            body = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                try:
                    body = f" | body: {resp.text[:300]}"
                except Exception:
                    pass
            click.echo(f"Error fetching transactions for {wallet_name} - {wallet_address}: {e}{body}")
            time.sleep(60)


def process_transactions(wallet, transactions, codex_api_key, alerts_config, last_processed_slot):
    wallet_address = wallet["address"]
    wallet_name = wallet["name"]
    transactions = sorted(transactions, key=lambda x: x["slot"])

    latest_slot = last_processed_slot

    for transaction in transactions:
        slot = transaction["slot"]

        if last_processed_slot and slot <= last_processed_slot:
            continue

        if not latest_slot or slot > latest_slot:
            latest_slot = slot

        sol_amount = extract_sol_amount(transaction, wallet_address)

        # Process SWAP transactions (wallet trades one token for another directly)
        if transaction["type"] == "SWAP":
            token_transfers = transaction.get("tokenTransfers", [])
            if not token_transfers:
                continue

            if len(token_transfers) == 1:
                # A single SPL-token leg means the other side of the swap was
                # native SOL (SOL isn't an SPL token transfer - it shows up as
                # the nativeBalanceChange we already captured as sol_amount).
                # This is what Helius reports for most aggregator-routed
                # (e.g. Jupiter) buys/sells against SOL.
                leg = token_transfers[0]
                token_mint = leg["mint"]
                amount = leg["tokenAmount"]

                if token_mint in IGNORED_MINTS or amount <= 0 or not sol_amount:
                    continue

                token_info = get_token_info(token_mint, codex_api_key)
                if leg.get("fromUserAccount") == wallet_address:
                    notify(alerts_config, "SOLD", wallet_name, wallet_address, token_info,
                           token_mint, amount, sol_amount, signature=transaction.get("signature"))
                elif leg.get("toUserAccount") == wallet_address:
                    notify(alerts_config, "BOUGHT", wallet_name, wallet_address, token_info,
                           token_mint, amount, sol_amount, signature=transaction.get("signature"))
                continue

            # Two (or more) SPL-token legs: a direct token-to-token swap with
            # no SOL involved. Resolve the actual outgoing/incoming legs for
            # this wallet rather than assuming index 0/1, skip it entirely if
            # either leg is a routing-hop stablecoin, and skip posting anything
            # if we can't cleanly resolve both sides (rather than post a
            # message with "N/A" in it).
            from_token = next((t for t in token_transfers if t.get("fromUserAccount") == wallet_address), None)
            to_token = next((t for t in token_transfers if t.get("toUserAccount") == wallet_address), None)

            if not from_token or not to_token:
                continue
            if from_token["mint"] in IGNORED_MINTS or to_token["mint"] in IGNORED_MINTS:
                continue

            out_info = get_token_info(from_token["mint"], codex_api_key)
            in_info = get_token_info(to_token["mint"], codex_api_key)
            message = (
                f"\U0001F501 *{escape_markdown(wallet_name)}* SWAPPED\n\n"
                f"*Sold:* {format_amount(from_token['tokenAmount'])} {escape_markdown(out_info['ticker'])}\n"
                f"*Bought:* {format_amount(to_token['tokenAmount'])} {escape_markdown(in_info['ticker'])}\n"
                f"CA (sold): `{from_token['mint']}`\n"
                f"CA (bought): `{to_token['mint']}`\n"
                f"Wallet: `{wallet_address}`"
            )
            telegram_bot_token = alerts_config.get("telegram_bot_token")
            telegram_chat_ids = alerts_config.get("telegram_chat_ids") or []
            if telegram_bot_token and telegram_chat_ids:
                # Buttons trade the newly-acquired token, since that's the position going forward.
                keyboard = build_trading_bot_keyboard(to_token["mint"])
                for chat_id in telegram_chat_ids:
                    send_telegram_notification(telegram_bot_token, chat_id, message, reply_markup=keyboard)

        # Process TRANSFER transactions (simple buy/sell)
        elif transaction["type"] == "TRANSFER":
            for token_transfer in transaction.get("tokenTransfers", []):
                token_mint = token_transfer["mint"]
                amount = token_transfer["tokenAmount"]

                if token_mint in IGNORED_MINTS or amount <= 0:
                    continue

                if token_transfer.get("toUserAccount") == wallet_address:
                    token_info = get_token_info(token_mint, codex_api_key)
                    notify(alerts_config, "BOUGHT", wallet_name, wallet_address, token_info,
                           token_mint, amount, sol_amount, signature=transaction.get("signature"))

                elif token_transfer.get("fromUserAccount") == wallet_address:
                    token_info = get_token_info(token_mint, codex_api_key)
                    notify(alerts_config, "SOLD", wallet_name, wallet_address, token_info,
                           token_mint, amount, sol_amount, signature=transaction.get("signature"))

    return latest_slot


def run_tasks_concurrently(wallets, helius_api_key, codex_api_key, alerts_config):
    """Run monitoring tasks concurrently for all wallets with a small delay between task starts."""

    def execute_with_delay(wallet):
        time.sleep(2)
        execute_monitoring(wallet, helius_api_key, codex_api_key, alerts_config)

    # Each wallet's monitoring loop runs forever (it's a `while True`), so it holds
    # its worker thread for the lifetime of the process rather than returning it to
    # the pool. A fixed cap here (e.g. 5) silently starves any wallets beyond that
    # count - they'd sit queued forever waiting for a thread that never frees up.
    # The pool size must scale with the wallet count, not be capped.
    with ThreadPoolExecutor(max_workers=len(wallets) or 1) as executor:
        futures = [executor.submit(execute_with_delay, wallet) for wallet in wallets]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                click.echo(f"An error occurred: {e}")