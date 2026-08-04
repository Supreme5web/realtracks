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
        if _sol_price_cache["price"] is not None and now - _sol_price_cache["ts"] < 60:
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


def get_wallet_token_balance(wallet_address, token_mint, helius_api_key):
    """
    Current balance of `token_mint` held by `wallet_address`, via Helius's Wallet
    Balances API (v1). Returns 0.0 if the wallet holds none (zero balances aren't
    returned by default, so "not found" means zero), or None if the lookup itself
    failed. Note: each call costs 100 Helius credits.
    """
    if not helius_api_key:
        return None
    try:
        resp = requests.get(
            f"https://api.helius.xyz/v1/wallet/{wallet_address}/balances",
            params={"api-key": helius_api_key, "showNative": "false"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        for token in data.get("balances", []):
            if token.get("mint") == token_mint:
                # `balance` is already decimal-adjusted (human-readable), no /10**decimals needed
                return float(token.get("balance") or 0.0)
        return 0.0
    except Exception as e:
        click.echo(f"Balance lookup failed for {wallet_address[:6]}...: {e}")
        return None


def estimate_supply(token_info):
    """Approximate circulating supply as marketCap / priceUSD (consistent with the MC already shown)."""
    try:
        price = float(token_info.get("price_usd") or 0)
        mc = float(token_info.get("market_cap") or 0)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return mc / price


def compute_holdings_pct(balance, supply):
    if balance is None or not supply:
        return None
    try:
        return (balance / supply) * 100
    except ZeroDivisionError:
        return None


def compute_pct_sold(units_sold, post_balance):
    """% of pre-sale holdings that this sell represents, derived from the current
    (post-sale) on-chain balance, so we don't need a separate balance snapshot."""
    if post_balance is None:
        return None
    pre_balance = post_balance + units_sold
    if pre_balance <= 0:
        return None
    return (units_sold / pre_balance) * 100


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


# Mints that show up as intermediate routing hops in multi-hop swaps (e.g. a
# Jupiter route that goes Token -> USDC -> Token) rather than as a token the
# person actually chose to buy or sell. We never alert on these directly.
IGNORED_MINTS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "So11111111111111111111111111111111111111112",  # Wrapped SOL
}

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
                       usd_value, token_mint, holdings, holdings_pct):
    safe_wallet_name = escape_markdown(wallet_name)
    safe_ticker = escape_markdown(token_info["ticker"])
    cap = format_compact_number(token_info["market_cap"])
    token_link = f"https://solscan.io/token/{token_mint}"
    holds_str = format_compact_number(holdings) if holdings is not None else "N/A"
    holds_pct_str = f"{holdings_pct:.2f}%" if holdings_pct is not None else "N/A"

    return (
        f"\U0001F7E2 *BUY ALERT*\n\n"
        f"*{safe_wallet_name}* swapped {format_sol(sol_amount)} SOL \u2192 "
        f"{format_amount(token_amount)} ({format_usd(usd_value)}) "
        f"[#{safe_ticker}]({token_link}) at {cap} MC\n\n"
        f"Holds: {holds_str} ({holds_pct_str})\n"
        f"CA: `{token_mint}`\n"
        f"Wallet: `{wallet_address}`"
    )


def build_sell_message(wallet_name, wallet_address, token_info, token_amount, sol_amount,
                        usd_value, token_mint, holdings, holdings_pct, pct_sold,
                        pnl_usd, pnl_pct):
    safe_wallet_name = escape_markdown(wallet_name)
    safe_ticker = escape_markdown(token_info["ticker"])
    cap = format_compact_number(token_info["market_cap"])
    token_link = f"https://solscan.io/token/{token_mint}"
    holds_str = format_compact_number(holdings) if holdings is not None else "N/A"
    holds_pct_str = f"{holdings_pct:.2f}%" if holdings_pct is not None else "N/A"
    pct_sold_str = f"{pct_sold:.0f}% sold" if pct_sold is not None else "sold"

    lines = [
        f"\U0001F534 *SELL ALERT*",
        "",
        f"*{safe_wallet_name}* swapped {format_amount(token_amount)} ({format_usd(usd_value)}) "
        f"[#{safe_ticker}]({token_link}) \u2192 {format_sol(sol_amount)} SOL ({pct_sold_str}) at {cap} MC",
        "",
    ]
    if pnl_usd is not None and pnl_pct is not None:
        lines.append(f"PnL: {format_usd(pnl_usd, signed=True)} ({format_signed_pct(pnl_pct)})")
    lines.append(f"Holds: {holds_str} ({holds_pct_str})")
    lines.append(f"CA: `{token_mint}`")
    lines.append(f"Wallet: `{wallet_address}`")
    return "\n".join(lines)


def send_telegram_notification(bot_token, chat_id, message):
    """Send a message to a Telegram chat via the Bot API."""
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
                               coin_ticker, holdings, holdings_pct, pct_sold=None,
                               pnl_usd=None, pnl_pct=None):
    """Send a Discord notification with a formatted embed. (Optional, only used if a Discord webhook is configured.)"""
    if not webhook_url:
        return
    try:
        embed_color = 65280 if action == "BOUGHT" else 16711680  # Green for BOUGHT, Red for SOLD
        fields = [
            {"name": "**Token**", "value": f'${coin_ticker} - {coin_name}', "inline": True},
            {"name": "**Market Cap**", "value": f'${format_compact_number(coin_cap)}', "inline": False},
            {"name": "**Amount**", "value": f"{format_amount(token_amount)} ({format_usd(usd_value)})", "inline": False},
            {"name": "**SOL**", "value": f"{format_sol(sol_amount)} SOL" + (f" ({pct_sold:.0f}% sold)" if pct_sold is not None else ""), "inline": False},
        ]
        if pnl_usd is not None and pnl_pct is not None:
            fields.append({"name": "**PnL**", "value": f"{format_usd(pnl_usd, signed=True)} ({format_signed_pct(pnl_pct)})", "inline": False})
        fields.append({
            "name": "**Holds**",
            "value": f"{format_compact_number(holdings) if holdings is not None else 'N/A'} "
                     f"({holdings_pct:.2f}%)" if holdings_pct is not None else "N/A",
            "inline": False,
        })
        fields.append({"name": "**Token Mint**", "value": token_mint, "inline": False})
        fields.append({"name": "**Wallet**", "value": wallet_address, "inline": False})

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
           token_amount, sol_amount, helius_api_key):
    """
    Compute USD value / holdings % / PnL for a buy or sell, update the cost-basis
    tracker, and fan the alert out to whichever channels are configured.
    """
    telegram_bot_token = alerts_config.get("telegram_bot_token")
    telegram_chat_id = alerts_config.get("telegram_chat_id")
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

    balance = get_wallet_token_balance(wallet_address, token_mint, helius_api_key)
    supply = estimate_supply(token_info)
    holdings_pct = compute_holdings_pct(balance, supply)

    pct_sold = None
    pnl_usd = None
    pnl_pct = None

    if action == "BOUGHT":
        if sol_usd_value is not None:
            record_buy(wallet_address, token_mint, token_amount, sol_usd_value)
    elif action == "SOLD":
        pct_sold = compute_pct_sold(token_amount, balance)
        pnl_usd, pnl_pct = record_sell(wallet_address, token_mint, token_amount, sol_usd_value)

    if telegram_bot_token and telegram_chat_id:
        if action == "BOUGHT":
            message = build_buy_message(
                wallet_name, wallet_address, token_info, token_amount, sol_amount,
                usd_value, token_mint, balance, holdings_pct,
            )
        else:
            message = build_sell_message(
                wallet_name, wallet_address, token_info, token_amount, sol_amount,
                usd_value, token_mint, balance, holdings_pct, pct_sold, pnl_usd, pnl_pct,
            )
        send_telegram_notification(telegram_bot_token, telegram_chat_id, message)

    if discord_webhook:
        send_discord_notification(
            discord_webhook, wallet_name, wallet_address, action, token_mint,
            token_amount, sol_amount, usd_value, token_info["market_cap"], token_info["name"],
            token_info["ticker"], balance, holdings_pct, pct_sold, pnl_usd, pnl_pct,
        )


def execute_monitoring(wallet, helius_key, codex_api_key, alerts_config):
    wallet_name = wallet["name"]
    wallet_address = wallet["address"]

    if wallet_address not in last_processed_slots:
        last_processed_slots[wallet_address] = None

    while True:
        base_url = f"https://api.helius.xyz/v0/addresses/{wallet_address}/transactions"
        params = {
            "api-key": helius_key,
            "types": ["TRANSFER", "SWAP"],
            "limit": 5,
        }

        try:
            response = requests.get(base_url, params=params, timeout=15)
            response.raise_for_status()
            transactions = response.json()

            if transactions:
                last_processed_slots[wallet_address] = process_transactions(
                    wallet, transactions, codex_api_key, alerts_config, last_processed_slots[wallet_address], helius_key
                )
            else:
                click.echo(f"No new transactions for {wallet_name} ({wallet_address}).")

            time.sleep(30)
        except requests.exceptions.RequestException as e:
            click.echo(f"Error fetching transactions for {wallet_name} - {wallet_address}: {e}")
            time.sleep(60)


def process_transactions(wallet, transactions, codex_api_key, alerts_config, last_processed_slot, helius_api_key):
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

        sol_amount = round(
            next(
                (abs(account["nativeBalanceChange"]) / 1e9 for account in transaction.get("accountData", []) if account["account"] == wallet_address),
                0.0,
            ),
            4,
        )

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
                           token_mint, amount, sol_amount, helius_api_key)
                elif leg.get("toUserAccount") == wallet_address:
                    notify(alerts_config, "BOUGHT", wallet_name, wallet_address, token_info,
                           token_mint, amount, sol_amount, helius_api_key)
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
            telegram_chat_id = alerts_config.get("telegram_chat_id")
            if telegram_bot_token and telegram_chat_id:
                send_telegram_notification(telegram_bot_token, telegram_chat_id, message)

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
                           token_mint, amount, sol_amount, helius_api_key)

                elif token_transfer.get("fromUserAccount") == wallet_address:
                    token_info = get_token_info(token_mint, codex_api_key)
                    notify(alerts_config, "SOLD", wallet_name, wallet_address, token_info,
                           token_mint, amount, sol_amount, helius_api_key)

    return latest_slot


def run_tasks_concurrently(wallets, helius_api_key, codex_api_key, alerts_config):
    """Run monitoring tasks concurrently for all wallets with a small delay between task starts."""

    def execute_with_delay(wallet):
        time.sleep(2)
        execute_monitoring(wallet, helius_api_key, codex_api_key, alerts_config)

    with ThreadPoolExecutor(max_workers=min(len(wallets), 5) or 1) as executor:
        futures = [executor.submit(execute_with_delay, wallet) for wallet in wallets]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                click.echo(f"An error occurred: {e}")