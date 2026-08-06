import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import click

# Timestamp (unix seconds) of the last swap event we've processed per tracked
# wallet, used as the lower bound of the next Codex query window so we only
# ever fetch new events since the last poll.
last_processed_timestamp = {}

# Weighted-average cost basis per (wallet, token), used to compute realized
# PnL on sells. Lives only in memory - resets if the process restarts, so
# PnL on a token won't be accurate until we've seen at least one buy of it
# since the last deploy.
_cost_basis = {}
_cost_basis_lock = threading.Lock()

# How often each wallet's monitoring loop checks Codex for new swap events.
# Much cheaper than the old QuickNode-polling design (one Codex query per
# wallet per poll, typically, vs. a signatures call plus one getTransaction
# call per signature), so this can run tighter than the old 30s interval.
POLL_INTERVAL_SECONDS = 15


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


WSOL_MINT = "So11111111111111111111111111111111111111112"

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

# Fetches this wallet's swap events directly from Codex, already classified
# as Buy/Sell with USD values computed - no separate raw-RPC fetch or
# manual pre/post balance diffing needed. `timestamp` bounds the query to
# only events since the last poll; ASC direction + cursor pagination lets us
# walk forward through a busy window without missing anything.
CODEX_MAKER_EVENTS_QUERY = """
query GetMakerEvents($maker: String!, $from: Int!, $to: Int!, $cursor: String) {
  getTokenEventsForMaker(
    query: {maker: $maker, eventType: Swap, timestamp: {from: $from, to: $to}}
    direction: ASC
    limit: 200
    cursor: $cursor
  ) {
    items {
      eventDisplayType
      timestamp
      transactionHash
      token0Address
      token1Address
      data {
        ... on SwapEventData {
          amount0
          amount1
          priceUsdTotal
        }
      }
    }
    cursor
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
        # marketCap is fully-diluted; fall back to circulatingMarketCap if unset.
        # Codex returns these as strings over GraphQL, so cast to float here -
        # otherwise downstream arithmetic (e.g. sol_amount * price_usd) blows up
        # with "can't multiply sequence by non-int of type 'float'".
        try:
            market_cap = float(result.get("marketCap") or result.get("circulatingMarketCap") or 0)
        except (TypeError, ValueError):
            market_cap = 0

        try:
            price_usd = float(result.get("priceUSD") or 0)
        except (TypeError, ValueError):
            price_usd = 0

        return {
            "market_cap": market_cap,
            "name": token.get("name") or "Unknown",
            "ticker": token.get("symbol") or "N/A",
            "price_usd": price_usd,
        }
    except Exception as e:
        click.echo(f"Codex lookup failed for {token_mint}: {e}")
        return defaults


def get_wallet_swap_events(wallet_address, from_ts, to_ts, codex_api_key, max_pages=25):
    """
    Fetch every Swap event for this wallet in [from_ts, to_ts] from Codex,
    paginating with `cursor` until Codex reports no more pages. Raises on
    any failure (missing key, HTTP error, GraphQL error) so the caller's
    existing retry/backoff logic handles it the same way a failed RPC call
    used to.
    """
    if not codex_api_key:
        raise RuntimeError("CODEX_API_KEY not set")

    events = []
    cursor = None
    for _ in range(max_pages):
        resp = requests.post(
            CODEX_GRAPHQL_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": codex_api_key,
            },
            json={
                "query": CODEX_MAKER_EVENTS_QUERY,
                "variables": {"maker": wallet_address, "from": from_ts, "to": to_ts, "cursor": cursor},
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            raise RuntimeError(f"Codex getTokenEventsForMaker error: {data['errors']}")

        payload = (data.get("data") or {}).get("getTokenEventsForMaker") or {}
        items = payload.get("items") or []
        events.extend(items)

        cursor = payload.get("cursor")
        if not cursor or not items:
            break
    else:
        click.echo(
            f"[SolTracker] hit the {max_pages}-page cap fetching Codex swap events for "
            f"{wallet_address} in window [{from_ts}, {to_ts}] - wallet may be extremely "
            f"active this window"
        )

    return events


def format_market_cap(value):
    """Compact, trimmed market cap for the Telegram alert layout, e.g. 2800000 -> '$2.8M'."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_000_000_000:
        num, suffix = value / 1_000_000_000, "B"
    elif value >= 1_000_000:
        num, suffix = value / 1_000_000, "M"
    elif value >= 1_000:
        num, suffix = value / 1_000, "K"
    else:
        num, suffix = value, ""
    s = f"{num:.2f}".rstrip("0").rstrip(".")
    return f"${sign}{s}{suffix}"


def _trim_trailing_zero_decimals(formatted_amount):
    """'31,111,261.00' -> '31,111,261', but '31,111,261.42' is left alone."""
    return formatted_amount[:-3] if formatted_amount.endswith(".00") else formatted_amount


def build_buy_message(wallet_name, wallet_address, token_info, token_amount, sol_amount,
                       usd_value, token_mint):
    safe_wallet_name = escape_markdown(wallet_name)
    safe_ticker = escape_markdown(token_info["ticker"])
    cap = format_market_cap(token_info["market_cap"])
    amount = _trim_trailing_zero_decimals(format_amount(token_amount))

    return (
        f"\U0001F7E2 *BUY*\n"
        f"\n"
        f"*{safe_wallet_name}* bought {amount} *{safe_ticker}*\n"
        f"*Spent:* {format_sol(sol_amount)} SOL ({format_usd(usd_value)})\n"
        f"*Entry:* {cap} *MC*\n"
        f"\n"
        f"*CA:* `{token_mint}`\n"
        f"*Wallet:* `{wallet_address}`"
    )


def build_sell_message(wallet_name, wallet_address, token_info, token_amount, sol_amount,
                        usd_value, token_mint, pnl_usd, pnl_pct):
    safe_wallet_name = escape_markdown(wallet_name)
    safe_ticker = escape_markdown(token_info["ticker"])
    cap = format_market_cap(token_info["market_cap"])
    amount = _trim_trailing_zero_decimals(format_amount(token_amount))

    lines = [
        f"\U0001F534 *SELL*",
        "",
        f"*{safe_wallet_name}* sold {amount} *{safe_ticker}*",
        f"*Received:* {format_sol(sol_amount)} SOL ({format_usd(usd_value)})",
    ]
    if pnl_usd is not None and pnl_pct is not None:
        lines.append(f"\U0001F4C8 PnL: {format_usd(pnl_usd, signed=True)} ({format_signed_pct(pnl_pct)})")
    lines.append(f"*MC:* {cap}")
    lines.append("")
    lines.append(f"*CA:* `{token_mint}`")
    lines.append(f"*Wallet:* `{wallet_address}`")
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
           token_amount, sol_amount, sol_usd_value, signature=None):
    """
    Compute holdings % / PnL for a buy or sell, update the cost-basis tracker,
    and fan the alert out to whichever channels are configured.

    sol_usd_value is the swap's total USD value as computed by Codex itself
    (SwapEventData.priceUsdTotal) - since Codex already prices the specific
    swap at the time it happened, there's no need for a separate SOL/USD
    price lookup or a cross-check against a second, independently-fetched
    price the way the old QuickNode-based version needed.
    """
    telegram_bot_token = alerts_config.get("telegram_bot_token")
    telegram_chat_ids = alerts_config.get("telegram_chat_ids") or []
    discord_webhook = alerts_config.get("discord_webhook")

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
                sol_usd_value, token_mint,
            )
        else:
            message = build_sell_message(
                wallet_name, wallet_address, token_info, token_amount, sol_amount,
                sol_usd_value, token_mint, pnl_usd, pnl_pct,
            )
        keyboard = build_trading_bot_keyboard(token_mint)
        for chat_id in telegram_chat_ids:
            send_telegram_notification(telegram_bot_token, chat_id, message, reply_markup=keyboard)

    if discord_webhook:
        send_discord_notification(
            discord_webhook, wallet_name, wallet_address, action, token_mint,
            token_amount, sol_amount, sol_usd_value, token_info["market_cap"], token_info["name"],
            token_info["ticker"], pnl_usd, pnl_pct,
        )


def process_swap_event(wallet, event, codex_api_key, alerts_config):
    """
    Turn one Codex Swap event into a BUY/SELL alert. Codex has already done
    the hard part (classifying Buy vs Sell, computing USD value) - this just
    picks out the SOL leg vs the traded-token leg and fires the alert.

    Only trades where one side of the pair is native/wrapped SOL are
    considered "a trade" here, matching the original tool's scope (alerts
    are always phrased as SOL spent/received). A token/token or
    token/stablecoin pair swap is skipped, same as before.

    Returns a short status string describing the outcome (for the caller to
    aggregate into a per-poll summary) rather than logging directly.
    """
    wallet_address = wallet["address"]
    wallet_name = wallet["name"]

    token0 = event.get("token0Address")
    token1 = event.get("token1Address")
    data = event.get("data") or {}

    if token0 == WSOL_MINT:
        sol_leg_raw, token_leg_raw = data.get("amount0"), data.get("amount1")
        token_mint = token1
    elif token1 == WSOL_MINT:
        sol_leg_raw, token_leg_raw = data.get("amount1"), data.get("amount0")
        token_mint = token0
    else:
        return "non_sol_pair"

    try:
        sol_amount = abs(float(sol_leg_raw))
        token_amount = abs(float(token_leg_raw))
    except (TypeError, ValueError):
        return "bad_amount"

    if not sol_amount or not token_amount:
        return "zero_amount"

    display_type = event.get("eventDisplayType")
    if display_type not in ("Buy", "Sell"):
        return "unknown_direction"

    try:
        raw_usd_total = data.get("priceUsdTotal")
        usd_value = abs(float(raw_usd_total)) if raw_usd_total is not None else None
    except (TypeError, ValueError):
        usd_value = None

    token_info = get_token_info(token_mint, codex_api_key)
    action = "BOUGHT" if display_type == "Buy" else "SOLD"
    notify(
        alerts_config, action, wallet_name, wallet_address, token_info, token_mint,
        token_amount, sol_amount, usd_value, signature=event.get("transactionHash"),
    )
    return "buy" if action == "BOUGHT" else "sell"


def execute_monitoring(wallet, codex_api_key, alerts_config):
    wallet_name = wallet["name"]
    wallet_address = wallet["address"]

    if wallet_address not in last_processed_timestamp:
        # First-ever poll for this wallet: start watching from "now" rather than
        # pulling historical trades, so a deploy/restart doesn't re-alert on old
        # activity that already happened before this process started.
        last_processed_timestamp[wallet_address] = int(time.time())
        click.echo(
            f"[SolTracker] {wallet_name} ({wallet_address}): starting fresh, "
            f"watching for new trades from now on"
        )

    while True:
        try:
            from_ts = last_processed_timestamp[wallet_address] + 1
            to_ts = int(time.time())

            if from_ts > to_ts:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            events = get_wallet_swap_events(wallet_address, from_ts, to_ts, codex_api_key)

            if events:
                click.echo(
                    f"[SolTracker] {wallet_name} ({wallet_address}): found {len(events)} "
                    f"new swap event(s) this poll"
                )
                outcome_counts = {}
                for event in events:
                    try:
                        outcome = process_swap_event(wallet, event, codex_api_key, alerts_config)
                        if outcome in ("buy", "sell"):
                            click.echo(
                                f"[SolTracker] {wallet_name} - {event.get('transactionHash')}: "
                                f"detected {'BUY' if outcome == 'buy' else 'SELL'}, sending alert"
                            )
                        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
                    except Exception as e:
                        # Don't let one bad/unparseable event abort the rest of the batch.
                        click.echo(
                            f"[SolTracker] {wallet_name} - {event.get('transactionHash')}: "
                            f"failed to process: {e}"
                        )
                        outcome_counts["exception"] = outcome_counts.get("exception", 0) + 1

                summary = ", ".join(f"{count} {label}" for label, count in sorted(outcome_counts.items()))
                click.echo(f"[SolTracker] {wallet_name} ({wallet_address}): poll summary - {summary}")

                # Advance to the newest event timestamp we actually saw, not to `to_ts`,
                # so we never skip an event that landed after our query fired but shares
                # `to_ts`'s second.
                last_processed_timestamp[wallet_address] = max(e["timestamp"] for e in events)
            else:
                click.echo(f"No new transactions for {wallet_name} ({wallet_address}).")
                last_processed_timestamp[wallet_address] = to_ts

            time.sleep(POLL_INTERVAL_SECONDS)
        except requests.exceptions.RequestException as e:
            body = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                try:
                    body = f" | body: {resp.text[:300]}"
                except Exception:
                    pass
            click.echo(f"Error fetching events for {wallet_name} - {wallet_address}: {e}{body}")
            time.sleep(60)
        except Exception as e:
            click.echo(f"Error processing events for {wallet_name} - {wallet_address}: {e}")
            time.sleep(60)


def run_tasks_concurrently(wallets, codex_api_key, alerts_config):
    """Run monitoring tasks concurrently for all wallets with a small delay between task starts."""

    def execute_with_delay(wallet):
        time.sleep(2)
        execute_monitoring(wallet, codex_api_key, alerts_config)

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
