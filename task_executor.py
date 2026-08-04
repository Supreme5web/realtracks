import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import click

# global dict for tracking last processed slot for the wallets being tracked
last_processed_slots = {}


def format_market_cap(value):
    """Format a market cap number as a compact string, e.g. 1234567 -> '1.23M'."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


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
    Look up market cap / name / ticker for a token via the Codex.io API.
    Never raises - returns safe defaults on any failure (missing key, bad
    response, no match, network error) so a single bad lookup can't crash
    the whole monitoring loop.
    """
    defaults = {"market_cap": 0, "name": "Unknown", "ticker": "N/A"}

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

        return {
            "market_cap": market_cap,
            "name": token.get("name") or "Unknown",
            "ticker": token.get("symbol") or "N/A",
        }
    except Exception as e:
        click.echo(f"Codex lookup failed for {token_mint}: {e}")
        return defaults


def build_alert_message(action, wallet_name, token_info, amount, sol_amount, signature):
    """Build a Telegram-formatted (Markdown) alert message for a buy or sell."""
    emoji = "\U0001F7E2" if action == "BOUGHT" else "\U0001F534"  # green/red circle
    sol_label = "SOL Spent" if action == "BOUGHT" else "SOL Received"
    cap = format_market_cap(token_info["market_cap"])
    safe_wallet_name = escape_markdown(wallet_name)
    safe_name = escape_markdown(token_info["name"])
    safe_ticker = escape_markdown(token_info["ticker"])
    return (
        f"{emoji} *{safe_wallet_name}* {action}\n\n"
        f"*Token:* {safe_name} (${safe_ticker})\n"
        f"*Market Cap:* ${cap}\n"
        f"*Amount:* {amount}\n"
        f"*{sol_label}:* {sol_amount} SOL\n"
        f"[View Transaction](https://solscan.io/tx/{signature})"
    )


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
    except requests.RequestException as e:
        # Avoid ever logging the bot token, which requests.RequestException's
        # str(e) includes as part of the request URL.
        click.echo("Error sending Telegram notification (request failed)")


def send_discord_notification(webhook_url, wallet_name, action, token_mint, token_amount, sol_amount, transaction_signature, coin_cap, coin_name, coin_ticker):
    """Send a Discord notification with a formatted embed. (Optional, only used if a Discord webhook is configured.)"""
    if not webhook_url:
        return
    try:
        embed_color = 65280 if action == "BOUGHT" else 16711680  # Green for BOUGHT, Red for SOLD
        embed = {
            "embeds": [
                {
                    "title": f"{wallet_name} - **{action}**",
                    "color": embed_color,
                    "fields": [
                        {"name": "**Token**", "value": f'${coin_ticker} - {coin_name}', "inline": True},
                        {"name": "**Market Cap**", "value": f'${format_market_cap(coin_cap)}', "inline": False},
                        {"name": "**Token Mint**", "value": token_mint, "inline": False},
                        {"name": "**Token Amount**", "value": str(token_amount), "inline": False},
                        {"name": "**Sol Amount**", "value": f"{sol_amount} SOL", "inline": False},
                        {"name": "**Transaction**", "value": f"[View Transaction](https://solscan.io/tx/{transaction_signature})", "inline": False},
                    ],
                    "footer": {"text": "Transaction Notification"},
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                }
            ]
        }
        response = requests.post(webhook_url, json=embed, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        click.echo(f"Error sending Discord notification: {e}")


def notify(alerts_config, action, wallet_name, token_info, amount, sol_amount, signature, token_mint):
    """Fan out a buy/sell alert to whichever channels are configured."""
    telegram_bot_token = alerts_config.get("telegram_bot_token")
    telegram_chat_id = alerts_config.get("telegram_chat_id")
    discord_webhook = alerts_config.get("discord_webhook")

    if telegram_bot_token and telegram_chat_id:
        message = build_alert_message(action, wallet_name, token_info, amount, sol_amount, signature)
        send_telegram_notification(telegram_bot_token, telegram_chat_id, message)

    if discord_webhook:
        send_discord_notification(
            discord_webhook, wallet_name, action, token_mint, amount, sol_amount,
            signature, token_info["market_cap"], token_info["name"], token_info["ticker"],
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
                    wallet, transactions, codex_api_key, alerts_config, last_processed_slots[wallet_address]
                )
            else:
                click.echo(f"No new transactions for {wallet_name} ({wallet_address}).")

            time.sleep(30)
        except requests.exceptions.RequestException as e:
            click.echo(f"Error fetching transactions for {wallet_name} - {wallet_address}: {e}")
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
            if len(token_transfers) >= 1:
                from_token = token_transfers[0]
                to_token = token_transfers[1] if len(token_transfers) > 1 else None

                if from_token.get("fromUserAccount") == wallet_address:
                    out_info = get_token_info(from_token["mint"], codex_api_key)
                    in_info = get_token_info(to_token["mint"], codex_api_key) if to_token else None
                    message = (
                        f"\U0001F501 *{escape_markdown(wallet_name)}* SWAPPED\n\n"
                        f"*Sold:* {from_token['tokenAmount']} {escape_markdown(out_info['ticker'])}\n"
                        f"*Bought:* {to_token['tokenAmount'] if to_token else 'N/A'} "
                        f"{escape_markdown(in_info['ticker']) if in_info else 'N/A'}\n"
                        f"[View Transaction](https://solscan.io/tx/{transaction['signature']})"
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

                if token_transfer.get("toUserAccount") == wallet_address:
                    token_info = get_token_info(token_mint, codex_api_key)
                    notify(alerts_config, "BOUGHT", wallet_name, token_info, amount, sol_amount, transaction["signature"], token_mint)

                elif token_transfer.get("fromUserAccount") == wallet_address:
                    token_info = get_token_info(token_mint, codex_api_key)
                    notify(alerts_config, "SOLD", wallet_name, token_info, amount, sol_amount, transaction["signature"], token_mint)

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