import os
import json
import time
import threading

from flask import Flask, jsonify

from task_executor import run_tasks_concurrently

app = Flask(__name__)

START_TIME = time.time()
tracker_status = {"started": False, "error": None, "wallet_count": 0}


def load_wallets_from_env():
    """
    WALLETS env var must be a JSON array like:
    [{"name": "Wallet 1", "address": "..."}, {"name": "Wallet 2", "address": "..."}]
    """
    raw = os.environ.get("WALLETS", "")
    if not raw:
        raise ValueError("WALLETS env var is not set")
    try:
        wallets = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"WALLETS env var is not valid JSON: {e}")
    if not isinstance(wallets, list) or not wallets:
        raise ValueError('WALLETS must be a non-empty JSON array of {"name": ..., "address": ...}')
    for w in wallets:
        if not isinstance(w, dict) or "name" not in w or "address" not in w:
            raise ValueError('Each wallet needs a "name" and "address" field')
    return wallets


def start_tracking_background():
    """Runs forever in a background thread. Never lets an exception kill the web process -
    on failure it records the error so /health can report it, and the web service stays up."""
    try:
        wallets = load_wallets_from_env()

        helius_api_key = os.environ.get("HELIUS_API_KEY")
        codex_api_key = os.environ.get("CODEX_API_KEY")
        if not helius_api_key:
            raise ValueError("HELIUS_API_KEY env var is not set")
        if not codex_api_key:
            raise ValueError("CODEX_API_KEY env var is not set")

        alerts_config = {
            "telegram_bot_token": os.environ.get("TELEGRAM_BOT_TOKEN"),
            # Supports one or more destinations, comma-separated, e.g.
            # "123456789,@mychannel" to alert both your own DM with the bot
            # and a channel. Whitespace around each entry is stripped.
            "telegram_chat_ids": [
                c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()
            ],
            "discord_webhook": os.environ.get("DISCORD_WEBHOOK"),
        }
        has_telegram = alerts_config["telegram_bot_token"] and alerts_config["telegram_chat_ids"]
        has_discord = alerts_config["discord_webhook"]
        if not has_telegram and not has_discord:
            raise ValueError(
                "Set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID, or DISCORD_WEBHOOK, as env vars"
            )

        tracker_status["wallet_count"] = len(wallets)
        tracker_status["started"] = True
        tracker_status["error"] = None

        print(f"[SolTracker] Starting tracking for {len(wallets)} wallet(s)", flush=True)
        run_tasks_concurrently(wallets, helius_api_key, codex_api_key, alerts_config)

    except Exception as e:
        tracker_status["started"] = False
        tracker_status["error"] = str(e)
        print(f"[SolTracker] Failed to start tracking: {e}", flush=True)


@app.route("/")
@app.route("/health")
def health():
    """Ping this endpoint (e.g. from UptimeRobot) every ~10 minutes to stop
    Render's free tier from spinning the service down after 15 min idle."""
    return jsonify(
        {
            "status": "ok" if tracker_status["started"] and not tracker_status["error"] else "error",
            "uptime_seconds": round(time.time() - START_TIME),
            "wallets_tracked": tracker_status["wallet_count"],
            "error": tracker_status["error"],
        }
    ), (200 if tracker_status["started"] else 503)


# Start the tracker once, when the module is first imported.
# IMPORTANT: only run this process with a single worker (see render start command) -
# each extra worker would spawn its own copy of the tracking loop and send duplicate alerts.
_tracker_thread = threading.Thread(target=start_tracking_background, daemon=True)
_tracker_thread.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))