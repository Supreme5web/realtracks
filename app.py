import os
import time
import threading

from flask import Flask, jsonify

from pumpfun_watcher import start_watcher_background, watcher_status

app = Flask(__name__)

START_TIME = time.time()

# Start the watcher once, when the module is first imported.
# IMPORTANT: only run this process with a single worker (see render start
# command) - each extra worker would spawn its own copy of the watcher and
# send duplicate alerts (and double the metered PumpPortal usage).
_watcher_thread = threading.Thread(target=start_watcher_background, daemon=True)
_watcher_thread.start()


@app.route("/")
@app.route("/health")
def health():
    """Ping this endpoint (e.g. from UptimeRobot) every ~10 minutes to stop
    Render's free tier from spinning the service down after 15 min idle."""
    healthy = watcher_status["connected"] and not watcher_status["last_error"]
    return jsonify(
        {
            "status": "ok" if healthy else "error",
            "uptime_seconds": round(time.time() - START_TIME),
            "connected": watcher_status["connected"],
            "tokens_seen": watcher_status["tokens_seen"],
            "tokens_tracking": watcher_status["tokens_tracking"],
            "alerts_sent": watcher_status["alerts_sent"],
            "last_error": watcher_status["last_error"],
        }
    ), (200 if healthy else 503)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
