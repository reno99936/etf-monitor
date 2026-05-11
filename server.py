#!/usr/bin/env python3
"""ETF 監控本機 Server"""

from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import os
import subprocess
import sys
import threading
from datetime import datetime
import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_status = {"running": False, "log": [], "started_at": None, "finished_at": None}
_lock = threading.Lock()


def _start_fetch():
    with _lock:
        if _status["running"]:
            return False
        _status.update(running=True, log=["[server] 啟動 fetch_data.py ..."],
                       started_at=datetime.now(TAIPEI_TZ).isoformat(), finished_at=None)

    def _run():
        try:
            proc = subprocess.Popen(
                [sys.executable, "fetch_data.py"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", cwd=BASE_DIR,
                env={**os.environ, "PYTHONUTF8": "1"},
            )
            for line in proc.stdout:
                with _lock:
                    _status["log"].append(line.rstrip())
            proc.wait()
        except Exception as e:
            with _lock:
                _status["log"].append(f"[server] 錯誤: {e}")
        finally:
            with _lock:
                _status["running"] = False
                _status["finished_at"] = datetime.now(TAIPEI_TZ).isoformat()

    threading.Thread(target=_run, daemon=True).start()
    return True


def _send_telegram(text: str) -> bool:
    import urllib.request
    cfg_path = os.path.join(BASE_DIR, "data", "config.json")
    try:
        with open(cfg_path, encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return False
    token = cfg.get("tg_token", "")
    chat  = cfg.get("tg_chat",  "")
    if not token or not chat:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat, "text": text, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()).get("ok", False)
    except Exception:
        return False


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def log_message(self, fmt, *args):
        pass  # 靜音 access log

    def do_GET(self):
        if self.path.startswith("/api/status"):
            self._json(dict(_status, log=_status["log"][-50:]))
        else:
            super().do_GET()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if self.path == "/api/update":
            started = _start_fetch()
            self._json({"status": "started" if started else "already_running"})

        elif self.path == "/api/send-telegram":
            ok = _send_telegram(body.get("text", ""))
            self._json({"ok": ok})

        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, data):
        payload = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(payload))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    port = 8080
    server = HTTPServer(("127.0.0.1", port), Handler)
    print(f"ETF 監控 Server：http://localhost:{port}")
    print("Ctrl+C 停止")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
