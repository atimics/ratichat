"""Save explorer keys in Fly through a private, short-lived loopback form."""

import html
import re
import secrets
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

APP = "ratichat-bot-prod"
KEY_NAMES = ("TRONGRID_API_KEY", "BLOCKSCOUT_API_KEY")


def parse_keys(body):
    if len(body) > 4096:
        raise ValueError("Use the two explorer key fields.")
    values = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=2)
    if set(values) - set(KEY_NAMES) or any(len(v) != 1 for v in values.values()):
        raise ValueError("Use the two explorer key fields once each.")
    keys = {name: values[name][0].strip() for name in KEY_NAMES if values.get(name, [""])[0].strip()}
    if not keys or any(not re.fullmatch(r"[A-Za-z0-9._-]{20,512}", value) for value in keys.values()):
        raise ValueError("Paste a complete explorer API key.")
    return keys


def request_allowed(method, path, headers, token, port):
    origin = f"http://127.0.0.1:{port}"
    return (path == "/" + token and headers.get("Host") == f"127.0.0.1:{port}"
            and (method == "GET" or headers.get("Origin") == origin))


def import_keys(keys):
    payload = "".join(name + "=" + value + "\n" for name, value in keys.items())
    # The pipe carries key values. Command arguments and output contain no values.
    result = subprocess.run(["fly", "secrets", "import", "--app", APP, "--stage"],
                            input=payload, text=True, capture_output=True, timeout=120)
    return result.returncode == 0


def form_page(token):
    fields = "".join(f'<label>{name}<input type="password" name="{name}" autocomplete="off"></label>' for name in KEY_NAMES)
    return ('<!doctype html><meta charset="utf-8"><title>RatiChat explorer keys</title>'
            '<style>body{font:18px system-ui;max-width:650px;margin:60px auto;padding:20px}label{display:block;margin:24px 0}input{display:block;width:100%;padding:10px;box-sizing:border-box}button{padding:12px}</style>'
            '<h1>Save explorer keys in Fly</h1><p>App: ' + APP + '</p>'
            '<p>Paste either or both keys. Fly will store them as staged secrets. The next deployment applies them.</p>'
            '<form method="post" action="/' + html.escape(token, quote=True) + '">' + fields
            + '<button>Save in Fly</button></form>')


def handler_for(token):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(15)

        def log_message(self, *args):
            pass

        def reply(self, status, body):
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            # Same-origin forms retain their Origin header. Other destinations
            # receive no referrer, so the private path stays on this computer.
            self.send_header("Referrer-Policy", "same-origin")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def authorized(self):
            return request_allowed(self.command, self.path, self.headers, token, self.server.server_port)

        def do_GET(self):
            if not self.authorized():
                return self.reply(403, "Open the private setup link.")
            self.reply(200, form_page(token))

        def do_POST(self):
            if not self.authorized():
                return self.reply(403, "Open the private setup link.")
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 4096 or self.headers.get("Content-Type") != "application/x-www-form-urlencoded":
                    raise ValueError("Use the setup form.")
                keys = parse_keys(self.rfile.read(length))
            except (ValueError, UnicodeError):
                return self.reply(400, "Paste complete keys in the setup form.")
            with self.server.import_lock:
                if self.server.saved:
                    return self.reply(409, "The keys are already saved.")
                try:
                    saved = import_keys(keys)
                except (OSError, subprocess.TimeoutExpired):
                    saved = False
                if not saved:
                    return self.reply(502, "Fly needs another attempt. Return to the setup form.")
                self.server.saved = True
            self.reply(200, "<!doctype html><title>Keys saved</title><h1>Keys saved in Fly</h1><p>The next deployment applies them.</p>")
            print("Saved secret names: " + ", ".join(keys), flush=True)
            threading.Thread(target=self.server.shutdown, daemon=True).start()
    return Handler


def main():
    token = secrets.token_urlsafe(32)
    with ThreadingHTTPServer(("127.0.0.1", 0), handler_for(token)) as server:
        server.import_lock = threading.Lock()
        server.saved = False
        server.timeout = 20
        timer = threading.Timer(900, server.shutdown)
        timer.daemon = True
        timer.start()
        print(f"Private setup URL: http://127.0.0.1:{server.server_port}/{token}", flush=True)
        try:
            server.serve_forever()
        finally:
            timer.cancel()


if __name__ == "__main__":
    main()
