from http.server import HTTPServer, BaseHTTPRequestHandler
import os
import random
import sys
import urllib.request
import urllib.error
import json
from urllib.parse import urljoin

def get_required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None:
        sys.exit(1)
    return value

def get_required_int_env(var_name: str) -> int:
    value_str = get_required_env(var_name)
    try:
        return int(value_str)
    except ValueError:
        sys.exit(1)

PORT = get_required_int_env("PORT")
MONOLITH_URL = get_required_env("MONOLITH_URL")
MOVIES_SERVICE_URL = get_required_env("MOVIES_SERVICE_URL")
GRADUAL_MIGRATION_RAW = os.getenv("GRADUAL_MIGRATION", "false").lower()
if GRADUAL_MIGRATION_RAW in ("true", "1", "yes", "on"):
    GRADUAL_MIGRATION = True
elif GRADUAL_MIGRATION_RAW in ("false", "0", "no", "off", ""):
    GRADUAL_MIGRATION = False
else:
    sys.exit(1)

if GRADUAL_MIGRATION:
    MOVIES_MIGRATION_PERCENT = get_required_int_env("MOVIES_MIGRATION_PERCENT")
    if not (0 <= MOVIES_MIGRATION_PERCENT <= 100):
        sys.exit(1)
else:
    MOVIES_MIGRATION_PERCENT = 0


def should_route_to_movies() -> bool:
    if not GRADUAL_MIGRATION:
        return False
    # Генерируем число от 1 до 100
    return random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT


def forward_request(target_base_url: str, path: str, method: str, headers: dict, body: bytes = None):
    url = urljoin(target_base_url.rstrip('/') + '/', path.lstrip('/'))

    req = urllib.request.Request(url, data=body, method=method)
    for key, value in headers.items():
        if key.lower() != 'host':
            req.add_header(key, value)

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            response_body = response.read()
            response_headers = dict(response.headers)
            return response.status, response_headers, response_body
    except urllib.error.HTTPError as e:
        response_body = e.read()
        response_headers = dict(e.headers)
        return e.code, response_headers, response_body
    except Exception as e:
        error_msg = f"Proxy error: {e}".encode('utf-8')
        return 502, {"Content-Type": "text/plain; charset=utf-8"}, error_msg


class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._proxy_request()

    def do_POST(self):
        self._proxy_request()

    def _proxy_request(self):
        path = self.path
        if path == "/api/proxy/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"status": True}, ensure_ascii=False).encode("utf-8"))
            return
        elif path.startswith("/api/movies"):
            if should_route_to_movies():
                target_url = MOVIES_SERVICE_URL
            else:
                target_url = MONOLITH_URL
        else:
            target_url = MONOLITH_URL

        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length > 0 else None

        status, resp_headers, resp_body = forward_request(
            target_base_url=target_url,
            path=path,
            method=self.command,
            headers=dict(self.headers),
            body=body
        )

        self.send_response(status)
        for key, value in resp_headers.items():
            if key.lower() not in ("connection", "transfer-encoding", "keep-alive"):
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(resp_body)

if __name__ == "__main__":
    host = "127.0.0.1" if os.name == "nt" else "0.0.0.0"
    server_address = (host, PORT)

    try:
        httpd = HTTPServer(server_address, ProxyHandler)
        print(f"Сервер запущен")
        httpd.serve_forever()
    except PermissionError:
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(0)