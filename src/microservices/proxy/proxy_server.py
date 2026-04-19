import http.server
import urllib.request
import urllib.error
import random
import json
import os

MONOLITH_BASE_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_BASE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
MOVIES_SERVICE_TRAFFIC_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"

class StranglerFigProxyHandler(http.server.BaseHTTPRequestHandler):
    def _resolve_target(self, path: str) -> str:
        if GRADUAL_MIGRATION and path.startswith("/api/movies"):
            if random.randint(1, 100) <= MOVIES_SERVICE_TRAFFIC_PERCENT:
                return MOVIES_SERVICE_BASE_URL
        return MONOLITH_BASE_URL

    def _proxy_request(self, method: str) -> None:
        target_url = f"{self._resolve_target(self.path)}{self.path}"

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else None

        req = urllib.request.Request(url=target_url, data=body, method=method)

        for name, value in self.headers.items():
            if name.lower() != "host":
                req.add_header(name, value)

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                response_body = response.read()
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in ('transfer-encoding', 'content-length', 'content-encoding'):
                        self.send_header(name, value)
                self.send_header("Content-Length", str(len(response_body)))
                self.end_headers()
                self.wfile.write(response_body)

        except urllib.error.HTTPError as e:
            error_body = e.read() if e.fp else b""
            self.send_response(e.code)
            for name, value in e.headers.items():
                if name.lower() not in ('transfer-encoding', 'content-length', 'content-encoding'):
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(error_body)))
            self.end_headers()
            self.wfile.write(error_body)

        except urllib.error.URLError as e:
            body = json.dumps({
                "error": "Bad Gateway",
                "message": f"Целевой сервис недоступен: {e.reason}",
                "target": target_url,
            }).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        except Exception as e:
            body = json.dumps({
                "error": "Internal Server Error",
                "message": str(e),
            }).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def do_GET(self):     self._proxy_request("GET")
    def do_POST(self):    self._proxy_request("POST")
    def do_PUT(self):     self._proxy_request("PUT")
    def do_PATCH(self):   self._proxy_request("PATCH")
    def do_DELETE(self):  self._proxy_request("DELETE")
    def do_HEAD(self):    self._proxy_request("HEAD")
    def do_OPTIONS(self): self._proxy_request("OPTIONS")

    def log_message(self, format, *args):
        pass

def main():
    port = int(os.environ.get('PORT', 8000))
    server = http.server.HTTPServer(("0.0.0.0", port), StranglerFigProxyHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()

if __name__ == "__main__":
    main()