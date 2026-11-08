"""Thin HTTP layer over the engine (standard library only)."""

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .engine import Engine, Response

log = logging.getLogger(__name__)


def encode(resp: Response) -> tuple[bytes, str]:
    if isinstance(resp.body, str):
        return resp.body.encode("utf-8", "backslashreplace"), "text/plain; charset=utf-8"
    # backslashreplace: a lone surrogate (valid in JSON, not in UTF-8) goes out as its JSON escape (\ud83d)
    return json.dumps(resp.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "backslashreplace"), \
        "application/json"


def make_server(engine: Engine, host: str, port: int, stop: threading.Event) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            url = urlsplit(self.path)
            resp = engine.answer(url.path, parse_qs(url.query, keep_blank_values=True))
            if resp.delay_s and stop.wait(resp.delay_s):
                return  # stopping: drop the delayed answer
            body, content_type = encode(resp)
            self.send_response(resp.status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            for name, value in resp.headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            log.debug("%s %s", self.address_string(), fmt % args)

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server
