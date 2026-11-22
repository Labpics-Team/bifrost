#!/usr/bin/env python3
"""An isolated, real Bifrost with SQLite and a controllable OpenAI provider.

python3 runtime-fixture.py --binary /absolute/path/to/tmp/bifrost-http
ADAPTIVE_RUNTIME_FIXTURE_URL=http://127.0.0.1:18862 enables the companion spec.
No production configuration is read, and only this process's child is restarted.
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Fixture:
    def __init__(self, args):
        self.args = args
        self.binary = Path(args.binary).resolve(strict=True)
        self.directory = tempfile.TemporaryDirectory(prefix="bifrost-adaptive-")
        self.app_dir = Path(self.directory.name)
        self.process = None
        self.log = None
        self.generation = 0
        self.failure = 429
        self.requests = deque(maxlen=100)
        self.lock = threading.Lock()
        upstream = f"http://127.0.0.1:{args.upstream_port}"
        config = {
            "disable_onboarding": True,
            "client": {"drop_excess_requests": False, "initial_pool_size": 50, "enable_logging": True, "enforce_auth_on_inference": False},
            "config_store": {"enabled": True, "type": "sqlite", "config": {"path": str(self.app_dir / "config.db")}},
            "logs_store": {"enabled": True, "type": "sqlite", "config": {"path": str(self.app_dir / "logs.db")}},
            "governance": {"auth_config": {"is_enabled": True, "admin_username": "adaptive-test", "admin_password": "adaptive-test-password"}},
            "framework": {"pricing": {"pricing_url": upstream + "/pricing.json", "model_parameters_url": upstream + "/model-parameters.json", "mcp_library_sync_interval": 0}},
            "providers": {
                "openai": {
                    "keys": [
                        {"id": "failure-key", "name": "Failure key", "value": "sk-adaptive-failure", "models": ["test-model", "unavailable-model"], "weight": 1},
                        {"id": "healthy-key", "name": "Healthy key", "value": "sk-adaptive-healthy", "models": ["test-model"], "weight": 1},
                    ],
                    "network_config": {"base_url": upstream, "default_request_timeout_in_seconds": 5, "max_retries": 0},
                },
                "backup": {
                    "keys": [{"id": "backup-key", "name": "Backup key", "value": "sk-adaptive-backup", "models": ["test-model"], "weight": 1}],
                    "custom_provider_config": {"base_provider_type": "openai"},
                    "network_config": {"base_url": upstream, "default_request_timeout_in_seconds": 5, "max_retries": 0},
                },
            },
        }
        (self.app_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")

    def start(self):
        self.generation += 1
        self.log = (self.app_dir / f"server-{self.generation}.log").open("w", encoding="utf-8")
        env = {key: os.environ[key] for key in ("PATH", "HOME", "TMPDIR") if key in os.environ}
        self.process = subprocess.Popen([str(self.binary), "--app-dir", str(self.app_dir), "--port", str(self.args.port), "--host", "127.0.0.1", "--log-level", "info"], env=env, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError((self.app_dir / f"server-{self.generation}.log").read_text()[-10000:])
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.args.port}/health", timeout=1) as response:
                    if response.status == 200:
                        return
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(0.1)
        raise RuntimeError("Local Bifrost did not become ready within 90 seconds")

    def stop(self, crash=False):
        if self.process and self.process.poll() is None:
            self.process.kill() if crash else self.process.terminate()
            try:
                self.process.wait(timeout=35)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log:
            self.log.close()

    def restart(self, crash=False):
        with self.lock:
            self.stop(crash)
            self.start()


def handler_for(fixture):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_json(self, value, status=200, headers=None):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/_fixture/status":
                return self.send_json({"fixture": "adaptive-routing", "generation": fixture.generation, "gateway": f"http://127.0.0.1:{fixture.args.port}", "requests": list(fixture.requests)})
            if self.path.endswith("/models"):
                return self.send_json({"object": "list", "data": [{"id": "test-model", "object": "model", "created": 1, "owned_by": "fixture"}]})
            if self.path in ("/pricing.json", "/model-parameters.json"):
                return self.send_json({"test-model": {"provider": "openai", "mode": "chat", "input_cost_per_token": 0.000001, "output_cost_per_token": 0.000002, "max_input_tokens": 4096, "max_output_tokens": 256}})
            self.send_json({})

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
            if self.path == "/_fixture/mode":
                failure = body.get("failure", 0)
                if failure not in (0, 401, 429, 503):
                    return self.send_json({"error": "unsupported test response"}, 400)
                fixture.failure = failure
                return self.send_json({"failure": fixture.failure})
            if self.path == "/_fixture/restart":
                try:
                    fixture.restart(crash=body.get("crash", False))
                    return self.send_json({"generation": fixture.generation})
                except Exception as error:
                    return self.send_json({"error": str(error)}, 500)
            key = self.headers.get("Authorization", "").removeprefix("Bearer sk-adaptive-")
            status = fixture.failure if key == "failure" else 0
            fixture.requests.append({"key": key, "status": status or 200, "stream": bool(body.get("stream"))})
            if status:
                return self.send_json({"error": {"message": "Controlled provider refusal", "type": "rate_limit_error" if status == 429 else "server_error", "code": "fixture_refusal"}}, status, {"Retry-After": "120"})
            response = {"id": "chatcmpl-fixture", "created": int(time.time()), "model": body.get("model", "test-model")}
            if body.get("stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                for delta, finish in [({"role": "assistant", "content": "Fixture response"}, None), ({}, "stop")]:
                    data = {**response, "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
                    self.wfile.write(("data: " + json.dumps(data) + "\n\n").encode())
                    self.wfile.flush()
                self.wfile.write(b"data: [DONE]\n\n")
                return
            self.send_json({**response, "object": "chat.completion", "choices": [{"index": 0, "message": {"role": "assistant", "content": "Fixture response"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 2, "completion_tokens": 2, "total_tokens": 4}})
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--port", type=int, default=18861)
    parser.add_argument("--upstream-port", type=int, default=18862)
    args = parser.parse_args()
    for port in (args.port, args.upstream_port):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    fixture = Fixture(args)
    server = ThreadingHTTPServer(("127.0.0.1", args.upstream_port), handler_for(fixture))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    stopped = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopped.set())
    try:
        fixture.start()
        print(json.dumps({"ready": True, "gateway": f"http://127.0.0.1:{args.port}", "fixture": f"http://127.0.0.1:{args.upstream_port}", "app_dir": str(fixture.app_dir)}), flush=True)
        stopped.wait()
    finally:
        fixture.stop()
        server.shutdown()
        server.server_close()
        fixture.directory.cleanup()


if __name__ == "__main__":
    main()
