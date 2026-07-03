"""
In-process mock of a Keycloak token endpoint + a FirecREST v2 API server, used to test
the `firecrest` automatic allocation backend.

Unlike the PBS/Slurm mocks, which replace scheduler binaries (`qsub`, `sbatch`, ...),
the firecrest backend talks to an HTTP API, so this mock runs a real HTTP server on an
ephemeral localhost port inside the pytest process.

Mocked endpoints (response shapes follow the firecrest-v2 sources):
    POST   /token                       -> {"access_token", "expires_in"}
    POST   /compute/{system}/jobs       -> {"jobId": "<n>"}
    GET    /compute/{system}/jobs       -> {"jobs": [...]}     (includes "jobId")
    GET    /compute/{system}/jobs/{id}  -> {"jobs": [<job>]} or 404
    DELETE /compute/{system}/jobs/{id}  -> 204 (job state -> CANCELLED) or 404
"""

import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional


class MockFirecrest:
    def __init__(self):
        self.jobs: Dict[str, dict] = {}
        # Submissions as dicts with "name", "working_directory" and "script" keys
        self.submitted: List[dict] = []
        self.token_counter = 0
        # "METHOD endpoint" -> number of requests
        self.counters: Dict[str, int] = {}
        self.deleted_jobs: List[str] = []
        self.fail_api: List[int] = []
        self.fail_token: List[int] = []
        self.token_expires_in = 300
        self.lock = threading.Lock()

        self.server: Optional[ThreadingHTTPServer] = None
        self.thread: Optional[threading.Thread] = None
        self.port: Optional[int] = None

    @property
    def api_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def token_url(self) -> str:
        return f"{self.api_url}/token"

    def set_job_state(self, job_id: str, state: str):
        with self.lock:
            job = self.jobs[job_id]
            job["state"] = state
            now = int(time.time())
            if state == "RUNNING":
                job["start"] = now
            elif state not in ("PENDING", "CONFIGURING"):
                job["start"] = job["start"] or now
                job["end"] = now

    def fail_next_api_request(self, code: int, count: int = 1):
        with self.lock:
            self.fail_api.extend([code] * count)

    def fail_next_token_request(self, code: int, count: int = 1):
        with self.lock:
            self.fail_token.extend([code] * count)

    def counter(self, key: str) -> int:
        with self.lock:
            return self.counters.get(key, 0)

    def __enter__(self) -> "MockFirecrest":
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass

            def reply(self, code: int, payload=None):
                body = json.dumps(payload).encode() if payload is not None else b""
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def count(self, key: str):
                with mock.lock:
                    mock.counters[key] = mock.counters.get(key, 0) + 1

            def check_auth(self) -> bool:
                auth = self.headers.get("Authorization", "")
                if not auth.startswith("Bearer mock-token-"):
                    self.reply(401, {"message": "missing or invalid bearer token"})
                    return False
                return True

            def injected_fault(self) -> bool:
                with mock.lock:
                    if mock.fail_api:
                        code = mock.fail_api.pop(0)
                        self.reply(code, {"message": f"injected failure {code}"})
                        return True
                return False

            def read_body(self) -> bytes:
                length = int(self.headers.get("Content-Length") or 0)
                return self.rfile.read(length) if length else b""

            def do_POST(self):
                body = self.read_body()

                if self.path == "/token":
                    self.count("POST /token")
                    with mock.lock:
                        if mock.fail_token:
                            code = mock.fail_token.pop(0)
                            self.reply(code, {"error": f"injected token failure {code}"})
                            return
                        mock.token_counter += 1
                        n = mock.token_counter
                        expires = mock.token_expires_in
                    if b"client_id=" not in body or b"client_secret=" not in body:
                        self.reply(401, {"error": "invalid_client"})
                        return
                    self.reply(200, {"access_token": f"mock-token-{n}", "expires_in": expires})
                    return

                if re.fullmatch(r"/compute/[^/]+/jobs", self.path):
                    self.count("POST /compute/jobs")
                    if not self.check_auth() or self.injected_fault():
                        return
                    payload = json.loads(body)["job"]
                    with mock.lock:
                        job_id = str(len(mock.jobs) + 1)
                        mock.jobs[job_id] = {"state": "PENDING", "start": None, "end": None}
                        mock.submitted.append(
                            {
                                "job_id": job_id,
                                "name": payload["name"],
                                "working_directory": payload["workingDirectory"],
                                "script": payload["script"],
                            }
                        )
                    self.reply(200, {"jobId": job_id})
                    return

                self.reply(404, {"message": "unknown endpoint"})

            def do_GET(self):
                if re.fullmatch(r"/compute/[^/]+/jobs", self.path):
                    self.count("GET /compute/jobs")
                    if not self.check_auth() or self.injected_fault():
                        return
                    with mock.lock:
                        jobs = [job_response(job_id, job) for (job_id, job) in mock.jobs.items()]
                    self.reply(200, {"jobs": jobs})
                    return

                m = re.fullmatch(r"/compute/[^/]+/jobs/([^/]+)", self.path)
                if m:
                    self.count("GET /compute/jobs/{id}")
                    if not self.check_auth() or self.injected_fault():
                        return
                    with mock.lock:
                        job = mock.jobs.get(m.group(1))
                        payload = {"jobs": [job_response(m.group(1), job)]} if job else None
                    if payload is None:
                        self.reply(404, {"message": "job not found"})
                    else:
                        self.reply(200, payload)
                    return

                self.reply(404, {"message": "unknown endpoint"})

            def do_DELETE(self):
                m = re.fullmatch(r"/compute/[^/]+/jobs/([^/]+)", self.path)
                if m:
                    self.count("DELETE /compute/jobs/{id}")
                    if not self.check_auth() or self.injected_fault():
                        return
                    with mock.lock:
                        job = mock.jobs.get(m.group(1))
                        if job is None:
                            self.reply(404, {"message": "job not found"})
                            return
                        job["state"] = "CANCELLED"
                        job["end"] = int(time.time())
                        mock.deleted_jobs.append(m.group(1))
                    self.reply(204)
                    return
                self.reply(404, {"message": "unknown endpoint"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def job_response(job_id: str, job: dict) -> dict:
    return {
        "jobId": job_id,
        "status": {"state": job["state"]},
        "time": {"start": job["start"], "end": job["end"]},
    }
