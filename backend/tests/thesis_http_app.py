"""Local browser-test harness: real thesis routes, read-only synthetic analytics.

Never a production entry point. Explicit fixture mode and a local fixture origin
are required; every non-thesis mutation is rejected. No schema creation on startup.
"""
import os
from urllib.parse import urlsplit

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
import requests

from backend.api.routes.portfolio_theses import router

if os.getenv("CEIORA_THESIS_TEST_MODE") != "1":
    raise RuntimeError("This is only a local browser-test harness")
origin = os.environ["CEIORA_THESIS_TEST_FIXTURE_ORIGIN"]
if urlsplit(origin).hostname not in {"127.0.0.1", "localhost", "host.docker.internal"}:
    raise RuntimeError("Fixture origin must be local")
app = FastAPI()
app.include_router(router, prefix="/api")


@app.get("/test-health")
def health():
    return {"local_test": True}


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def fixture(path: str, request: Request):
    if request.method != "GET" or path.startswith("portfolio/thes") or path == "portfolio/manual-order":
        raise HTTPException(405, "Non-thesis writes are disabled in this test harness")
    session = requests.Session()
    session.trust_env = False
    upstream = session.get(f"{origin}/api/{path}", params=request.query_params, headers={"Host": "127.0.0.1:8100"}, timeout=10)
    return Response(upstream.content, status_code=upstream.status_code, media_type="application/json")
