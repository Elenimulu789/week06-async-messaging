import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
import threading
import time
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from src.broker import Publisher
from src.store import KeyConflict, Store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOG = logging.getLogger(__name__)


class JobInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    title: str = Field(min_length=1, max_length=100)
    duration: int = Field(default=4, ge=3, le=5, strict=True)
    mode: Literal["normal", "retry_once", "poison"] = "normal"


def create_app(store=None, publisher=None, run_relay=True):
    store = store or Store(os.getenv("JOB_DB", "data/jobs.db"))
    store.init()
    publisher = publisher or Publisher(store)
    stop = threading.Event()

    def relay():
        while not stop.wait(1):
            for id_ in store.pending():
                if stop.is_set():
                    return
                try:
                    publisher.dispatch(id_)
                except Exception as error:
                    LOG.warning("OUTBOX_WAIT job=%s reason=%s", id_, type(error).__name__)
                    break

    @asynccontextmanager
    async def lifespan(_app):
        thread = threading.Thread(target=relay, daemon=True)
        if run_relay:
            thread.start()
        yield
        stop.set()
        if run_relay:
            thread.join(timeout=7)

    app = FastAPI(title="Week 06 • Async Report Jobs", lifespan=lifespan)
    app.state.store = store

    @app.middleware("http")
    async def timing(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Response-Time-Ms"] = f"{(time.perf_counter()-start)*1000:.2f}"
        return response

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(Path(__file__).parent / "static" / "index.html")

    @app.get("/architecture", include_in_schema=False)
    def architecture():
        return FileResponse(Path(__file__).parent / "static" / "architecture.svg",
                            media_type="image/svg+xml")

    @app.get("/healthz")
    def health():
        return {"status": "ok", "scope": "API process; not broker readiness"}

    @app.post("/api/jobs", status_code=202)
    def submit(body: JobInput, idempotency_key: str | None = Header(
            default=None, min_length=1, max_length=128)):
        try:
            id_ = store.create(body.model_dump(), idempotency_key or str(uuid4()))
        except KeyConflict:
            raise HTTPException(409, "Idempotency key already used with a different payload.")
        try:
            publisher.dispatch(id_)
        except Exception as error:
            LOG.warning("PUBLISH_PENDING job=%s reason=%s", id_, type(error).__name__)
            return JSONResponse(status_code=503, content={
                "detail": "Broker confirmation unavailable. Saved for retry; check this job ID.",
                "id": id_, "status_url": f"/api/jobs/{id_}"},
                headers={"Retry-After": "2", "Location": f"/api/jobs/{id_}"})
        job = store.get(id_)
        return JSONResponse(status_code=202, content={
            "id": id_, "status": job["status"], "status_url": f"/api/jobs/{id_}"},
            headers={"Location": f"/api/jobs/{id_}"})

    @app.get("/api/jobs")
    def jobs():
        return store.recent()

    @app.get("/api/jobs/{id_}")
    def status(id_: str):
        job = store.get(id_)
        if not job:
            raise HTTPException(404, "Job not found.")
        return job

    return app
