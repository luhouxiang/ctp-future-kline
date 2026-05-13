# -*- coding: utf-8 -*-
"""Falcon/uvicorn HTTP entry for the Python strategy runtime.

The strategy algorithms still live behind ``StrategyService``.  This module only
replaces the transport layer: Go sends JSON POST requests, market events are
queued with a push id, and Go polls the push id until a decision is available.
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import falcon.asgi
import uvicorn

from strategy_common import DEFAULT_STRATEGY_LOG_FILE, _configure_logging, _instance_id, _mode_key
from strategy_runtime_service import StrategyService
from strategy_types import RequestDict, ResponseDict, RuntimeKey

logger = logging.getLogger(__name__)

TASK_TTL_SECONDS = 10 * 60
WORKER_IDLE_SECONDS = 60


class PushIDNotFound(KeyError):
    """Raised when a push id is unknown or has expired."""


@dataclass
class AsyncTask:
    push_id: str
    key: RuntimeKey
    method_name: str
    request: RequestDict
    status: str = "queued"
    result: ResponseDict | None = None
    error: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    done: threading.Event = field(default_factory=threading.Event)


class AsyncStrategyRunner:
    """Runs market-event strategy calls asynchronously and serializes per runtime."""

    def __init__(self, service: StrategyService, ttl_seconds: int = TASK_TTL_SECONDS) -> None:
        self.service = service
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._lock = threading.RLock()
        self._tasks: dict[str, AsyncTask] = {}
        self._queues: dict[RuntimeKey, queue.Queue[AsyncTask]] = {}
        self._workers: dict[RuntimeKey, threading.Thread] = {}

    def enqueue(self, method_name: str, request: RequestDict) -> str:
        instance_id = _instance_id(request)
        if not instance_id:
            raise ValueError("missing required field(s): instance.instance_id")
        key: RuntimeKey = (_mode_key(request), instance_id)
        push_id = uuid.uuid4().hex
        task = AsyncTask(push_id=push_id, key=key, method_name=method_name, request=request)
        with self._lock:
            self._cleanup_locked(time.time())
            self._tasks[push_id] = task
            work_queue = self._queues.get(key)
            if work_queue is None:
                work_queue = queue.Queue()
                self._queues[key] = work_queue
            worker = self._workers.get(key)
            if worker is None or not worker.is_alive():
                worker = threading.Thread(target=self._worker_loop, args=(key, work_queue), daemon=True)
                self._workers[key] = worker
                worker.start()
            work_queue.put(task)
        # logger.info("strategy event queued: push_id=%s key=%s method=%s", push_id, key, method_name)
        return push_id

    def result(self, push_id: str, wait_ms: int = 0) -> dict[str, Any]:
        push_id = str(push_id or "").strip()
        if not push_id:
            raise ValueError("push_id is required")
        task = self._task(push_id)
        wait_seconds = max(0, min(int(wait_ms or 0), 5000)) / 1000.0
        if wait_seconds > 0 and not task.done.is_set():
            task.done.wait(wait_seconds)
        task = self._task(push_id)
        if task.status == "done":
            return {"push_id": push_id, "status": "done", "result": task.result or {}}
        if task.status == "error":
            return {"push_id": push_id, "status": "error", "error": task.error}
        return {"push_id": push_id, "status": task.status}

    def _task(self, push_id: str) -> AsyncTask:
        now = time.time()
        with self._lock:
            self._cleanup_locked(now)
            task = self._tasks.get(push_id)
            if task is None:
                raise PushIDNotFound(push_id)
            if task.done.is_set() and now - task.updated_at > self.ttl_seconds:
                del self._tasks[push_id]
                raise PushIDNotFound(push_id)
            return task

    def _worker_loop(self, key: RuntimeKey, work_queue: queue.Queue[AsyncTask]) -> None:
        while True:
            try:
                task = work_queue.get(timeout=WORKER_IDLE_SECONDS)
            except queue.Empty:
                with self._lock:
                    if work_queue.empty() and self._queues.get(key) is work_queue:
                        self._queues.pop(key, None)
                        self._workers.pop(key, None)
                        return
                continue
            self._run_task(task)
            work_queue.task_done()

    def _run_task(self, task: AsyncTask) -> None:
        with self._lock:
            task.status = "running"
            task.updated_at = time.time()
        try:
            fn = getattr(self.service, task.method_name)
            out = fn(task.request, None)
            with self._lock:
                task.status = "done"
                task.result = out or {}
                task.updated_at = time.time()
        except Exception as exc:
            logger.exception("strategy async task failed: push_id=%s method=%s", task.push_id, task.method_name)
            with self._lock:
                task.status = "error"
                task.error = str(exc)
                task.updated_at = time.time()
        finally:
            task.done.set()

    def _cleanup_locked(self, now: float) -> None:
        expired = [
            push_id for push_id, task in self._tasks.items()
            if task.done.is_set() and now - task.updated_at > self.ttl_seconds
        ]
        for push_id in expired:
            del self._tasks[push_id]


class StrategyHTTPResource:
    def __init__(self, service: StrategyService | None = None, runner: AsyncStrategyRunner | None = None) -> None:
        self.service = service or StrategyService()
        self.runner = runner or AsyncStrategyRunner(self.service)

    async def on_post_ping(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.Ping)

    async def on_post_list(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.ListStrategies)

    async def on_post_load(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.LoadStrategy)

    async def on_post_start_requirements(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.GetStartRequirements)

    async def on_post_start(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.StartInstance)

    async def on_post_stop(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.StopInstance)

    async def on_post_on_tick(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._enqueue(req, resp, "OnTick")

    async def on_post_on_bar(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._enqueue(req, resp, "OnBar")

    async def on_post_on_replay_bar(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._enqueue(req, resp, "OnReplayBar")

    async def on_post_result(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        try:
            body = await self._request_media(req)
            resp.media = self.runner.result(str(body.get("push_id") or ""), int(body.get("wait_ms") or 0))
        except PushIDNotFound:
            resp.status = falcon.HTTP_404
            resp.media = {"status": "expired", "error": "push_id not found or expired"}
        except ValueError as exc:
            resp.status = falcon.HTTP_400
            resp.media = {"status": "error", "error": str(exc)}

    async def on_post_backtest_run(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.RunBacktest)

    async def on_post_backtest_result(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.GetBacktestResult)

    async def on_post_optimizer_run_sweep(self, req: falcon.asgi.Request, resp: falcon.asgi.Response) -> None:
        await self._direct(req, resp, self.service.RunParameterSweep)

    async def _enqueue(self, req: falcon.asgi.Request, resp: falcon.asgi.Response, method_name: str) -> None:
        try:
            body = await self._request_media(req)
            push_id = self.runner.enqueue(method_name, body)
            resp.media = {"ok": True, "push_id": push_id, "status": "queued"}
        except ValueError as exc:
            resp.status = falcon.HTTP_400
            resp.media = {"ok": False, "status": "error", "error": str(exc)}

    async def _direct(self, req: falcon.asgi.Request, resp: falcon.asgi.Response, fn: Any) -> None:
        try:
            body = await self._request_media(req)
            resp.media = fn(body, None)
        except ValueError as exc:
            resp.status = falcon.HTTP_400
            resp.media = {"ok": False, "error": str(exc)}
        except Exception as exc:
            logger.exception("strategy http request failed: %s", getattr(fn, "__name__", fn))
            resp.status = falcon.HTTP_500
            resp.media = {"ok": False, "error": str(exc)}

    async def _request_media(self, req: falcon.asgi.Request) -> RequestDict:
        if not req.content_length:
            return {}
        media = await req.get_media()
        if media is None:
            return {}
        if not isinstance(media, dict):
            raise ValueError("json body must be an object")
        return media


def build_app(service: StrategyService | None = None, runner: AsyncStrategyRunner | None = None) -> falcon.asgi.App:
    app = falcon.asgi.App()
    resource = StrategyHTTPResource(service, runner)
    app.add_route("/health/ping", resource, suffix="ping")
    app.add_route("/registry/list", resource, suffix="list")
    app.add_route("/runtime/load", resource, suffix="load")
    app.add_route("/runtime/start-requirements", resource, suffix="start_requirements")
    app.add_route("/runtime/start", resource, suffix="start")
    app.add_route("/runtime/stop", resource, suffix="stop")
    app.add_route("/runtime/on_tick", resource, suffix="on_tick")
    app.add_route("/runtime/on_bar", resource, suffix="on_bar")
    app.add_route("/runtime/on_replay_bar", resource, suffix="on_replay_bar")
    app.add_route("/runtime/result", resource, suffix="result")
    app.add_route("/backtest/run", resource, suffix="backtest_run")
    app.add_route("/backtest/result", resource, suffix="backtest_result")
    app.add_route("/optimizer/run-sweep", resource, suffix="optimizer_run_sweep")
    return app


def split_addr(addr: str) -> tuple[str, int]:
    raw = str(addr or "").strip()
    if not raw:
        return "127.0.0.1", 50051
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlsplit(raw)
        return parsed.hostname or "127.0.0.1", parsed.port or 8000
    if ":" not in raw:
        return raw, 8000
    host, port_text = raw.rsplit(":", 1)
    return host or "127.0.0.1", int(port_text)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:50051")
    parser.add_argument("--log-file", default=DEFAULT_STRATEGY_LOG_FILE)
    parser.add_argument("--log-level", default=os.environ.get("STRATEGY_LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    _configure_logging(args.log_file, args.log_level)
    host, port = split_addr(args.addr)
    logger.info("strategy http service listening on %s:%s log_file=%s", host, port, args.log_file)
    print(f"strategy http service listening on {host}:{port}", flush=True)
    uvicorn.run(
        build_app(),
        host=host,
        port=port,
        log_level=str(args.log_level or "info").lower(),
        access_log=False,
    )


if __name__ == "__main__":
    main()
