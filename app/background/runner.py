"""Async job runner — drains a queue of background jobs with dedup."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional, Tuple
import structlog

from app.background.registry import registry
from app.database import SessionLocal

logger = structlog.get_logger()


class JobRunner:
    """In-process async job queue with deduplication.

    Modeled on EventBatcher: enqueue() is non-blocking, a background task
    drains the queue and executes handlers in a thread pool.
    """

    def __init__(self, max_queue: int = 1000, max_workers: int = 4) -> None:
        self._queue: asyncio.Queue[Tuple[str, Dict[str, Any], Optional[str]]] = asyncio.Queue(
            maxsize=max_queue
        )
        self._pending: set[Tuple[str, str]] = set()  # (job_name, dedup_key)
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="job_runner")

    async def start(self) -> None:
        if self._task:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="job_runner")
        logger.info("Job runner started")

    async def stop(self) -> None:
        if not self._task:
            return
        self._stop_event.set()
        # Drain remaining items
        await self._drain()
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None
        self._executor.shutdown(wait=False)
        logger.info("Job runner stopped")

    async def enqueue(
        self, job_name: str, payload: Dict[str, Any], dedup_key: Optional[str] = None
    ) -> bool:
        """Add a job to the queue. Returns False if deduplicated or queue full."""
        if dedup_key is not None:
            key = (job_name, dedup_key)
            if key in self._pending:
                logger.debug("Job deduplicated", job_name=job_name, dedup_key=dedup_key)
                return False
            self._pending.add(key)

        try:
            self._queue.put_nowait((job_name, payload, dedup_key))
            return True
        except asyncio.QueueFull:
            # Remove from pending since we couldn't enqueue
            if dedup_key is not None:
                self._pending.discard((job_name, dedup_key))
            logger.warning("Job runner queue full", job_name=job_name)
            return False

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            try:
                job_name, payload, dedup_key = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                handler = registry.get_handler(job_name)
                if handler is None:
                    logger.error("No handler registered", job_name=job_name)
                    continue

                await loop.run_in_executor(
                    self._executor, self._execute_handler, handler, job_name, payload
                )
            except Exception as exc:
                logger.error(
                    "Job execution failed",
                    job_name=job_name,
                    payload=payload,
                    error=str(exc),
                )
            finally:
                if dedup_key is not None:
                    self._pending.discard((job_name, dedup_key))

    @staticmethod
    def _execute_handler(handler, job_name: str, payload: Dict[str, Any]) -> None:
        """Run a handler synchronously in the thread pool."""
        db = SessionLocal()
        try:
            handler.handle(db, payload)
            logger.info("Job completed", job_name=job_name, payload=payload)
        except Exception as exc:
            logger.error(
                "Job handler error",
                job_name=job_name,
                payload=payload,
                error=str(exc),
                exc_info=True,
            )
        finally:
            db.close()

    async def _drain(self) -> None:
        """Process remaining queued jobs during shutdown."""
        loop = asyncio.get_running_loop()
        while not self._queue.empty():
            try:
                job_name, payload, dedup_key = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            try:
                handler = registry.get_handler(job_name)
                if handler:
                    await loop.run_in_executor(
                        self._executor, self._execute_handler, handler, job_name, payload
                    )
            except Exception as exc:
                logger.error("Job drain failed", job_name=job_name, error=str(exc))
            finally:
                if dedup_key is not None:
                    self._pending.discard((job_name, dedup_key))


# Singleton
job_runner = JobRunner()
