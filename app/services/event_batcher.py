"""In-process event batcher to reduce per-event write overhead."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
import structlog

from app.config import settings
from app.database import SessionLocal
from app.models.visit import VisitEvent

logger = structlog.get_logger()


class EventBatcher:
    """Batch insert VisitEvent rows using a background task."""

    def __init__(self) -> None:
        self.enabled = settings.event_batch_enabled
        self.batch_size = settings.event_batch_size
        self.max_delay_ms = settings.event_batch_max_delay_ms
        self.max_queue_size = settings.event_batch_max_queue
        self._queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=self.max_queue_size)
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if not self.enabled or self._task:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="event_batcher")
        logger.info("Event batcher started", batch_size=self.batch_size)

    async def stop(self) -> None:
        if not self._task:
            return
        self._stop_event.set()
        await self._drain_pending()
        self._task.cancel()
        try:
            await self._task
        except Exception:
            pass
        self._task = None
        logger.info("Event batcher stopped")

    async def enqueue(self, event_dict: Dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        try:
            self._queue.put_nowait(event_dict)
            return True
        except asyncio.QueueFull:
            logger.warning("Event batcher queue full; falling back to direct insert")
            return False

    async def _run(self) -> None:
        delay_seconds = max(self.max_delay_ms / 1000.0, 0.05)
        while not self._stop_event.is_set():
            batch: List[Dict[str, Any]] = []
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=delay_seconds)
                batch.append(item)
            except asyncio.TimeoutError:
                item = None

            while len(batch) < self.batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if batch:
                await asyncio.to_thread(self._flush_batch, batch)

    def _flush_batch(self, batch: List[Dict[str, Any]]) -> None:
        db = SessionLocal()
        try:
            db.execute(VisitEvent.__table__.insert(), batch)
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.error("Event batch insert failed", error=str(exc), batch_size=len(batch))
        finally:
            db.close()

    async def _drain_pending(self) -> None:
        while not self._queue.empty():
            batch: List[Dict[str, Any]] = []
            while not self._queue.empty() and len(batch) < self.batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if batch:
                await asyncio.to_thread(self._flush_batch, batch)


event_batcher = EventBatcher()
