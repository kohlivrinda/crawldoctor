"""Async sweep scheduler — fires registered sweeps on cadence with advisory locking."""
from __future__ import annotations

import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
import structlog

from app.background.models import BackgroundJobState
from app.background.registry import RegisteredJob, registry
from app.background.runner import job_runner
from app.config import settings
from app.database import SessionLocal
from sqlalchemy import text

logger = structlog.get_logger()


class JobScheduler:
    """Periodically fires sweep functions for registered jobs.

    Each sweep is protected by a Postgres advisory lock so only one
    worker runs a given sweep at a time. Watermarks are advanced to
    the sweep start time after successful enqueue.
    """

    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._last_run: Dict[str, datetime] = {}
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="job_scheduler")

    async def start(self) -> None:
        if self._task:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="job_scheduler")
        logger.info("Job scheduler started")

    async def stop(self) -> None:
        if not self._task:
            return
        self._stop_event.set()
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None
        self._executor.shutdown(wait=False)
        logger.info("Job scheduler stopped")

    async def _run(self) -> None:
        """Main loop — checks every 60s which sweeps are due."""
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=60.0)
                break  # stop_event was set
            except asyncio.TimeoutError:
                pass  # 60s elapsed, do a tick

            for job in registry.all_jobs():
                if self._is_due(job):
                    try:
                        await loop.run_in_executor(
                            self._executor, self._run_sweep, job, loop
                        )
                    except Exception as exc:
                        logger.error(
                            "Sweep failed", job_name=job.name, error=str(exc), exc_info=True
                        )

    def _is_due(self, job: RegisteredJob) -> bool:
        """Check if enough time has passed since last sweep for this job."""
        last = self._last_run.get(job.name)
        if last is None:
            return True
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        return elapsed >= job.sweep_interval_minutes * 60

    def _advisory_lock_id(self, job_name: str) -> int:
        """Deterministic 32-bit lock ID from job name."""
        h = hashlib.md5(job_name.encode()).hexdigest()
        return int(h[:8], 16)

    def _run_sweep(self, job: RegisteredJob, loop: asyncio.AbstractEventLoop) -> None:
        """Execute one sweep: lock -> read watermark -> sweep -> enqueue -> advance watermark."""
        db = SessionLocal()
        try:
            lock_id = self._advisory_lock_id(job.name)

            # Try advisory lock — skip if another worker holds it
            acquired = db.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id}
            ).scalar()

            if not acquired:
                logger.debug("Sweep skipped (lock held)", job_name=job.name)
                return

            try:
                sweep_time = datetime.now(timezone.utc)

                # Read watermark
                state = db.query(BackgroundJobState).filter(
                    BackgroundJobState.job_name == job.name
                ).first()

                if state:
                    since = state.last_processed_at
                else:
                    since = sweep_time - timedelta(days=settings.summary_backfill_days)

                # Run sweep to get payloads
                payloads = job.instance.sweep(db, since)

                if payloads:
                    logger.info(
                        "Sweep produced jobs",
                        job_name=job.name,
                        count=len(payloads),
                        since=since.isoformat(),
                    )

                # Enqueue each payload (async — need to bridge from sync context)
                for payload in payloads:
                    dedup_key = payload.get("client_id")
                    asyncio.run_coroutine_threadsafe(
                        job_runner.enqueue(job.name, payload, dedup_key=dedup_key),
                        loop,
                    )

                # Advance watermark
                if state:
                    state.last_processed_at = sweep_time
                else:
                    state = BackgroundJobState(
                        job_name=job.name, last_processed_at=sweep_time
                    )
                    db.add(state)

                db.commit()
                self._last_run[job.name] = sweep_time

                if payloads:
                    logger.info("Sweep completed", job_name=job.name, watermark=sweep_time.isoformat())
                else:
                    logger.debug("Sweep completed (no new work)", job_name=job.name)

            finally:
                # Release advisory lock
                db.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id})
                db.commit()

        except Exception as exc:
            logger.error("Sweep error", job_name=job.name, error=str(exc), exc_info=True)
            try:
                db.rollback()
            except Exception:
                pass
        finally:
            db.close()


# Singleton
job_scheduler = JobScheduler()
