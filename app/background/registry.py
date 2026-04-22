"""Background job registry — single decorator registers handler + sweep."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol, Type
from sqlalchemy.orm import Session
import structlog

logger = structlog.get_logger()


class JobProtocol(Protocol):
    """Interface that registered job classes must implement."""

    def sweep(self, db: Session, since: Any) -> List[Dict[str, Any]]: ...
    def handle(self, db: Session, payload: Dict[str, Any]) -> None: ...


@dataclass
class RegisteredJob:
    """Metadata for a registered background job."""

    name: str
    sweep_interval_minutes: int
    instance: JobProtocol


class BackgroundRegistry:
    """Stores registered background jobs keyed by name."""

    def __init__(self) -> None:
        self._jobs: Dict[str, RegisteredJob] = {}

    def job(self, name: str, sweep_interval_minutes: int = 5) -> Callable:
        """Class decorator that registers a job handler + sweep.

        Usage:
            @registry.job("recompute_journey", sweep_interval_minutes=5)
            class RecomputeJourney:
                def sweep(self, db, since): ...
                def handle(self, db, payload): ...
        """

        def decorator(cls: Type) -> Type:
            if name in self._jobs:
                raise ValueError(f"Job '{name}' is already registered")
            instance = cls()
            if not hasattr(instance, "sweep") or not callable(instance.sweep):
                raise TypeError(f"Job class {cls.__name__} must implement sweep(db, since)")
            if not hasattr(instance, "handle") or not callable(instance.handle):
                raise TypeError(f"Job class {cls.__name__} must implement handle(db, payload)")
            self._jobs[name] = RegisteredJob(
                name=name,
                sweep_interval_minutes=sweep_interval_minutes,
                instance=instance,
            )
            logger.info("Registered background job", job_name=name, sweep_interval=sweep_interval_minutes)
            return cls

        return decorator

    def get_handler(self, name: str) -> Optional[JobProtocol]:
        """Look up a job instance by name."""
        entry = self._jobs.get(name)
        return entry.instance if entry else None

    def all_jobs(self) -> List[RegisteredJob]:
        """Return all registered jobs."""
        return list(self._jobs.values())


# Singleton
registry = BackgroundRegistry()
