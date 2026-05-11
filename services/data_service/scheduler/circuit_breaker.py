"""
Circuit breaker state machine for task execution.

Prevents repeated execution of persistently failing tasks.
States: CLOSED (normal) -> OPEN (failing, skip task) -> HALF_OPEN (try one attempt) -> CLOSED.

State is persisted per-task as JSON files with atomic writes.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger


class CircuitBreaker:
    """Circuit breaker that tracks consecutive failures per task."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, state_dir: Path = Path("data/circuit_breaker")):
        self._state_dir = state_dir
        self._state_dir.mkdir(parents=True, exist_ok=True)

    def _get_state_path(self, task_name: str) -> Path:
        """Get the state file path for a task."""
        # Sanitize task_name to prevent path traversal
        safe_name = task_name.replace("/", "_").replace("\\", "_")
        return self._state_dir / f"{safe_name}.json"

    def _load_state(self, task_name: str) -> dict:
        """Load circuit breaker state from disk."""
        path = self._get_state_path(task_name)
        if not path.exists():
            return {
                "state": self.CLOSED,
                "failure_count": 0,
                "last_failure_time": None,
                "opened_at": None,
            }
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"Failed to load circuit breaker state for {task_name}: {e}")
            return {
                "state": self.CLOSED,
                "failure_count": 0,
                "last_failure_time": None,
                "opened_at": None,
            }

    def _save_state(self, task_name: str, state: dict) -> None:
        """Write state atomically (write to .tmp, then rename)."""
        path = self._get_state_path(task_name)
        tmp_path = path.with_suffix(".tmp")
        try:
            tmp_path.write_text(
                json.dumps(state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp_path.rename(path)
        except Exception as e:
            logger.error(f"Failed to save circuit breaker state for {task_name}: {e}")
            # Clean up tmp file if rename failed
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def should_skip(self, task_name: str, config: dict) -> bool:
        """Check if a task should be skipped due to open circuit breaker.

        Args:
            task_name: Task identifier.
            config: Circuit breaker config dict with keys:
                enabled, failure_threshold, recovery_timeout.

        Returns:
            True if the task should be skipped (circuit is open).
        """
        if not config or not config.get("enabled", False):
            return False

        threshold = int(config.get("failure_threshold", 3))
        recovery_timeout = int(config.get("recovery_timeout", 3600))

        state = self._load_state(task_name)
        current_state = state.get("state", self.CLOSED)

        if current_state == self.CLOSED:
            return False  # Normal operation, allow execution

        if current_state == self.OPEN:
            opened_at = state.get("opened_at")
            if opened_at:
                elapsed = (
                    datetime.now() - datetime.fromisoformat(opened_at)
                ).total_seconds()
                if elapsed >= recovery_timeout:
                    # Transition to half-open: allow one attempt
                    state["state"] = self.HALF_OPEN
                    self._save_state(task_name, state)
                    logger.info(
                        f"Circuit breaker half-open for {task_name}, allowing attempt"
                    )
                    return False
            return True  # Still open, skip the task

        if current_state == self.HALF_OPEN:
            return False  # Allow one attempt to test recovery

        return False

    def record_success(self, task_name: str) -> None:
        """Reset circuit breaker to closed on task success."""
        state = self._load_state(task_name)
        current = state.get("state", self.CLOSED)
        if current != self.CLOSED:
            logger.info(f"Circuit breaker closed for {task_name} after success")
        self._save_state(
            task_name,
            {
                "state": self.CLOSED,
                "failure_count": 0,
                "last_failure_time": None,
                "opened_at": None,
            },
        )

    def record_failure(self, task_name: str, config: dict) -> None:
        """Record a task failure and potentially open the circuit breaker.

        Args:
            task_name: Task identifier.
            config: Circuit breaker config dict with keys:
                enabled, failure_threshold, recovery_timeout.
        """
        if not config or not config.get("enabled", False):
            return

        threshold = int(config.get("failure_threshold", 3))

        state = self._load_state(task_name)
        current_state = state.get("state", self.CLOSED)

        if current_state == self.HALF_OPEN:
            # Failure during half-open: go back to open
            state["state"] = self.OPEN
            state["opened_at"] = datetime.now().isoformat()
            state["failure_count"] = state.get("failure_count", 0) + 1
            self._save_state(task_name, state)
            logger.warning(
                f"Circuit breaker re-opened for {task_name} (half-open failure)"
            )
            return

        # CLOSED state: increment failure count
        state["failure_count"] = state.get("failure_count", 0) + 1
        state["last_failure_time"] = datetime.now().isoformat()

        if state["failure_count"] >= threshold:
            state["state"] = self.OPEN
            state["opened_at"] = datetime.now().isoformat()
            logger.warning(
                f"Circuit breaker opened for {task_name} "
                f"(failures: {state['failure_count']}, threshold: {threshold})"
            )

        self._save_state(task_name, state)
