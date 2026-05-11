"""Tests for HistoryDB: SQLite execution history with concurrent write safety."""

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from services.data_service.scheduler.history import HistoryDB


@pytest.fixture
def history_db(tmp_path: Path) -> HistoryDB:
    """Create a HistoryDB with a temp database."""
    db_path = tmp_path / "test_history.db"
    return HistoryDB(db_path)


def test_record_start_and_complete(history_db: HistoryDB):
    """Record start, then complete with success, verify get_last_execution."""
    record_id = history_db.record_start("test_task")
    assert record_id > 0

    history_db.record_complete(
        record_id=record_id,
        status="success",
        return_code=0,
        error_message="",
        duration_ms=1500,
    )

    result = history_db.get_last_execution("test_task")
    assert result is not None
    assert result["task_name"] == "test_task"
    assert result["status"] == "success"
    assert result["return_code"] == 0
    assert result["duration_ms"] == 1500
    assert result["start_time"] is not None
    assert result["end_time"] is not None


def test_record_failure_with_error(history_db: HistoryDB):
    """Record start, then complete with failed status and error message."""
    record_id = history_db.record_start("failing_task")

    history_db.record_complete(
        record_id=record_id,
        status="failed",
        return_code=1,
        error_message="ImportError: no module named xyz",
        duration_ms=300,
    )

    result = history_db.get_last_execution("failing_task")
    assert result is not None
    assert result["status"] == "failed"
    assert result["return_code"] == 1
    assert "ImportError" in result["error_message"]
    assert result["duration_ms"] == 300


def test_concurrent_writes(history_db: HistoryDB):
    """Spawn 5 threads each writing 10 records, verify all 50 records present."""
    errors: list[Exception] = []

    def writer(task_name: str, count: int):
        try:
            for i in range(count):
                rid = history_db.record_start(f"{task_name}_sub{i}")
                history_db.record_complete(
                    record_id=rid,
                    status="success" if i % 2 == 0 else "failed",
                    return_code=0 if i % 2 == 0 else 1,
                    error_message="" if i % 2 == 0 else "err",
                    duration_ms=i * 100,
                )
        except Exception as e:
            errors.append(e)

    threads = []
    for t in range(5):
        th = threading.Thread(target=writer, args=(f"task_{t}", 10))
        threads.append(th)
        th.start()

    for th in threads:
        th.join(timeout=10)

    assert len(errors) == 0, f"Concurrent write errors: {errors}"

    # Verify total record count
    conn = sqlite3.connect(history_db._db_path, timeout=30)
    count = conn.execute("SELECT COUNT(*) FROM task_history").fetchone()[0]
    conn.close()
    assert count == 50, f"Expected 50 records, got {count}"


def test_get_task_stats(history_db: HistoryDB):
    """Insert mix of success/failure records, verify stats calculation."""
    # 3 successes, 2 failures
    for i in range(5):
        rid = history_db.record_start("stats_task")
        history_db.record_complete(
            record_id=rid,
            status="success" if i < 3 else "failed",
            return_code=0 if i < 3 else 1,
            error_message="" if i < 3 else "err",
            duration_ms=1000 + i * 100,
        )

    stats = history_db.get_task_stats("stats_task", days=7)
    assert stats["total"] == 5
    assert stats["success"] == 3
    assert stats["failed"] == 2
    assert stats["avg_duration_ms"] > 0
