"""
SQLite-based execution history with WAL mode and thread-safe writes.

Records task start/complete/fail events. Both the scheduler engine and
the HTTP API process read from this database concurrently (WAL mode
allows concurrent reads while the engine writes).
"""

import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger


class HistoryDB:
    """Thread-safe SQLite execution history database."""

    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        """Create a new connection with WAL mode."""
        conn = sqlite3.connect(self._db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        """Initialize the database table and indexes."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS task_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_name TEXT NOT NULL,
                        start_time TEXT NOT NULL,
                        end_time TEXT,
                        status TEXT NOT NULL,
                        return_code INTEGER,
                        error_message TEXT,
                        duration_ms INTEGER
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_task_history_task_start
                    ON task_history(task_name, start_time)
                """)
                conn.commit()
            finally:
                conn.close()

    def record_start(self, task_name: str) -> int:
        """Record task start. Returns the row ID."""
        with self._lock:
            conn = self._connect()
            try:
                cursor = conn.execute(
                    "INSERT INTO task_history (task_name, start_time, status) "
                    "VALUES (?, ?, ?)",
                    (task_name, datetime.now().isoformat(), "running"),
                )
                conn.commit()
                return cursor.lastrowid
            finally:
                conn.close()

    def record_complete(
        self,
        record_id: int,
        status: str,
        return_code: int,
        error_message: str,
        duration_ms: int,
    ) -> None:
        """Record task completion (success, failed, or timeout)."""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE task_history "
                    "SET end_time = ?, status = ?, return_code = ?, "
                    "    error_message = ?, duration_ms = ? "
                    "WHERE id = ?",
                    (
                        datetime.now().isoformat(),
                        status,
                        return_code,
                        error_message[:2000] if error_message else "",
                        duration_ms,
                        record_id,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

    def get_last_execution(self, task_name: str) -> Optional[dict]:
        """Get the most recent execution record for a task."""
        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM task_history "
                "WHERE task_name = ? ORDER BY start_time DESC LIMIT 1",
                (task_name,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_task_stats(self, task_name: str, days: int = 7) -> dict:
        """Return aggregate stats for a task over the last N days."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    AVG(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) as avg_duration_ms
                FROM task_history
                WHERE task_name = ?
                  AND start_time > datetime('now', ? || ' days')
                """,
                (task_name, f"-{days}"),
            )
            row = cursor.fetchone()
            return {
                "total": row[0] or 0,
                "success": row[1] or 0,
                "failed": row[2] or 0,
                "avg_duration_ms": round(row[3], 2) if row[3] else 0,
            }
        finally:
            conn.close()

    def get_recent_tasks(self, limit: int = 50) -> list[dict]:
        """Return recent execution records across all tasks."""
        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM task_history ORDER BY start_time DESC LIMIT ?",
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def has_successful_run_on_date(self, task_name: str, target_date: str) -> bool:
        """Check if a task has a successful run on the given date."""
        day_start = f"{target_date}T00:00:00"
        day_end = f"{target_date}T23:59:59.999999"
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT 1 FROM task_history "
                "WHERE task_name = ? AND status = 'success' "
                "  AND start_time >= ? AND start_time <= ? "
                "LIMIT 1",
                (task_name, day_start, day_end),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def get_all_task_stats(self, days: int = 7) -> dict:
        """Return aggregate stats for all tasks over the last N days."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                SELECT
                    task_name,
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    AVG(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) as avg_duration_ms
                FROM task_history
                WHERE start_time > datetime('now', ? || ' days')
                GROUP BY task_name
                ORDER BY total DESC
                """,
                (f"-{days}",),
            )
            rows = cursor.fetchall()
            return {
                row[0]: {
                    "total": row[1],
                    "success": row[2],
                    "failed": row[3],
                    "avg_duration_ms": round(row[4], 2) if row[4] else 0,
                }
                for row in rows
            }
        finally:
            conn.close()
