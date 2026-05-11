"""数据库访问层 - 复用 core/database.py"""
import sys
from pathlib import Path
from typing import Optional, Generator, List, Dict, Any
from contextlib import contextmanager

from sqlalchemy import text

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


class DatabaseAccess:
    def __init__(self):
        self._manager = None

    def _get_manager(self):
        if self._manager is not None:
            return self._manager

        from core.database import DatabaseManager
        self._manager = DatabaseManager()
        return self._manager

    @contextmanager
    def get_connection(self):
        manager = self._get_manager()
        engine = manager.get_engine()
        conn = engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute_query(self, sql: str, params: dict = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            result = conn.execute(text(sql), params or {})
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]

    def execute_one(self, sql: str, params: dict = None) -> Optional[Dict[str, Any]]:
        rows = self.execute_query(sql, params)
        return rows[0] if rows else None

    def execute_update(self, sql: str, params: dict = None) -> int:
        with self.get_connection() as conn:
            result = conn.execute(text(sql), params or {})
            return result.rowcount

    def get_stock_selections(
        self, trade_date: str, strategy: str = None, limit: int = 50
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT code, name, score, strategy, pick_date, reason
            FROM stock_selections
            WHERE pick_date = :trade_date
        """
        params = {"trade_date": trade_date}
        if strategy:
            sql += " AND strategy = :strategy"
            params["strategy"] = strategy
        sql += " ORDER BY score DESC LIMIT :limit"
        params["limit"] = limit
        return self.execute_query(sql, params)
