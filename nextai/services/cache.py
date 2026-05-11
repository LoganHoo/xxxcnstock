"""缓存访问层 - 复用 core/redis_client.py"""
import sys
import json
from pathlib import Path
from typing import Optional, Any

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


class CacheAccess:
    def __init__(self, prefix: str = "xcnstock:api"):
        self._client = None
        self.prefix = prefix

    def _get_client(self):
        if self._client is not None:
            return self._client

        try:
            from core.redis_client import get_redis_client
            self._client = get_redis_client()
            return self._client
        except Exception:
            self._client = None
            return None

    def _key(self, key: str) -> str:
        return f"{self.prefix}:{key}"

    def get(self, key: str) -> Optional[Any]:
        client = self._get_client()
        if client is None:
            return None

        try:
            value = client.get(self._key(key))
            if value is None:
                return None
            return json.loads(value)
        except Exception:
            return None

    def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        client = self._get_client()
        if client is None:
            return False

        try:
            client.setex(self._key(key), ttl, json.dumps(value, ensure_ascii=False))
            return True
        except Exception:
            return False

    def delete(self, key: str) -> bool:
        client = self._get_client()
        if client is None:
            return False

        try:
            client.delete(self._key(key))
            return True
        except Exception:
            return False

    def get_scan_result(self, trade_date: str) -> Optional[dict]:
        return self.get(f"scan:{trade_date}")

    def cache_scan_result(self, trade_date: str, result: dict, ttl: int = 600) -> bool:
        return self.set(f"scan:{trade_date}", result, ttl)

    def get_prediction(self, trade_date: str) -> Optional[dict]:
        return self.get(f"prediction:{trade_date}")

    def cache_prediction(self, trade_date: str, result: dict, ttl: int = 600) -> bool:
        return self.set(f"prediction:{trade_date}", result, ttl)
