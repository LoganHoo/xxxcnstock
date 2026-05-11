"""Filter 服务 - 封装过滤器引擎"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime

import polars as pl

from nextai.services.data_access import DataAccess


@dataclass
class FilterStats:
    filter_name: str
    input_count: int
    output_count: int
    removed_count: int
    enabled: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FilterResponse:
    success: bool
    original_count: int
    filtered_count: int
    removed_count: int
    stats: List[FilterStats]
    stocks: List[Dict[str, Any]]
    execution_time: float
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "original_count": self.original_count,
            "filtered_count": self.filtered_count,
            "removed_count": self.removed_count,
            "stats": [s.to_dict() for s in self.stats],
            "stocks": self.stocks,
            "execution_time": round(self.execution_time, 2),
            "error": self.error,
        }


class FilterService:
    """过滤器服务"""

    def __init__(
        self,
        data_access: DataAccess,
        config_dir: str = "config/filters",
        preset: str = "default",
    ):
        self.data_access = data_access
        self.config_dir = config_dir
        self.preset = preset
        self._engine = None

    def _get_engine(self):
        if self._engine is not None:
            return self._engine

        project_root = Path(__file__).resolve().parent.parent.parent
        sys.path.insert(0, str(project_root))

        from filters.filter_engine import FilterEngine

        self._engine = FilterEngine(
            config_dir=self.config_dir, preset=self.preset
        )
        return self._engine

    def apply_filters(
        self,
        stock_list: Optional[pl.DataFrame] = None,
        filter_names: Optional[List[str]] = None,
    ) -> FilterResponse:
        import time
        start = time.time()

        try:
            if stock_list is None:
                stock_list = self.data_access.get_stock_list()

            original_count = stock_list.height
            engine = self._get_engine()

            result = engine.apply_filters(stock_list, filter_names=filter_names)
            filtered_count = result.height

            stats = []
            for name, f in engine.filters.items():
                stats.append(
                    FilterStats(
                        filter_name=name,
                        input_count=original_count,
                        output_count=filtered_count,
                        removed_count=original_count - filtered_count,
                        enabled=f.is_enabled(),
                    )
                )

            stocks = []
            if "code" in result.columns:
                for row in result.iter_rows(named=True):
                    stocks.append(
                        {
                            "code": str(row.get("code", "")).zfill(6),
                            "name": str(row.get("name", "")),
                        }
                    )

            return FilterResponse(
                success=True,
                original_count=original_count,
                filtered_count=filtered_count,
                removed_count=original_count - filtered_count,
                stats=stats,
                stocks=stocks,
                execution_time=time.time() - start,
            )

        except Exception as e:
            return FilterResponse(
                success=False,
                original_count=0,
                filtered_count=0,
                removed_count=0,
                stats=[],
                stocks=[],
                execution_time=time.time() - start,
                error=str(e),
            )

    def list_filters(self) -> List[Dict[str, Any]]:
        try:
            engine = self._get_engine()
            return [
                {
                    "name": name,
                    "enabled": f.is_enabled(),
                    "description": f.description,
                }
                for name, f in engine.filters.items()
            ]
        except Exception:
            return []
