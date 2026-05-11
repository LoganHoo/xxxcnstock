"""统一数据访问层 - 封装K线数据、股票列表、数据库访问"""
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import polars as pl


class DataAccess:
    """统一数据访问层"""

    def __init__(
        self,
        kline_dir: str = "data/kline",
        stock_list_path: str = "data/stock_list.parquet",
        limitup_dir: str = "data/limitup",
    ):
        self.kline_dir = Path(kline_dir)
        self.stock_list_path = Path(stock_list_path)
        self.limitup_dir = Path(limitup_dir)
        self._stock_names: Optional[Dict[str, str]] = None
        self._stock_list: Optional[pl.DataFrame] = None

    def get_stock_list(self) -> pl.DataFrame:
        if self._stock_list is not None:
            return self._stock_list

        if self.stock_list_path.exists():
            self._stock_list = pl.read_parquet(self.stock_list_path)
        else:
            self._stock_list = pl.DataFrame()

        return self._stock_list

    def get_stock_names(self) -> Dict[str, str]:
        if self._stock_names is not None:
            return self._stock_names

        stock_list = self.get_stock_list()
        if stock_list.height == 0:
            self._stock_names = {}
            return self._stock_names

        names = {}
        for row in stock_list.iter_rows(named=True):
            code = str(row.get("code", "")).zfill(6)
            name = str(row.get("name", ""))
            names[code] = name

        self._stock_names = names
        return self._stock_names

    def get_kline(self, code: str, days: int = 60) -> Optional[pl.DataFrame]:
        file_path = self.kline_dir / f"{code}.parquet"
        if not file_path.exists():
            return None

        df = pl.read_parquet(file_path)
        if df.height == 0:
            return None

        if "trade_date" in df.columns and days > 0:
            cutoff = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")
            df = df.filter(pl.col("trade_date") >= cutoff)

        return df

    def get_kline_batch(self, codes: List[str], days: int = 60) -> Dict[str, pl.DataFrame]:
        result = {}
        for code in codes:
            kline = self.get_kline(code, days)
            if kline is not None and kline.height > 0:
                result[code] = kline
        return result

    def get_limitup_data(self, trade_date: Optional[str] = None) -> Optional[pl.DataFrame]:
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        file_path = self.limitup_dir / f"{trade_date}.parquet"
        if not file_path.exists():
            return None

        return pl.read_parquet(file_path)

    def check_data_freshness(self, code: str, max_age_days: int = 30) -> bool:
        kline = self.get_kline(code, days=max_age_days + 10)
        if kline is None or kline.height == 0:
            return False

        if "trade_date" not in kline.columns:
            return True

        latest_date = kline.select(pl.col("trade_date").max()).item()
        if latest_date is None:
            return False

        latest = datetime.strptime(str(latest_date), "%Y%m%d")
        age = (datetime.now() - latest).days
        return age <= max_age_days

    def get_available_codes(self) -> List[str]:
        if not self.kline_dir.exists():
            return []

        return [
            f.stem for f in self.kline_dir.glob("*.parquet")
        ]
