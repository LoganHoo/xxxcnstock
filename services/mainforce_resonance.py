"""
主力痕迹共振检测器
识别四大主力信号：高质量涨停、未回补跳空、标准连阳、关键位放量
"""
import polars as pl
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path


class MainForceSignal:
    S1_QUALITY_LIMIT_UP = "S1_quality_limit_up"
    S2_UNFILLED_GAP = "S2_unfilled_gap"
    S3_CONSECUTIVE_UP = "S3_consecutive_up"
    S4_BREAKOUT_VOLUME = "S4_breakout_volume"


class MainForceDetector:
    """主力痕迹共振检测器"""

    def __init__(self, lookback_days: int = 20):
        self.lookback_days = lookback_days

    def detect(self, df: pl.DataFrame) -> Dict:
        """
        检测股票的四大主力痕迹信号

        Args:
            df: K线数据，必须包含列:
                code, trade_date, open, close, high, low, volume

        Returns:
            包含信号检测结果的字典
        """
        if df is None or len(df) < 5:
            return self._empty_result()

        df = df.sort("trade_date")

        signals = {}

        signals[MainForceSignal.S1_QUALITY_LIMIT_UP] = self._detect_quality_limit_up(df)
        signals[MainForceSignal.S2_UNFILLED_GAP] = self._detect_unfilled_gap(df)
        signals[MainForceSignal.S3_CONSECUTIVE_UP] = self._detect_consecutive_up(df)
        signals[MainForceSignal.S4_BREAKOUT_VOLUME] = self._detect_breakout_volume(df)

        signal_count = sum(1 for v in signals.values() if v["detected"])
        grade = self._compute_grade(signal_count)

        return {
            "code": df["code"][0] if len(df) > 0 else None,
            "grade": grade,
            "signal_count": signal_count,
            "signals": signals
        }

    def _empty_result(self) -> Dict:
        """返回空结果"""
        return {
            "code": None,
            "grade": "N",
            "signal_count": 0,
            "signals": {
                MainForceSignal.S1_QUALITY_LIMIT_UP: {"detected": False},
                MainForceSignal.S2_UNFILLED_GAP: {"detected": False},
                MainForceSignal.S3_CONSECUTIVE_UP: {"detected": False},
                MainForceSignal.S4_BREAKOUT_VOLUME: {"detected": False}
            }
        }

    def _detect_quality_limit_up(self, df: pl.DataFrame) -> Dict:
        """
        检测高质量涨停
        条件：20日内有涨停 + 量能>=前日1.5倍 + 之后10日横盘不跌
        """
        result = {"detected": False}

        if len(df) < 11:
            return result

        close = df["close"].to_numpy()
        prev_close = df["close"].shift(1).to_numpy()
        volume = df["volume"].to_numpy()
        low = df["low"].to_numpy()

        for i in range(1, len(close)):
            if close[i] >= prev_close[i] * 1.095:
                limit_up_idx = i

                if volume[i] >= volume[i-1] * 1.5:
                    consolidation_ok = True
                    for j in range(limit_up_idx + 1, min(limit_up_idx + 11, len(close))):
                        if low[j] < close[limit_up_idx] * 0.97:
                            consolidation_ok = False
                            break
                        if close[j] < prev_close[j] * 0.9:
                            consolidation_ok = False
                            break

                    if consolidation_ok:
                        result = {
                            "detected": True,
                            "limit_up_date": df["trade_date"][limit_up_idx],
                            "volume_ratio": round(volume[i] / volume[i-1], 2) if volume[i-1] > 0 else 0,
                            "consolidation_days": min(10, len(close) - limit_up_idx - 1)
                        }
                        break

        return result

    def _detect_unfilled_gap(self, df: pl.DataFrame) -> Dict:
        """
        检测未回补的向上跳空缺口
        条件：3日内存在向上跳空缺口，且至今未回补
        """
        result = {"detected": False}

        if len(df) < 4:
            return result

        close = df["close"].to_numpy()
        high = df["high"].to_numpy()
        low = df["low"].to_numpy()

        for i in range(2, len(close)):
            today_low = low[i]
            yesterday_high = high[i-1]

            if today_low > yesterday_high:
                gap_size = today_low - yesterday_high
                gap_start_idx = i

                unfilled = True
                for j in range(gap_start_idx + 1, len(close)):
                    if low[j] <= yesterday_high:
                        unfilled = False
                        break

                if unfilled:
                    result = {
                        "detected": True,
                        "gap_date": df["trade_date"][gap_start_idx],
                        "gap_size": round(gap_size, 3),
                        "days_since_gap": len(close) - gap_start_idx
                    }
                    break

        return result

    def _detect_consecutive_up(self, df: pl.DataFrame) -> Dict:
        """
        检测标准连阳结构
        条件：连续3日收盘价创新高，且每日最低价>=前日收盘价
        """
        result = {"detected": False}

        if len(df) < 4:
            return result

        close = df["close"].to_numpy()
        low = df["low"].to_numpy()

        consecutive_count = 1
        start_idx = len(close) - 1

        for i in range(1, len(close)):
            if close[i] > close[i-1] and low[i] >= close[i-1]:
                if consecutive_count == 1:
                    start_idx = i - 1
                consecutive_count += 1
            else:
                if consecutive_count >= 3:
                    break
                consecutive_count = 1

        if consecutive_count >= 3:
            result = {
                "detected": True,
                "consecutive_days": consecutive_count,
                "start_date": df["trade_date"][max(0, start_idx)],
                "latest_close": round(close[-1], 2)
            }

        return result

    def _detect_breakout_volume(self, df: pl.DataFrame) -> Dict:
        """
        检测关键位有效放量
        条件：放量突破日均量2倍 + 站稳放量区
        """
        result = {"detected": False}

        if len(df) < 5:
            return result

        close = df["close"].to_numpy()
        volume = df["volume"].to_numpy()
        low = df["low"].to_numpy()
        avg_volume = volume[:-1].mean() if len(volume) > 1 else volume.mean()

        for i in range(3, len(close)):
            if volume[i] >= avg_volume * 2:
                breakout_idx = i
                breakout_price = close[breakout_idx]

                standing_firm = True
                for j in range(breakout_idx + 1, len(close)):
                    if close[j] < breakout_price:
                        standing_firm = False
                        break

                if standing_firm:
                    result = {
                        "detected": True,
                        "volume_ratio": round(volume[i] / avg_volume, 2) if avg_volume > 0 else 0,
                        "breakout_date": df["trade_date"][breakout_idx],
                        "breakout_price": round(breakout_price, 2)
                    }
                    break

        return result

    def _compute_grade(self, signal_count: int) -> str:
        """根据信号计数计算级别"""
        if signal_count >= 4:
            return "S+"
        elif signal_count == 3:
            return "A"
        elif signal_count == 2:
            return "B"
        elif signal_count == 1:
            return "C"
        else:
            return "N"


def scan_mainforce_signals(kline_dir: str, output_path: Optional[str] = None,
                          max_data_age_days: int = 30) -> pl.DataFrame:
    """
    扫描全市场股票的主力痕迹信号

    Args:
        kline_dir: K线数据目录
        output_path: 可选，输出文件路径
        max_data_age_days: 数据最大年龄（天），超过此期限的股票数据将被忽略，默认30天

    Returns:
        包含所有股票信号检测结果的DataFrame
    """
    from pathlib import Path
    from datetime import timedelta, date

    kline_path = Path(kline_dir)
    detector = MainForceDetector()
    cutoff_date = (date.today() - timedelta(days=max_data_age_days)).isoformat()

    results = []

    for parquet_file in kline_path.glob("*.parquet"):
        if parquet_file.name == ".fetch_progress.json":
            continue

        try:
            df = pl.read_parquet(parquet_file)
            if df is None or len(df) < 5:
                continue

            latest_date = df["trade_date"].max()
            if str(latest_date) < cutoff_date:
                continue

            result = detector.detect(df)

            if result["signal_count"] >= 2:
                results.append({
                    "code": result["code"],
                    "grade": result["grade"],
                    "signal_count": result["signal_count"],
                    "S1": result["signals"][MainForceSignal.S1_QUALITY_LIMIT_UP]["detected"],
                    "S2": result["signals"][MainForceSignal.S2_UNFILLED_GAP]["detected"],
                    "S3": result["signals"][MainForceSignal.S3_CONSECUTIVE_UP]["detected"],
                    "S4": result["signals"][MainForceSignal.S4_BREAKOUT_VOLUME]["detected"]
                })

        except Exception as e:
            continue

    if not results:
        return pl.DataFrame()

    result_df = pl.DataFrame(results)
    result_df = result_df.sort(["signal_count", "code"], descending=[True, False])

    if output_path:
        result_df.write_parquet(output_path)

    return result_df


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        kline_dir = sys.argv[1]
    else:
        kline_dir = Path(__file__).parent.parent / "data" / "kline"

    print(f"扫描K线目录: {kline_dir}")

    result = scan_mainforce_signals(str(kline_dir))

    print(f"\n扫描完成: 共发现{len(result)}只股票有主力信号")
    if len(result) > 0:
        print("\n=== S+ 主升浪信号 ===")
        s_plus = result.filter(pl.col("grade") == "S+")
        if len(s_plus) > 0:
            print(s_plus)
        else:
            print("无")

        print("\n=== A级重点关注 ===")
        a_stocks = result.filter(pl.col("grade") == "A")
        if len(a_stocks) > 0:
            print(a_stocks.head(20))