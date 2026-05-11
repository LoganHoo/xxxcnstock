#!/usr/bin/env python3
"""
波段推荐系统 - "3步买"战法
基于 PRD 13.3 波段推荐模块

核心策略：
- 第一步：趋势确认（EMA排列 + 月线MACD金叉）
- 第二步：回调识别（缩量回调至20日均线）
- 第三步：买入执行（30%底仓 + 20%滚动仓）

使用方式：
  python swing_recommendation.py                    # 默认模式
  python swing_recommendation.py --min-confidence 70  # 高置信度模式
  python swing_recommendation.py --date 20260510     # 指定日期
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "kline"
OUTPUT_DIR = PROJECT_ROOT / "data" / "predictions"
DB_URL = "mysql+pymysql://nextai:100200@49.233.10.199:3306/xcnstock?charset=utf8mb4"


@dataclass
class SwingSignal:
    """波段信号"""
    code: str
    name: str = ""
    close: float = 0.0
    pct_chg: float = 0.0

    # 趋势指标
    ema5: float = 0.0
    ema10: float = 0.0
    ema20: float = 0.0
    ema60: float = 0.0
    ema120: float = 0.0
    price_vs_ema60: float = 0.0

    # MACD指标
    macd_dif: float = 0.0
    macd_dea: float = 0.0
    macd_hist: float = 0.0
    macd_golden_cross: bool = False
    macd_above_zero: bool = False

    # 回调指标
    pullback_pct: float = 0.0
    volume_shrink_ratio: float = 0.0
    ema20_support: bool = False

    # 评分
    trend_score: float = 0.0
    pullback_score: float = 0.0
    volume_score: float = 0.0
    total_score: float = 0.0
    confidence: float = 0.0

    # 信号
    step1_passed: bool = False
    step2_passed: bool = False
    step3_passed: bool = False

    # 交易建议
    entry_price: float = 0.0
    stop_loss: float = 0.0
    target_price: float = 0.0
    position_size: float = 0.0

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "name": self.name,
            "close": round(float(self.close), 2),
            "pct_chg": round(float(self.pct_chg), 2),
            "ema5": round(float(self.ema5), 2),
            "ema10": round(float(self.ema10), 2),
            "ema20": round(float(self.ema20), 2),
            "ema60": round(float(self.ema60), 2),
            "ema120": round(float(self.ema120), 2),
            "price_vs_ema60": round(float(self.price_vs_ema60), 2),
            "macd_dif": round(float(self.macd_dif), 3),
            "macd_dea": round(float(self.macd_dea), 3),
            "macd_hist": round(float(self.macd_hist), 3),
            "macd_golden_cross": bool(self.macd_golden_cross),
            "macd_above_zero": bool(self.macd_above_zero),
            "pullback_pct": round(float(self.pullback_pct), 2),
            "volume_shrink_ratio": round(float(self.volume_shrink_ratio), 2),
            "ema20_support": bool(self.ema20_support),
            "trend_score": round(float(self.trend_score), 1),
            "pullback_score": round(float(self.pullback_score), 1),
            "volume_score": round(float(self.volume_score), 1),
            "total_score": round(float(self.total_score), 1),
            "confidence": round(float(self.confidence), 1),
            "step1_passed": bool(self.step1_passed),
            "step2_passed": bool(self.step2_passed),
            "step3_passed": bool(self.step3_passed),
            "entry_price": round(float(self.entry_price), 2),
            "stop_loss": round(float(self.stop_loss), 2),
            "target_price": round(float(self.target_price), 2),
            "position_size": round(float(self.position_size), 1),
        }


def _ema(prices: np.ndarray, period: int) -> float:
    """计算EMA"""
    if len(prices) < period:
        return float(prices[-1]) if len(prices) > 0 else 0.0

    ema = float(prices[0])
    multiplier = 2.0 / (period + 1)

    for price in prices[1:]:
        ema = (price - ema) * multiplier + ema

    return ema


def _calculate_ema_series(prices: np.ndarray, period: int) -> np.ndarray:
    """计算EMA序列"""
    if len(prices) < period:
        return np.full(len(prices), np.nan)

    ema = np.zeros(len(prices))
    ema[:period] = np.nan
    ema[period - 1] = np.mean(prices[:period])

    multiplier = 2.0 / (period + 1)

    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i - 1]) * multiplier + ema[i - 1]

    return ema


def _macd(prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float, float]:
    """计算MACD（用于日线）"""
    if len(prices) < slow + signal:
        return 0.0, 0.0, 0.0

    ema_fast = _calculate_ema_series(prices, fast)
    ema_slow = _calculate_ema_series(prices, slow)

    dif = ema_fast - ema_slow

    dif_valid = dif[~np.isnan(dif)]
    if len(dif_valid) < signal:
        return 0.0, 0.0, 0.0

    dea = _ema(dif_valid, signal)
    hist = (dif[-1] - dea) * 2

    return float(dif[-1]), float(dea), float(hist)


def _monthly_macd(daily_prices: np.ndarray, dates: np.ndarray) -> Tuple[float, float, float, bool, bool]:
    """
    计算月线MACD（基于日线数据）
    返回：(dif, dea, hist, 金叉, 在零轴上方)
    """
    if len(daily_prices) < 120:
        return 0.0, 0.0, 0.0, False, False

    date_strings = [str(d)[:7] for d in dates]
    month_map = {}
    for i, ds in enumerate(date_strings):
        if ds not in month_map:
            month_map[ds] = {"close": daily_prices[i], "month_start": date_strings[i]}
        else:
            month_map[ds]["close"] = daily_prices[i]

    monthly_data = [(k, v["close"]) for k, v in sorted(month_map.items())]

    if len(monthly_data) < 30:
        return 0.0, 0.0, 0.0, False, False

    monthly_closes = np.array([m[1] for m in monthly_data])

    macd_dif, macd_dea, macd_hist = _macd(monthly_closes, 12, 26, 9)

    golden_cross = False
    if len(monthly_closes) >= 2:
        prev_dif, prev_dea = _macd(monthly_closes[:-1], 12, 26, 9)
        curr_dif, curr_dea = macd_dif, macd_dea
        golden_cross = (prev_dif <= prev_dea) and (curr_dif > curr_dea)

    above_zero = macd_dif > 0

    return macd_dif, macd_dea, macd_hist, golden_cross, above_zero


def _detect_limit_up_day(volumes: np.ndarray, pct_chg: float = 9.5) -> Tuple[bool, int, float]:
    """
    检测涨停日
    返回：(是否涨停, 涨停日索引, 涨停日成交量)
    """
    for i in range(len(volumes) - 1, max(0, len(volumes) - 5), -1):
        if i < 1:
            continue
        prev_vol = volumes[i - 1]
        curr_vol = volumes[i]

        if prev_vol > 0:
            vol_ratio = curr_vol / prev_vol
        else:
            vol_ratio = 1.0

        if vol_ratio > 2.0 and pct_chg > 9.0:
            return True, i, float(curr_vol)

    return False, -1, 0.0


def _calculate_pullback(highs: np.ndarray, limit_up_idx: int, current_idx: int) -> float:
    """计算从高点回调的百分比"""
    if limit_up_idx < 0 or limit_up_idx >= len(highs):
        return 0.0

    high_after_limitup = np.max(highs[limit_up_idx:current_idx + 1]) if current_idx > limit_up_idx else highs[limit_up_idx]
    current_price = highs[current_idx] if current_idx < len(highs) else highs[-1]

    if high_after_limitup <= 0:
        return 0.0

    pullback = (high_after_limitup - current_price) / high_after_limitup * 100
    return float(pullback)


def _check_ema20_support(prices: np.ndarray, ema20: float, tolerance: float = 0.02) -> bool:
    """检查价格是否在EMA20附近获得支撑"""
    if len(prices) < 5 or ema20 <= 0:
        return False

    recent_prices = prices[-5:]
    min_price = np.min(recent_prices)
    max_price = np.max(recent_prices)

    ema20_low = ema20 * (1 - tolerance)
    ema20_high = ema20 * (1 + tolerance)

    return ema20_low <= min_price <= ema20_high


def load_kline_data(code: str, days: int = 250) -> Optional[pl.DataFrame]:
    """从Parquet加载K线数据"""
    file_path = DATA_DIR / f"{code}.parquet"

    if not file_path.exists():
        return None

    try:
        df = pl.read_parquet(file_path)

        if len(df) < 60:
            return None

        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        alt_cols = ['trade_date', 'open', 'high', 'low', 'close', 'volume']

        if not all(col in df.columns for col in required_cols):
            if all(col in df.columns for col in alt_cols):
                df = df.rename({'trade_date': 'date'})
            else:
                return None

        df = df.sort('date').tail(days)

        return df

    except Exception as e:
        logger.debug(f"[{code}] 加载K线失败: {e}")
        return None


def analyze_stock(code: str, df: pl.DataFrame) -> Optional[SwingSignal]:
    """分析单只股票"""
    if df is None or len(df) < 120:
        return None

    try:
        closes = df['close'].to_numpy()
        highs = df['high'].to_numpy()
        lows = df['low'].to_numpy()
        volumes = df['volume'].to_numpy()
        dates = df['date'].to_numpy() if 'date' in df.columns else df['trade_date'].to_numpy()

        current_price = closes[-1]
        prev_close = closes[-2] if len(closes) > 1 else current_price
        pct_chg = (current_price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0

        signal = SwingSignal(code=code)
        signal.close = current_price
        signal.pct_chg = pct_chg

        ema5 = _ema(closes, 5)
        ema10 = _ema(closes, 10)
        ema20 = _ema(closes, 20)
        ema60 = _ema(closes, 60)
        ema120 = _ema(closes, 120)

        signal.ema5 = ema5
        signal.ema10 = ema10
        signal.ema20 = ema20
        signal.ema60 = ema60
        signal.ema120 = ema120
        signal.price_vs_ema60 = (current_price - ema60) / ema60 * 100 if ema60 > 0 else 0.0

        macd_dif, macd_dea, macd_hist = _macd(closes)
        monthly_dif, monthly_dea, monthly_hist, golden_cross, above_zero = _monthly_macd(closes, dates)

        signal.macd_dif = macd_dif
        signal.macd_dea = macd_dea
        signal.macd_hist = macd_hist
        signal.macd_golden_cross = golden_cross
        signal.macd_above_zero = monthly_dif > 0

        has_limit_up, limit_up_idx, limit_up_vol = _detect_limit_up_day(volumes)

        if has_limit_up and limit_up_idx >= 0 and limit_up_idx < len(closes) - 1:
            pullback = _calculate_pullback(highs, limit_up_idx, len(closes) - 1)
            signal.pullback_pct = pullback

            if limit_up_idx < len(volumes) - 1:
                vol_after_limitup = np.mean(volumes[limit_up_idx + 1:])
                if vol_after_limitup > 0:
                    signal.volume_shrink_ratio = np.mean(volumes[-5:]) / vol_after_limitup

        signal.ema20_support = _check_ema20_support(closes, ema20)

        # 第一步：趋势确认
        # - 股价 > 60日均线（长期上升趋势）
        # - EMA多头排列（5>10>20）
        # - 月线MACD金叉（加分项，非必须）
        price_above_ema60 = current_price > ema60
        ema_bullish = ema5 > ema10 > ema20
        monthly_macd_golden = golden_cross

        signal.step1_passed = price_above_ema60 and ema_bullish

        # 第二步：回调识别（需要先有涨停）
        # - 涨停后出现回调（最高价回落 8-15%）
        # - 回调缩量（成交量萎缩至涨停量 30-50%）
        # - 在 20日均线附近获得支撑
        signal.step2_passed = (
            8.0 <= signal.pullback_pct <= 15.0 and
            signal.volume_shrink_ratio <= 0.5 and
            signal.ema20_support
        )

        signal.step3_passed = signal.step1_passed and signal.step2_passed

        # 第二步：回调识别
        # - 有涨停回调：最高价回落 8-15%，缩量，在20日均线获得支撑
        # - 无涨停但有正常回调：价格在20日均线获得支撑，RSI超卖后反弹
        has_limit_up_pullback = has_limit_up and limit_up_idx >= 0
        normal_pullback = signal.ema20_support and not has_limit_up_pullback

        if has_limit_up_pullback:
            signal.step2_passed = (
                8.0 <= signal.pullback_pct <= 15.0 and
                signal.volume_shrink_ratio <= 0.5
            )
        else:
            signal.step2_passed = normal_pullback

        signal.step3_passed = signal.step1_passed and signal.step2_passed

        if signal.step1_passed:
            signal.trend_score = 30.0
            if ema5 > ema10 > ema20 > ema60:
                signal.trend_score += 10.0
            if price_above_ema60:
                signal.trend_score += 10.0
            if monthly_macd_golden:
                signal.trend_score += 20.0
            elif signal.macd_above_zero:
                signal.trend_score += 5.0

        if signal.step2_passed:
            if has_limit_up_pullback:
                signal.pullback_score = 30.0
                if 10.0 <= signal.pullback_pct <= 12.0:
                    signal.pullback_score += 5.0
                if signal.volume_shrink_ratio <= 0.3:
                    signal.pullback_score += 5.0
            else:
                signal.pullback_score = 15.0

        if signal.step3_passed:
            signal.volume_score = 20.0

        signal.total_score = signal.trend_score + signal.pullback_score + signal.volume_score

        signal.confidence = min(signal.total_score, 100.0)

        if signal.step3_passed:
            signal.entry_price = current_price
            signal.stop_loss = current_price * 0.97
            signal.target_price = highs[limit_up_idx] if has_limit_up else current_price * 1.08
            signal.position_size = 50.0

        return signal

    except Exception as e:
        logger.debug(f"[{code}] 分析失败: {e}")
        return None


def run_swing_scan(min_confidence: float = 50.0, max_results: int = 20) -> List[SwingSignal]:
    """扫描所有股票寻找波段机会"""
    logger.info(f"开始波段扫描（最小置信度: {min_confidence}%）")

    parquet_files = list(DATA_DIR.glob("*.parquet"))
    logger.info(f"找到 {len(parquet_files)} 个K线文件")

    results: List[SwingSignal] = []
    processed = 0

    for f in parquet_files:
        code = f.stem
        df = load_kline_data(code)

        if df is None:
            continue

        signal = analyze_stock(code, df)

        if signal and signal.confidence >= min_confidence:
            results.append(signal)

        processed += 1
        if processed % 500 == 0:
            logger.info(f"已处理 {processed} 只股票...")

    results.sort(key=lambda x: x.confidence, reverse=True)

    logger.info(f"扫描完成，找到 {len(results)} 只候选股票")

    return results[:max_results]


def save_results(signals: List[SwingSignal], trade_date: str):
    """保存结果"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    json_path = OUTPUT_DIR / f"swing_{trade_date}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "date": trade_date,
            "count": len(signals),
            "signals": [s.to_dict() for s in signals]
        }, f, ensure_ascii=False, indent=2)

    logger.info(f"结果已保存到: {json_path}")

    if signals:
        df = pl.DataFrame([s.to_dict() for s in signals])
        csv_path = OUTPUT_DIR / f"swing_{trade_date}.csv"
        df.write_csv(csv_path)
        logger.info(f"CSV已保存到: {csv_path}")


def print_report(signals: List[SwingSignal], trade_date: str):
    """打印报告"""
    print("\n" + "=" * 80)
    print(f"📊 波段推荐报告 - {trade_date}")
    print("=" * 80)

    if not signals:
        print("\n⚠️ 未找到符合条件的波段机会")
        return

    step3_count = sum(1 for s in signals if s.step3_passed)
    step2_count = sum(1 for s in signals if s.step2_passed)

    print(f"\n📈 扫描结果: {len(signals)} 只候选 | "
          f"3步买信号: {step3_count} | "
          f"回调确认: {step2_count}")

    print("\n" + "-" * 80)
    print(f"{'代码':<8} {'名称':<10} {'现价':>8} {'涨幅%':>8} {'EMA20':>8} {'回调%':>8} {'缩量比':>8} {'置信度':>8}")
    print("-" * 80)

    for s in signals[:20]:
        name = s.name if s.name else "-"
        print(f"{s.code:<8} {name:<10} {s.close:>8.2f} {s.pct_chg:>8.2f} "
              f"{s.ema20:>8.2f} {s.pullback_pct:>8.1f} {s.volume_shrink_ratio:>8.2f} {s.confidence:>8.1f}")

    print("\n" + "=" * 80)
    print("📋 3步买详情（Top 5）")
    print("=" * 80)

    top5 = [s for s in signals if s.step3_passed][:5]

    for i, s in enumerate(top5, 1):
        print(f"\n{i}. {s.code} {'-' if not s.name else s.name}")
        print(f"   现价: {s.close:.2f} | 置信度: {s.confidence:.1f}%")
        print(f"   ✅ 第一步（趋势）: EMA多头排列={s.step1_passed} | MACD金叉={s.macd_golden_cross}")
        print(f"   ✅ 第二步（回调）: 回调{s.pullback_pct:.1f}% | 缩量比{s.volume_shrink_ratio:.2f} | EMA20支撑={s.ema20_support}")
        print(f"   📍 买入价: {s.entry_price:.2f} | 止损: {s.stop_loss:.2f} | 目标: {s.target_price:.2f}")
        print(f"   📊 评分: 趋势{s.trend_score:.0f} + 回调{s.pullback_score:.0f} + 量{s.volume_score:.0f} = {s.total_score:.0f}")


def main():
    parser = argparse.ArgumentParser(description="波段推荐系统 - 3步买战法")
    parser.add_argument("--min-confidence", type=float, default=50.0, help="最小置信度 (默认: 50)")
    parser.add_argument("--max-results", type=int, default=20, help="最大结果数 (默认: 20)")
    parser.add_argument("--date", type=str, default=None, help="交易日期 YYYYMMDD (默认: 今天)")

    args = parser.parse_args()

    trade_date = args.date or datetime.now().strftime('%Y%m%d')

    logger.info(f"=" * 60)
    logger.info(f"波段推荐系统启动 | 日期: {trade_date} | 最小置信度: {args.min_confidence}%")
    logger.info(f"=" * 60)

    signals = run_swing_scan(
        min_confidence=args.min_confidence,
        max_results=args.max_results
    )

    print_report(signals, trade_date)

    save_results(signals, trade_date)

    return signals


if __name__ == "__main__":
    main()
