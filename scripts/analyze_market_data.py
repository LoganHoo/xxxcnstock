#!/usr/bin/env python3
"""
分析每日市场统计数据 - 从K线数据计算真实市场指标
"""
import sys
from pathlib import Path
import polars as pl

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def calculate_market_stats(target_date: str) -> dict:
    """从K线数据计算指定日期的市场统计"""
    kline_dir = project_root / "data" / "kline"

    rising = falling = flat = limit_up = limit_down = 0
    total_volume = 0.0
    total_stocks = 0

    for parquet_file in kline_dir.glob("*.parquet"):
        try:
            df = pl.read_parquet(parquet_file)
            day_data = df.filter(pl.col('trade_date') == target_date)

            if day_data.height > 0:
                total_stocks += 1
                row = day_data.to_dicts()[0]
                prev_close = row.get('preclose', row.get('open', 0))
                close = row.get('close', 0)
                volume = row.get('volume', 0)

                if prev_close > 0:
                    change_pct = (close - prev_close) / prev_close * 100
                    total_volume += volume * close / 100000000  # 转换为亿

                    if change_pct >= 9.9:
                        limit_up += 1
                        rising += 1
                    elif change_pct <= -9.9:
                        limit_down += 1
                        falling += 1
                    elif change_pct > 0.5:
                        rising += 1
                    elif change_pct < -0.5:
                        falling += 1
                    else:
                        flat += 1
        except Exception:
            pass

    total = rising + falling + flat
    if total > 0:
        if rising / total > 0.6:
            market_status = "强势上涨"
        elif falling / total > 0.6:
            market_status = "弱势下跌"
        else:
            market_status = "震荡整理"
    else:
        market_status = "数据不足"

    return {
        'date': target_date,
        'rising_count': rising,
        'falling_count': falling,
        'flat_count': flat,
        'limit_up_count': limit_up,
        'limit_down_count': limit_down,
        'turnover': round(total_volume, 2),
        'market_status': market_status,
        'total_stocks': total_stocks
    }


def main():
    """主函数"""
    from datetime import datetime, timedelta

    # 检查4月1日到4月17日的数据
    start_date = datetime(2026, 4, 1)
    end_date = datetime(2026, 4, 17)

    print("=" * 70)
    print("每日市场统计数据（从K线数据计算）")
    print("=" * 70)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        # 跳过周末
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue

        stats = calculate_market_stats(date_str)

        print(f"\n【{date_str}】")
        print(f"  上涨: {stats['rising_count']}只")
        print(f"  下跌: {stats['falling_count']}只")
        print(f"  平盘: {stats['flat_count']}只")
        print(f"  涨停: {stats['limit_up_count']}只")
        print(f"  跌停: {stats['limit_down_count']}只")
        print(f"  成交额: {stats['turnover']}亿")
        print(f"  市场状态: {stats['market_status']}")
        print(f"  统计股票数: {stats['total_stocks']}")

        current_date += timedelta(days=1)

    print("=" * 70)


if __name__ == "__main__":
    main()
