"""
更新今日K线数据并刷新选股结果
功能：
1. 通过腾讯API拉取所有股票最新一个交易日的K线数据
2. 增量更新到 data/kline/*.parquet
3. 读取前一天的选股结果
4. 查询这些股票的最新收盘价、涨跌幅
5. 输出更新后的选股报告
"""
import sys
import os
import re
import json
import time
import logging
import argparse
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent


def fetch_latest_kline(code: str) -> Optional[Dict]:
    if code.startswith('6'):
        symbol = f'sh{code}'
    else:
        symbol = f'sz{code}'

    url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
    params = {
        '_var': f'kline_dayqfq_{symbol}',
        'param': f'{symbol},day,,,3,qfq',
        'r': str(int(time.time() * 1000))
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://gu.qq.com/'
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        text = r.text
        match = re.match(r'kline_dayqfq_\w+=(.*)', text)
        if match:
            data = json.loads(match.group(1))
            if data.get('code') == 0:
                klines = data['data'][symbol].get('qfqday', [])
                if klines:
                    latest = klines[-1]
                    return {
                        'code': code,
                        'trade_date': latest[0],
                        'open': float(latest[1]),
                        'close': float(latest[2]),
                        'high': float(latest[3]),
                        'low': float(latest[4]),
                        'volume': int(float(latest[5])),
                    }
    except Exception as e:
        logger.debug(f"获取 {code} 最新K线失败: {e}")
    return None


def update_parquet_file(code: str, new_row: Dict, kline_dir: Path) -> bool:
    parquet_file = kline_dir / f"{code}.parquet"

    if parquet_file.exists():
        try:
            existing = pl.read_parquet(parquet_file)
            existing_dates = set(existing["trade_date"].to_list())

            if new_row["trade_date"] in existing_dates:
                return False

            new_df = pl.DataFrame([new_row])
            merged = pl.concat([existing, new_df], how="diagonal")
            merged = merged.sort("trade_date")
            merged.write_parquet(parquet_file)
            return True
        except Exception as e:
            logger.warning(f"更新 {code} Parquet失败: {e}")
            return False
    else:
        try:
            new_df = pl.DataFrame([new_row])
            new_df.write_parquet(parquet_file)
            return True
        except Exception as e:
            logger.warning(f"创建 {code} Parquet失败: {e}")
            return False


def load_strategy_result(result_path: Path) -> Optional[Dict]:
    if not result_path.exists():
        return None

    try:
        with open(result_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"读取选股结果失败: {e}")
        return None


def get_stock_codes_from_kline(kline_dir: Path) -> List[str]:
    codes = []
    for f in sorted(kline_dir.glob("*.parquet")):
        codes.append(f.stem)
    return codes


def main():
    parser = argparse.ArgumentParser(description="更新今日K线数据并刷新选股结果")
    parser.add_argument("--kline-dir", type=str, default="data/kline", help="K线数据目录")
    parser.add_argument("--result-file", type=str, default="reports/strategy_result.json", help="选股结果文件")
    parser.add_argument("--output", type=str, default="reports/strategy_result_updated.json", help="更新后的输出文件")
    parser.add_argument("--delay", type=float, default=0.15, help="请求间隔(秒)")
    parser.add_argument("--target-codes-only", action="store_true", help="仅更新选股结果中的股票")

    args = parser.parse_args()

    kline_dir = project_root / args.kline_dir
    result_path = project_root / args.result_file
    output_path = project_root / args.output

    print("=" * 60)
    print(f"K线数据更新 & 选股结果刷新")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    strategy_result = load_strategy_result(result_path)

    if strategy_result:
        selected_codes = [s["code"] for s in strategy_result.get("stocks", [])]
        print(f"\n前一天选股结果: {len(selected_codes)} 只股票")
        print(f"策略: {strategy_result['strategy']['name']}")
        print(f"选股时间: {strategy_result.get('timestamp', 'N/A')}")
    else:
        print(f"\n未找到选股结果: {result_path}")
        selected_codes = []

    if args.target_codes_only and selected_codes:
        codes_to_update = selected_codes
        print(f"\n仅更新选股中的 {len(codes_to_update)} 只股票")
    else:
        codes_to_update = get_stock_codes_from_kline(kline_dir)
        print(f"\n更新全部 {len(codes_to_update)} 只股票")

    print("\n开始拉取最新K线数据...")

    updated_count = 0
    failed_count = 0
    skipped_count = 0
    latest_data: Dict[str, Dict] = {}

    for i, code in enumerate(codes_to_update):
        if (i + 1) % 100 == 0:
            print(f"  进度: {i + 1}/{len(codes_to_update)} (更新: {updated_count}, 跳过: {skipped_count})")

        new_row = fetch_latest_kline(code)

        if new_row is None:
            failed_count += 1
            continue

        latest_data[code] = new_row

        was_updated = update_parquet_file(code, new_row, kline_dir)

        if was_updated:
            updated_count += 1
        else:
            skipped_count += 1

        time.sleep(args.delay)

    print(f"\n更新完成:")
    print(f"  新增/更新: {updated_count} 只")
    print(f"  已是最新: {skipped_count} 只")
    print(f"  获取失败: {failed_count} 只")

    if strategy_result and selected_codes:
        print(f"\n{'=' * 60}")
        print("选股结果更新")
        print(f"{'=' * 60}")

        updated_stocks = []
        for stock in strategy_result.get("stocks", []):
            code = stock["code"]
            latest = latest_data.get(code)

            if latest:
                prev_close = stock.get("close", 0)
                curr_close = latest["close"]
                change_pct = ((curr_close - prev_close) / prev_close * 100) if prev_close > 0 else 0

                updated_stock = {**stock}
                updated_stock["prev_close"] = prev_close
                updated_stock["current_close"] = curr_close
                updated_stock["change_pct"] = round(change_pct, 2)
                updated_stock["current_date"] = latest["trade_date"]
                updated_stock["current_volume"] = latest["volume"]
                updated_stocks.append(updated_stock)
            else:
                updated_stock = {**stock}
                updated_stock["prev_close"] = stock.get("close", 0)
                updated_stock["current_close"] = None
                updated_stock["change_pct"] = None
                updated_stock["current_date"] = None
                updated_stock["current_volume"] = None
                updated_stocks.append(updated_stock)

        updated_result = {
            "timestamp": datetime.now().isoformat(),
            "original_timestamp": strategy_result.get("timestamp"),
            "strategy": strategy_result["strategy"],
            "update_summary": {
                "total": len(updated_stocks),
                "updated": sum(1 for s in updated_stocks if s["current_close"] is not None),
                "failed": sum(1 for s in updated_stocks if s["current_close"] is None),
            },
            "stocks": updated_stocks
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(updated_result, f, ensure_ascii=False, indent=2, default=str)

        print(f"\n选股结果已更新: {output_path}")

        print(f"\n{'─' * 60}")
        print(f"{'代码':<8} {'前收':>8} {'现价':>8} {'涨跌幅':>8} {'策略分':>8}")
        print(f"{'─' * 60}")

        for stock in updated_stocks[:30]:
            code = stock["code"]
            prev = stock.get("prev_close", 0)
            curr = stock.get("current_close")
            chg = stock.get("change_pct")
            score = stock.get("strategy_score", 0)

            if curr is not None and chg is not None:
                chg_str = f"{chg:+.2f}%"
                if chg > 0:
                    chg_str = f"\033[91m{chg_str}\033[0m"
                elif chg < 0:
                    chg_str = f"\033[92m{chg_str}\033[0m"
                print(f"{code:<8} {prev:>8.3f} {curr:>8.3f} {chg_str:>8} {score:>8.1f}")
            else:
                print(f"{code:<8} {prev:>8.3f} {'N/A':>8} {'N/A':>8} {score:>8.1f}")

        if len(updated_stocks) > 30:
            print(f"  ... 共 {len(updated_stocks)} 只股票")

    print(f"\n{'=' * 60}")
    print("全部完成")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
