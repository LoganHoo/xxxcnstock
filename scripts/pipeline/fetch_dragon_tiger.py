#!/usr/bin/env python3
"""
龙虎榜数据独立采集脚本
特点：
1. 延迟数据标记 - 龙虎榜16:30-17:30陆续发布
2. 增量采集模式 - 多次检查直到数据完整
3. 可选数据标记 - 不影响主流程审计

使用方法:
    python scripts/pipeline/fetch_dragon_tiger.py              # 首次采集
    python scripts/pipeline/fetch_dragon_tiger.py --incremental  # 增量检查
    python scripts/pipeline/fetch_dragon_tiger.py --max-wait 3600  # 最大等待时间
"""
import sys
import os
import argparse
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from core.trading_calendar import get_recent_trade_dates


class DragonTigerFetcher:
    """龙虎榜数据获取器"""

    def __init__(self, target_date: str = None, max_wait: int = 3600):
        self.project_root = project_root
        self.data_dir = project_root / "data" / "dragon_tiger"
        self.data_dir.mkdir(exist_ok=True)

        # 确定目标日期
        if target_date:
            self.target_date = target_date
        else:
            trade_dates = get_recent_trade_dates(1)
            self.target_date = trade_dates[0] if trade_dates else datetime.now().strftime('%Y-%m-%d')

        self.max_wait = max_wait
        self.results = {
            'fetch_time': datetime.now().isoformat(),
            'target_date': self.target_date,
            'status': 'pending',
            'records': 0,
            'attempts': 0,
            'optional': True  # 标记为可选数据
        }

    def _log(self, message: str, level: str = 'info'):
        """输出日志"""
        prefix = {'info': 'ℹ️', 'warning': '⚠️', 'error': '❌', 'success': '✅'}.get(level, 'ℹ️')
        print(f"{prefix} {message}")

    def fetch_dragon_tiger(self) -> bool:
        """
        获取龙虎榜数据
        实际实现时需要接入tushare或akshare等数据源
        """
        try:
            # 这里使用模拟数据，实际使用时接入真实数据源
            # 例如：
            # import tushare as ts
            # pro = ts.pro_api()
            # df = pro.top_list(trade_date=self.target_date.replace('-', ''))

            # 模拟：检查是否有数据文件
            data_file = self.data_dir / f"{self.target_date}.csv"

            if data_file.exists():
                df = pd.read_csv(data_file)
                self.results['records'] = len(df)
                self.results['status'] = 'success'
                self._log(f"龙虎榜数据已存在: {len(df)}条记录")
                return True

            # 模拟延迟数据：16:30-17:30之间可能还没有数据
            current_time = datetime.now()
            if current_time.hour < 16 or (current_time.hour == 16 and current_time.minute < 45):
                self.results['status'] = 'pending'
                self._log("龙虎榜数据尚未发布，等待中...", 'warning')
                return False

            # 模拟生成数据（实际使用时替换为真实API调用）
            mock_data = self._generate_mock_data()
            if mock_data:
                mock_data.to_csv(data_file, index=False)
                self.results['records'] = len(mock_data)
                self.results['status'] = 'success'
                self._log(f"龙虎榜数据获取成功: {len(mock_data)}条记录", 'success')
                return True

            return False

        except Exception as e:
            self.results['status'] = 'error'
            self.results['error'] = str(e)
            self._log(f"获取龙虎榜数据失败: {e}", 'error')
            return False

    def _generate_mock_data(self) -> Optional[pd.DataFrame]:
        """生成模拟数据（实际使用时删除）"""
        # 模拟龙虎榜数据
        data = {
            'trade_date': [self.target_date] * 10,
            'code': ['000001', '000002', '600000', '600001', '300001',
                     '300002', '000858', '600519', '002415', '000333'],
            'name': ['平安银行', '万科A', '浦发银行', '邯郸钢铁', '特锐德',
                     '神州泰岳', '五粮液', '贵州茅台', '海康威视', '美的集团'],
            'close': [10.5, 15.2, 8.8, 5.5, 25.3, 18.6, 158.0, 1680.0, 32.5, 58.8],
            'pct_change': [10.02, -9.98, 10.05, -9.95, 20.01, -19.98, 7.5, 3.2, 5.8, 4.5],
            'turnover': [15.5, 12.3, 8.8, 22.1, 35.6, 28.9, 5.2, 2.1, 8.8, 6.5],
            'amount': [25.5, 18.3, 12.8, 8.5, 15.2, 12.1, 45.8, 68.5, 32.5, 28.9],
            'buy_amount': [12.5, 3.2, 8.5, 1.5, 10.2, 2.1, 25.8, 35.5, 18.5, 15.9],
            'sell_amount': [3.2, 12.5, 2.1, 5.8, 3.5, 8.8, 12.5, 18.5, 10.2, 8.5]
        }
        return pd.DataFrame(data)

    def incremental_fetch(self) -> bool:
        """
        增量采集模式
        多次检查直到数据完整或超过最大等待时间
        """
        self._log(f"开始增量采集龙虎榜数据 (目标日期: {self.target_date})")
        print(f"最大等待时间: {self.max_wait}秒")
        print("-" * 60)

        start_time = time.time()
        attempt = 0

        while time.time() - start_time < self.max_wait:
            attempt += 1
            self.results['attempts'] = attempt

            self._log(f"第 {attempt} 次尝试...")

            if self.fetch_dragon_tiger():
                self._save_status()
                return True

            # 等待10分钟后再次尝试
            if time.time() - start_time + 600 < self.max_wait:
                self._log("等待10分钟后再次尝试...")
                time.sleep(600)  #  sleep 10 minutes
            else:
                break

        # 超过最大等待时间
        self.results['status'] = 'timeout'
        self._log(f"超过最大等待时间 {self.max_wait}秒，标记为数据缺失", 'warning')
        self._save_status()
        return False

    def _save_status(self):
        """保存采集状态"""
        status_file = self.data_dir / f"status_{self.target_date}.json"
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        self._log(f"状态已保存: {status_file}")

    def get_status(self) -> Dict:
        """获取当前状态"""
        status_file = self.data_dir / f"status_{self.target_date}.json"
        if status_file.exists():
            with open(status_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return self.results


def main():
    parser = argparse.ArgumentParser(description='龙虎榜数据独立采集')
    parser.add_argument('--date', type=str, help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--incremental', action='store_true', help='增量采集模式')
    parser.add_argument('--max-wait', type=int, default=3600, help='最大等待时间(秒)，默认3600')
    parser.add_argument('--status', action='store_true', help='仅查询状态')

    args = parser.parse_args()

    fetcher = DragonTigerFetcher(
        target_date=args.date,
        max_wait=args.max_wait
    )

    if args.status:
        status = fetcher.get_status()
        print(json.dumps(status, ensure_ascii=False, indent=2))
        sys.exit(0)

    if args.incremental:
        success = fetcher.incremental_fetch()
    else:
        success = fetcher.fetch_dragon_tiger()

    # 返回码：0=成功, 1=失败但可继续, 2=完全失败
    if success:
        sys.exit(0)
    elif fetcher.results['status'] == 'timeout':
        sys.exit(1)  # 超时但可继续
    else:
        sys.exit(2)  # 失败


if __name__ == "__main__":
    main()
