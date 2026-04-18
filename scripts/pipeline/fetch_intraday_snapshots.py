#!/usr/bin/env python3
"""
盘中分时快照采集脚本
功能：
1. 采集关键时点的分时数据（09:35, 10:30, 11:30, 14:00, 14:10, 14:30, 15:00）
2. 检测盘中异动（V转、脉冲、跳水、资金切换）
3. 为复盘报告提供盘中动态分析

使用方法:
    python scripts/pipeline/fetch_intraday_snapshots.py --date 2026-04-16
    python scripts/pipeline/fetch_intraday_snapshots.py --detect-anomalies
"""
import sys
import os
import argparse
import json
from pathlib import Path
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from core.trading_calendar import get_recent_trade_dates


@dataclass
class IntradaySnapshot:
    """分时快照数据类"""
    code: str
    name: str
    time: str
    price: float
    volume: int
    amount: float
    change_pct: float


@dataclass
class AnomalyEvent:
    """异动事件数据类"""
    time: str
    type: str
    description: str
    affected_sectors: List[str]
    volume_spike: float
    price_change: float
    confidence: float


class IntradaySnapshotFetcher:
    """盘中分时快照获取器"""

    # 关键时点（盘中重要观察点）
    KEY_TIMEPOINTS = [
        "09:35",  # 开盘5分钟（观察开盘情绪）
        "10:30",  # 早盘中段
        "11:30",  # 午盘收盘
        "14:00",  # 午后开盘
        "14:10",  # 午后10分钟（观察午后情绪）
        "14:30",  # 尾盘前
        "15:00",  # 收盘
    ]

    def __init__(self, target_date: str = None):
        self.project_root = project_root
        self.data_dir = project_root / "data" / "intraday"
        self.data_dir.mkdir(exist_ok=True)

        if target_date:
            self.target_date = target_date
        else:
            trade_dates = get_recent_trade_dates(1)
            self.target_date = trade_dates[0] if trade_dates else datetime.now().strftime('%Y-%m-%d')

        self.results = {
            'fetch_time': datetime.now().isoformat(),
            'target_date': self.target_date,
            'snapshots': {},
            'anomalies': []
        }

    def _log(self, message: str, level: str = 'info'):
        """输出日志"""
        prefix = {'info': 'ℹ️', 'warning': '⚠️', 'error': '❌', 'success': '✅'}.get(level, 'ℹ️')
        print(f"{prefix} {message}")

    def fetch_snapshot(self, timepoint: str) -> Dict[str, IntradaySnapshot]:
        """
        获取指定时点的快照数据
        实际实现时需要接入实时行情API
        """
        self._log(f"获取 {timepoint} 时点快照...")

        # 模拟数据（实际使用时替换为真实API）
        snapshots = {}

        # 读取股票列表
        stock_list_file = self.project_root / "data" / "stock_list.csv"
        if stock_list_file.exists():
            df = pd.read_csv(stock_list_file)
            # 模拟前50只股票的快照
            for _, row in df.head(50).iterrows():
                code = str(row['code']).zfill(6)
                # 模拟价格变动
                base_price = 10.0
                change_pct = np.random.normal(0, 2)  # 随机涨跌幅
                price = base_price * (1 + change_pct / 100)

                snapshots[code] = IntradaySnapshot(
                    code=code,
                    name=row.get('name', code),
                    time=timepoint,
                    price=round(price, 2),
                    volume=np.random.randint(100000, 1000000),
                    amount=round(price * np.random.randint(100000, 1000000), 2),
                    change_pct=round(change_pct, 2)
                )

        self._log(f"获取到 {len(snapshots)} 只股票的 {timepoint} 快照")
        return snapshots

    def fetch_all_snapshots(self) -> Dict:
        """获取所有关键时点快照"""
        self._log(f"开始采集 {self.target_date} 的盘中分时快照")
        print("-" * 60)

        for timepoint in self.KEY_TIMEPOINTS:
            snapshots = self.fetch_snapshot(timepoint)
            self.results['snapshots'][timepoint] = {
                code: asdict(snapshot) for code, snapshot in snapshots.items()
            }

        self._save_snapshots()
        return self.results['snapshots']

    def detect_anomalies(self) -> List[AnomalyEvent]:
        """
        检测盘中异动
        基于相邻时点的价格、成交量变化检测异常
        """
        self._log("检测盘中异动...")

        anomalies = []
        snapshots = self.results['snapshots']

        if len(snapshots) < 2:
            return anomalies

        timepoints = sorted(snapshots.keys())

        for i in range(1, len(timepoints)):
            prev_time = timepoints[i - 1]
            curr_time = timepoints[i]

            prev_data = snapshots[prev_time]
            curr_data = snapshots[curr_time]

            # 检测V型反转
            v_reversal = self._detect_v_reversal(prev_data, curr_data)
            if v_reversal:
                anomalies.append(v_reversal)

            # 检测脉冲上涨
            spike = self._detect_spike(prev_data, curr_data)
            if spike:
                anomalies.append(spike)

            # 检测跳水
            plunge = self._detect_plunge(prev_data, curr_data)
            if plunge:
                anomalies.append(plunge)

            # 检测资金切换
            switch = self._detect_sector_switch(prev_data, curr_data)
            if switch:
                anomalies.append(switch)

        self.results['anomalies'] = [asdict(a) for a in anomalies]
        return anomalies

    def _detect_v_reversal(self, prev_data: Dict, curr_data: Dict) -> Optional[AnomalyEvent]:
        """检测V型反转"""
        # 简化的V型检测逻辑
        # 实际使用时需要更复杂的算法
        return None

    def _detect_spike(self, prev_data: Dict, curr_data: Dict) -> Optional[AnomalyEvent]:
        """检测脉冲上涨"""
        # 检测价格快速上涨
        price_changes = []
        for code in set(prev_data.keys()) & set(curr_data.keys()):
            if 'change_pct' in prev_data[code] and 'change_pct' in curr_data[code]:
                change = curr_data[code]['change_pct'] - prev_data[code]['change_pct']
                price_changes.append((code, change))

        # 如果有超过10%的股票上涨超过3%，认为是脉冲
        spike_stocks = [c for c, ch in price_changes if ch > 3]
        if len(spike_stocks) > len(price_changes) * 0.1:
            return AnomalyEvent(
                time=curr_data[list(curr_data.keys())[0]]['time'],
                type="脉冲上涨",
                description=f"{len(spike_stocks)}只股票快速上涨超过3%",
                affected_sectors=["待分析"],
                volume_spike=1.5,
                price_change=sum(ch for _, ch in price_changes if ch > 3) / len(spike_stocks),
                confidence=0.7
            )
        return None

    def _detect_plunge(self, prev_data: Dict, curr_data: Dict) -> Optional[AnomalyEvent]:
        """检测跳水"""
        price_changes = []
        for code in set(prev_data.keys()) & set(curr_data.keys()):
            if 'change_pct' in prev_data[code] and 'change_pct' in curr_data[code]:
                change = curr_data[code]['change_pct'] - prev_data[code]['change_pct']
                price_changes.append((code, change))

        # 如果有超过10%的股票下跌超过3%，认为是跳水
        plunge_stocks = [c for c, ch in price_changes if ch < -3]
        if len(plunge_stocks) > len(price_changes) * 0.1:
            return AnomalyEvent(
                time=curr_data[list(curr_data.keys())[0]]['time'],
                type="盘中跳水",
                description=f"{len(plunge_stocks)}只股票快速下跌超过3%",
                affected_sectors=["待分析"],
                volume_spike=2.0,
                price_change=sum(ch for _, ch in price_changes if ch < -3) / len(plunge_stocks),
                confidence=0.8
            )
        return None

    def _detect_sector_switch(self, prev_data: Dict, curr_data: Dict) -> Optional[AnomalyEvent]:
        """检测资金切换"""
        # 简化的资金切换检测
        # 实际使用时需要板块数据
        return None

    def generate_intraday_report(self) -> Dict:
        """生成盘中动态报告"""
        report = {
            'date': self.target_date,
            'key_timepoints': self.KEY_TIMEPOINTS,
            'snapshot_summary': {},
            'anomalies': self.results['anomalies'],
            'market_dynamics': {}
        }

        # 统计每个时点的市场状态
        for timepoint, data in self.results['snapshots'].items():
            if not data:
                continue

            changes = [d['change_pct'] for d in data.values() if 'change_pct' in d]
            if changes:
                report['snapshot_summary'][timepoint] = {
                    'avg_change': round(sum(changes) / len(changes), 2),
                    'max_change': round(max(changes), 2),
                    'min_change': round(min(changes), 2),
                    'up_count': sum(1 for c in changes if c > 0),
                    'down_count': sum(1 for c in changes if c < 0)
                }

        # 市场动态总结
        if self.results['anomalies']:
            report['market_dynamics']['notable_events'] = len(self.results['anomalies'])
            report['market_dynamics']['summary'] = self._summarize_dynamics()

        return report

    def _summarize_dynamics(self) -> str:
        """总结市场动态"""
        if not self.results['anomalies']:
            return "今日市场走势平稳，未检测到明显异动"

        summaries = []
        for anomaly in self.results['anomalies']:
            summaries.append(f"{anomaly['time']} {anomaly['type']}: {anomaly['description']}")

        return "; ".join(summaries)

    def _save_snapshots(self):
        """保存快照数据"""
        snapshot_file = self.data_dir / f"{self.target_date}_snapshots.json"
        with open(snapshot_file, 'w', encoding='utf-8') as f:
            json.dump(self.results['snapshots'], f, ensure_ascii=False, indent=2)
        self._log(f"快照数据已保存: {snapshot_file}")

    def save_report(self):
        """保存分析报告"""
        report = self.generate_intraday_report()
        report_file = self.data_dir / f"{self.target_date}_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self._log(f"分析报告已保存: {report_file}")
        return report


def main():
    parser = argparse.ArgumentParser(description='盘中分时快照采集')
    parser.add_argument('--date', type=str, help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--timepoints', type=str, help='自定义时点，逗号分隔 (如: 09:35,10:30)')
    parser.add_argument('--detect-anomalies', action='store_true', help='检测异动')
    parser.add_argument('--output', type=str, help='输出文件路径')

    args = parser.parse_args()

    fetcher = IntradaySnapshotFetcher(target_date=args.date)

    # 获取快照
    fetcher.fetch_all_snapshots()

    # 检测异动
    if args.detect_anomalies:
        anomalies = fetcher.detect_anomalies()
        print(f"\n检测到 {len(anomalies)} 个异动事件:")
        for anomaly in anomalies:
            print(f"  {anomaly.time} - {anomaly.type}: {anomaly.description}")

    # 生成报告
    report = fetcher.save_report()

    # 输出
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    else:
        print("\n" + "=" * 60)
        print("盘中动态报告")
        print("=" * 60)
        print(json.dumps(report, ensure_ascii=False, indent=2))

    sys.exit(0)


if __name__ == "__main__":
    main()
