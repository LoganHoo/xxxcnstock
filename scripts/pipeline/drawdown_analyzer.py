#!/usr/bin/env python3
"""
回撤分析模块 - 避免幸存者偏差
功能：
1. 分析昨日选股中跌幅最大的股票（回撤之最）
2. 区分系统性风险 vs 因子失效
3. 生成教训总结

使用方法:
    python scripts/pipeline/drawdown_analyzer.py --date 2026-04-16
"""
import sys
import os
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
import pandas as pd
from core.trading_calendar import get_recent_trade_dates


@dataclass
class DrawdownStock:
    """回撤股票数据类"""
    code: str
    name: str
    pick_date: str
    pick_price: float
    current_price: float
    drawdown: float
    market_return: float
    factor_scores: Dict[str, float]


class DrawdownAnalyzer:
    """回撤分析器"""

    def __init__(self, target_date: str = None):
        self.project_root = project_root
        self.kline_dir = project_root / "data" / "kline"
        self.picks_dir = project_root / "data" / "picks"

        # 确定目标日期（分析前一天的选股）
        if target_date:
            self.target_date = target_date
        else:
            trade_dates = get_recent_trade_dates(2)
            self.target_date = trade_dates[1] if len(trade_dates) >= 2 else None

        self.results = {
            'analysis_date': datetime.now().isoformat(),
            'target_date': self.target_date,
            'max_losers': [],
            'systematic_vs_idiosyncratic': {},
            'factor_failure_analysis': {},
            'lessons': []
        }

    def _log(self, message: str, level: str = 'info'):
        """输出日志"""
        prefix = {'info': 'ℹ️', 'warning': '⚠️', 'error': '❌', 'success': '✅'}.get(level, 'ℹ️')
        print(f"{prefix} {message}")

    def load_yesterday_picks(self) -> pd.DataFrame:
        """加载昨日选股"""
        if not self.target_date:
            return pd.DataFrame()

        picks_file = self.picks_dir / f"{self.target_date}.csv"
        if picks_file.exists():
            return pd.read_csv(picks_file)

        # 尝试其他格式
        picks_file = self.picks_dir / f"picks_{self.target_date}.csv"
        if picks_file.exists():
            return pd.read_csv(picks_file)

        return pd.DataFrame()

    def calculate_drawdowns(self) -> List[DrawdownStock]:
        """计算回撤数据"""
        self._log(f"分析 {self.target_date} 选股回撤情况")

        picks = self.load_yesterday_picks()
        if picks.empty:
            self._log("未找到昨日选股数据", 'warning')
            return []

        drawdowns = []

        for _, row in picks.iterrows():
            code = str(row.get('code', '')).zfill(6)

            # 读取K线数据
            kline_file = self.kline_dir / f"{code}.parquet"
            if not kline_file.exists():
                continue

            try:
                df = pl.read_parquet(kline_file)
                if len(df) < 2:
                    continue

                # 获取选股日和当前价格
                pick_data = df.filter(pl.col('trade_date') == self.target_date)
                if len(pick_data) == 0:
                    continue

                pick_price = pick_data['close'][0]

                # 获取最新价格（假设是今天）
                latest_data = df.sort('trade_date').tail(1)
                current_price = latest_data['close'][0]

                # 计算回撤
                drawdown = (current_price - pick_price) / pick_price * 100

                # 获取因子分数
                factor_scores = {}
                for col in ['momentum', 'volume', 'fund_flow', 'technical']:
                    if col in row:
                        factor_scores[col] = float(row[col])

                drawdowns.append(DrawdownStock(
                    code=code,
                    name=row.get('name', code),
                    pick_date=self.target_date,
                    pick_price=pick_price,
                    current_price=current_price,
                    drawdown=drawdown,
                    market_return=0.0,  # 需要计算大盘收益
                    factor_scores=factor_scores
                ))

            except Exception as e:
                self._log(f"处理 {code} 时出错: {e}", 'warning')
                continue

        # 按回撤排序（最差的在前）
        drawdowns.sort(key=lambda x: x.drawdown)
        return drawdowns

    def analyze_max_losers(self, drawdowns: List[DrawdownStock], top_n: int = 3) -> List[Dict]:
        """分析回撤最大的股票"""
        if not drawdowns:
            return []

        max_losers = []
        for stock in drawdowns[:top_n]:
            # 分析原因
            reasons = self._analyze_drawdown_reason(stock)

            max_losers.append({
                'code': stock.code,
                'name': stock.name,
                'drawdown': round(stock.drawdown, 2),
                'pick_price': stock.pick_price,
                'current_price': stock.current_price,
                'reasons': reasons,
                'factor_scores': stock.factor_scores
            })

        return max_losers

    def _analyze_drawdown_reason(self, stock: DrawdownStock) -> List[str]:
        """分析回撤原因"""
        reasons = []

        # 检查回撤幅度
        if stock.drawdown < -9:
            reasons.append("跌停或接近跌停")
        elif stock.drawdown < -5:
            reasons.append("大幅下跌")

        # 检查因子失效
        if stock.factor_scores:
            high_momentum = stock.factor_scores.get('momentum', 0) > 80
            if high_momentum and stock.drawdown < -5:
                reasons.append("高动量因子失效")

            low_volume = stock.factor_scores.get('volume', 0) < 30
            if low_volume:
                reasons.append("流动性不足")

        return reasons

    def analyze_systematic_vs_idiosyncratic(self, drawdowns: List[DrawdownStock]) -> Dict:
        """分析系统性风险 vs 个股特有风险"""
        if not drawdowns:
            return {}

        # 计算平均回撤
        avg_drawdown = sum(d.drawdown for d in drawdowns) / len(drawdowns)

        # 统计大幅回撤的数量
        large_drawdown_count = sum(1 for d in drawdowns if d.drawdown < -5)
        large_drawdown_ratio = large_drawdown_count / len(drawdowns)

        # 判断系统性风险
        if large_drawdown_ratio > 0.5:
            systematic_ratio = 0.7
            conclusion = "系统性风险主导"
        elif large_drawdown_ratio > 0.3:
            systematic_ratio = 0.5
            conclusion = "系统性风险与个股风险并存"
        else:
            systematic_ratio = 0.3
            conclusion = "个股特有风险主导"

        return {
            'average_drawdown': round(avg_drawdown, 2),
            'large_drawdown_count': large_drawdown_count,
            'large_drawdown_ratio': round(large_drawdown_ratio, 2),
            'systematic_ratio': systematic_ratio,
            'idiosyncratic_ratio': 1 - systematic_ratio,
            'conclusion': conclusion
        }

    def analyze_factor_failure(self, drawdowns: List[DrawdownStock]) -> Dict:
        """分析因子失效情况"""
        if not drawdowns:
            return {}

        factor_performance = {}

        for factor_name in ['momentum', 'volume', 'fund_flow', 'technical']:
            factor_stocks = [d for d in drawdowns if factor_name in d.factor_scores]
            if not factor_stocks:
                continue

            # 计算该因子高分股票的平均回撤
            high_score_stocks = [d for d in factor_stocks if d.factor_scores[factor_name] > 70]
            if high_score_stocks:
                avg_drawdown = sum(d.drawdown for d in high_score_stocks) / len(high_score_stocks)
                factor_performance[factor_name] = {
                    'high_score_count': len(high_score_stocks),
                    'average_drawdown': round(avg_drawdown, 2),
                    'failure_detected': avg_drawdown < -3
                }

        return factor_performance

    def generate_lessons(self, max_losers: List[Dict], systematic_analysis: Dict) -> List[str]:
        """生成教训总结"""
        lessons = []

        # 从最大回撤股票中学习
        for loser in max_losers:
            if loser['drawdown'] < -7:
                lessons.append(
                    f"{loser['name']}({loser['code']})回撤{loser['drawdown']}%，"
                    f"原因：{', '.join(loser['reasons'])}"
                )

        # 从系统性分析中学习
        if systematic_analysis.get('systematic_ratio', 0) > 0.5:
            lessons.append(
                "今日大盘系统性风险较高，选股策略应增加防御性因子权重"
            )

        # 从因子失效中学习
        for factor, perf in self.results.get('factor_failure_analysis', {}).items():
            if perf.get('failure_detected'):
                lessons.append(
                    f"{factor}因子今日表现不佳，高{factor}股票平均回撤{perf['average_drawdown']}%，"
                    f"建议调整该因子权重"
                )

        return lessons

    def run_analysis(self) -> Dict:
        """运行完整分析"""
        print("=" * 70)
        print("回撤分析 - 避免幸存者偏差")
        print("=" * 70)
        print(f"分析日期: {self.target_date}")
        print()

        # 1. 计算回撤
        drawdowns = self.calculate_drawdowns()
        if not drawdowns:
            self._log("无数据可分析", 'warning')
            return self.results

        print(f"共分析 {len(drawdowns)} 只昨日选股")
        print()

        # 2. 分析回撤最大的股票
        self._log("分析回撤最大的股票...")
        self.results['max_losers'] = self.analyze_max_losers(drawdowns)

        print("\n📉 回撤之最（Top 3）:")
        for i, loser in enumerate(self.results['max_losers'], 1):
            print(f"   {i}. {loser['name']}({loser['code']}): {loser['drawdown']}%")
            print(f"      原因: {', '.join(loser['reasons'])}")
        print()

        # 3. 系统性风险分析
        self._log("分析系统性风险...")
        self.results['systematic_vs_idiosyncratic'] = self.analyze_systematic_vs_idiosyncratic(drawdowns)

        sys_ratio = self.results['systematic_vs_idiosyncratic']
        print(f"\n📊 风险归因:")
        print(f"   系统性风险占比: {sys_ratio.get('systematic_ratio', 0) * 100:.0f}%")
        print(f"   个股特有风险占比: {sys_ratio.get('idiosyncratic_ratio', 0) * 100:.0f}%")
        print(f"   结论: {sys_ratio.get('conclusion', 'N/A')}")
        print()

        # 4. 因子失效分析
        self._log("分析因子失效情况...")
        self.results['factor_failure_analysis'] = self.analyze_factor_failure(drawdowns)

        print(f"\n🔍 因子表现:")
        for factor, perf in self.results['factor_failure_analysis'].items():
            status = "⚠️ 失效" if perf['failure_detected'] else "✅ 正常"
            print(f"   {factor}: 平均回撤 {perf['average_drawdown']}% {status}")
        print()

        # 5. 生成教训
        self.results['lessons'] = self.generate_lessons(
            self.results['max_losers'],
            self.results['systematic_vs_idiosyncratic']
        )

        print(f"\n💡 教训总结:")
        for i, lesson in enumerate(self.results['lessons'], 1):
            print(f"   {i}. {lesson}")
        print()

        # 保存结果
        self._save_results()

        return self.results

    def _save_results(self):
        """保存分析结果"""
        report_dir = self.project_root / "logs" / "drawdown_analysis"
        report_dir.mkdir(exist_ok=True)

        report_file = report_dir / f"{self.target_date}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        self._log(f"分析结果已保存: {report_file}")


def main():
    parser = argparse.ArgumentParser(description='回撤分析 - 避免幸存者偏差')
    parser.add_argument('--date', type=str, help='目标日期 (YYYY-MM-DD)，分析前一天的选股')
    parser.add_argument('--output', type=str, help='输出文件路径')

    args = parser.parse_args()

    analyzer = DrawdownAnalyzer(target_date=args.date)
    results = analyzer.run_analysis()

    # 输出JSON
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))

    sys.exit(0)


if __name__ == "__main__":
    main()
