#!/usr/bin/env python3
"""
数据新鲜度验证脚本
验证K线数据是否为当日收盘数据

使用方法:
    python scripts/pipeline/check_data_freshness.py           # 检查今日数据
    python scripts/pipeline/check_data_freshness.py --date 2026-04-16  # 检查指定日期
    python scripts/pipeline/check_data_freshness.py --threshold 0.8    # 设置阈值

退出码:
    0 - 数据新鲜度达标
    1 - 数据新鲜度不达标
    2 - 检查失败
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, List, Dict
import json

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
from core.trading_calendar import check_market_status, get_recent_trade_dates


class DataFreshnessChecker:
    """数据新鲜度检查器"""
    
    def __init__(self, target_date: str = None, threshold: float = 0.8):
        """
        初始化检查器
        
        Args:
            target_date: 目标日期 (YYYY-MM-DD)，默认为最近交易日
            threshold: 覆盖率阈值 (0-1)，默认80%
        """
        self.project_root = project_root
        self.kline_dir = project_root / "data" / "kline"
        self.threshold = threshold
        
        # 确定目标日期
        if target_date:
            self.target_date = target_date
        else:
            # 获取最近交易日
            trade_dates = get_recent_trade_dates(1)
            if trade_dates:
                self.target_date = trade_dates[0]
            else:
                self.target_date = datetime.now().strftime('%Y-%m-%d')
        
        self.results = {
            'target_date': self.target_date,
            'threshold': threshold,
            'total_stocks': 0,
            'fresh_stocks': 0,
            'stale_stocks': 0,
            'missing_stocks': 0,
            'coverage_rate': 0.0,
            'passed': False,
            'details': []
        }
    
    def check_freshness(self) -> Tuple[bool, Dict]:
        """
        检查数据新鲜度
        
        Returns:
            (是否通过, 详细结果)
        """
        print(f"=" * 60)
        print(f"数据新鲜度检查")
        print(f"目标日期: {self.target_date}")
        print(f"覆盖率阈值: {self.threshold * 100:.0f}%")
        print(f"=" * 60)
        
        if not self.kline_dir.exists():
            print(f"❌ K线数据目录不存在: {self.kline_dir}")
            return False, self.results
        
        # 获取所有股票文件
        parquet_files = list(self.kline_dir.glob("*.parquet"))
        self.results['total_stocks'] = len(parquet_files)
        
        print(f"\n检查 {len(parquet_files)} 只股票...")
        
        fresh_count = 0
        stale_count = 0
        missing_count = 0
        errors = []
        
        for i, parquet_file in enumerate(sorted(parquet_files)):
            code = parquet_file.stem
            
            try:
                # 读取parquet文件
                df = pl.read_parquet(parquet_file)
                
                if len(df) == 0:
                    missing_count += 1
                    errors.append(f"{code}: 数据为空")
                    continue
                
                # 获取最新日期
                latest_date = df['trade_date'].max()
                
                if latest_date == self.target_date:
                    fresh_count += 1
                else:
                    stale_count += 1
                    # 记录前10个过期股票
                    if len(errors) < 10:
                        errors.append(f"{code}: 最新日期 {latest_date} != 目标日期 {self.target_date}")
                
                # 进度显示
                if (i + 1) % 1000 == 0:
                    print(f"  已检查 {i + 1}/{len(parquet_files)} 只...")
                    
            except Exception as e:
                missing_count += 1
                if len(errors) < 10:
                    errors.append(f"{code}: 读取失败 - {str(e)}")
        
        # 计算覆盖率
        total_valid = fresh_count + stale_count
        coverage_rate = fresh_count / total_valid if total_valid > 0 else 0
        
        self.results['fresh_stocks'] = fresh_count
        self.results['stale_stocks'] = stale_count
        self.results['missing_stocks'] = missing_count
        self.results['coverage_rate'] = coverage_rate
        self.results['passed'] = coverage_rate >= self.threshold
        
        # 输出结果
        print(f"\n" + "=" * 60)
        print(f"检查结果")
        print(f"=" * 60)
        print(f"总股票数: {self.results['total_stocks']}")
        print(f"数据新鲜: {fresh_count} 只 ({fresh_count/len(parquet_files)*100:.1f}%)")
        print(f"数据过期: {stale_count} 只 ({stale_count/len(parquet_files)*100:.1f}%)")
        print(f"读取失败: {missing_count} 只 ({missing_count/len(parquet_files)*100:.1f}%)")
        print(f"\n有效覆盖率: {coverage_rate*100:.1f}%")
        print(f"阈值要求: {self.threshold*100:.0f}%")
        
        if errors:
            print(f"\n问题样本 (前10个):")
            for error in errors:
                print(f"  - {error}")
        
        if self.results['passed']:
            print(f"\n✅ 数据新鲜度检查通过")
        else:
            print(f"\n❌ 数据新鲜度检查未通过")
        
        return self.results['passed'], self.results
    
    def save_report(self):
        """保存检查报告"""
        report_file = self.project_root / "logs" / f"data_freshness_{self.target_date}.json"
        report_file.parent.mkdir(exist_ok=True)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        print(f"\n报告已保存: {report_file}")


def main():
    parser = argparse.ArgumentParser(description='数据新鲜度检查')
    parser.add_argument('--date', type=str, help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--threshold', type=float, default=0.8, help='覆盖率阈值 (0-1)，默认0.8')
    parser.add_argument('--quiet', action='store_true', help='静默模式，只输出结果')
    
    args = parser.parse_args()
    
    # 检查是否为交易日
    market_status = check_market_status()
    if not market_status['is_trading_day']:
        print("非交易日，跳过数据新鲜度检查")
        sys.exit(0)
    
    checker = DataFreshnessChecker(
        target_date=args.date,
        threshold=args.threshold
    )
    
    passed, results = checker.check_freshness()
    checker.save_report()
    
    # 根据检查结果返回退出码
    if passed:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
