#!/usr/bin/env python3
"""
智能数据审计脚本 - 带熔断机制
改进点：
1. 最大重试次数限制（默认5次）
2. 截止死线检查（默认17:20）
3. 熔断机制：超过阈值后使用降级模式
4. 审计看板生成

使用方法:
    python scripts/pipeline/smart_data_audit.py              # 标准模式
    python scripts/pipeline/smart_data_audit.py --deadline 17:20  # 自定义死线
    python scripts/pipeline/smart_data_audit.py --max-retries 3   # 自定义重试次数
"""
import sys
import os
import argparse
import json
import shutil
from pathlib import Path
from datetime import datetime, time
from typing import Tuple, List, Dict, Optional

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
from core.trading_calendar import check_market_status, get_recent_trade_dates
from core.data_version_manager import get_version_manager


class SmartDataAuditor:
    """智能数据审计器 - 带熔断机制"""

    def __init__(self, target_date: str = None, freshness_threshold: float = 0.85,
                 max_retries: int = 5, deadline: str = "17:20"):
        self.project_root = project_root
        self.kline_dir = project_root / "data" / "kline"
        self.threshold = freshness_threshold
        self.max_retries = max_retries
        self.deadline = deadline
        self.version_manager = get_version_manager()

        # 确定目标日期
        if target_date:
            self.target_date = target_date
        else:
            trade_dates = get_recent_trade_dates(1)
            self.target_date = trade_dates[0] if trade_dates else datetime.now().strftime('%Y-%m-%d')

        # 审计结果
        self.results = {
            'audit_time': datetime.now().isoformat(),
            'target_date': self.target_date,
            'freshness_threshold': freshness_threshold,
            'max_retries': max_retries,
            'deadline': deadline,
            'retry_count': 0,
            'circuit_breaker_triggered': False,
            'fallback_mode': False,
            'checks': {},
            'passed': False,
            'issues': [],
            'data_items': []
        }

        # 加载重试计数
        self._load_retry_count()

    def _load_retry_count(self):
        """加载当前重试次数"""
        retry_file = self.project_root / "logs" / f"audit_retry_{self.target_date}.json"
        if retry_file.exists():
            try:
                with open(retry_file, 'r') as f:
                    data = json.load(f)
                    self.results['retry_count'] = data.get('count', 0)
            except:
                pass

    def _save_retry_count(self):
        """保存重试次数"""
        retry_file = self.project_root / "logs" / f"audit_retry_{self.target_date}.json"
        retry_file.parent.mkdir(exist_ok=True)
        with open(retry_file, 'w') as f:
            json.dump({'count': self.results['retry_count']}, f)

    def _check_deadline(self) -> bool:
        """检查是否超过截止死线"""
        now = datetime.now().time()
        deadline_hour, deadline_minute = map(int, self.deadline.split(':'))
        deadline_time = time(deadline_hour, deadline_minute)
        return now > deadline_time

    def _log(self, message: str, level: str = 'info'):
        """输出日志"""
        prefix = {'info': 'ℹ️', 'warning': '⚠️', 'error': '❌', 'success': '✅', 'circuit': '🚨'}.get(level, 'ℹ️')
        print(f"{prefix} {message}")

    def check_freshness(self) -> Tuple[bool, Dict]:
        """检查数据新鲜度"""
        self._log(f"检查数据新鲜度 (目标日期: {self.target_date}, 阈值: {self.threshold*100:.0f}%)")

        if not self.kline_dir.exists():
            return False, {'error': f'K线数据目录不存在: {self.kline_dir}'}

        parquet_files = list(self.kline_dir.glob("*.parquet"))
        total = len(parquet_files)

        if total == 0:
            return False, {'error': '没有找到K线数据文件'}

        fresh_count = 0
        stale_count = 0
        missing_count = 0
        errors = []

        for i, parquet_file in enumerate(sorted(parquet_files)):
            code = parquet_file.stem
            try:
                df = pl.read_parquet(parquet_file)
                if len(df) == 0:
                    missing_count += 1
                    continue

                latest_date = df['trade_date'].max()
                if latest_date == self.target_date:
                    fresh_count += 1
                else:
                    stale_count += 1
                    if len(errors) < 5:
                        errors.append(f"{code}: 最新日期 {latest_date}")

                if (i + 1) % 1000 == 0:
                    self._log(f"  已检查 {i + 1}/{total} 只...")

            except Exception as e:
                missing_count += 1
                if len(errors) < 5:
                    errors.append(f"{code}: 读取失败")

        valid_count = fresh_count + stale_count
        coverage_rate = fresh_count / valid_count if valid_count > 0 else 0

        # 熔断检查：如果超过最大重试次数，降低阈值
        if self.results['retry_count'] >= self.max_retries:
            self.results['circuit_breaker_triggered'] = True
            self.results['fallback_mode'] = True
            adjusted_threshold = self.threshold * 0.8  # 降低20%
            self._log(f"🚨 熔断触发！重试次数已达{self.max_retries}次，阈值调整为{adjusted_threshold*100:.0f}%", 'circuit')
            passed = coverage_rate >= adjusted_threshold
        else:
            passed = coverage_rate >= self.threshold

        result = {
            'total_stocks': total,
            'fresh_stocks': fresh_count,
            'stale_stocks': stale_count,
            'missing_stocks': missing_count,
            'coverage_rate': coverage_rate,
            'threshold': adjusted_threshold if self.results['fallback_mode'] else self.threshold,
            'passed': passed,
            'sample_errors': errors
        }

        status = '通过' if passed else '未通过'
        level = 'success' if passed else 'warning'
        if self.results['fallback_mode']:
            level = 'circuit'
        self._log(f"新鲜度检查{status}: {fresh_count}/{valid_count} ({coverage_rate*100:.1f}%)", level)

        return passed, result

    def check_completeness(self) -> Tuple[bool, Dict]:
        """检查数据完整性"""
        self._log("检查数据完整性")

        issues = []
        latest_date = None
        stock_count = 0

        try:
            kline_files = list(self.kline_dir.glob("*.parquet"))
            file_count = len(kline_files)

            # 熔断模式下降低要求
            min_files = 3500 if self.results['fallback_mode'] else 4000

            if file_count < min_files:
                issues.append(f"K线文件不足: {file_count}个 (期望>={min_files})")

            # 读取最新日期数据
            dfs = []
            for f in kline_files[:5000]:
                try:
                    df = pl.read_parquet(f)
                    if len(df) > 0:
                        required_cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
                        available_cols = [c for c in required_cols if c in df.columns]
                        if len(available_cols) >= 6:
                            df = df.select(available_cols)
                            df_unique = df.unique(subset=['trade_date'])
                            dfs.append(df_unique)
                except:
                    pass

            if dfs:
                data = pl.concat(dfs, how="diagonal")
                latest_date = data["trade_date"].max()
                day_data = data.filter(pl.col("trade_date") == latest_date)
                stock_count = len(day_data)

                min_stocks = 3500 if self.results['fallback_mode'] else 4000
                if stock_count < min_stocks:
                    issues.append(f"最新日期({latest_date})数据不足: {stock_count}只")
            else:
                issues.append("无法读取K线数据")

        except Exception as e:
            issues.append(f"完整性检查失败: {e}")

        passed = len(issues) == 0
        result = {
            'file_count': file_count if 'file_count' in dir() else 0,
            'latest_date': latest_date,
            'stock_count': stock_count,
            'passed': passed,
            'issues': issues
        }

        status = '通过' if passed else '未通过'
        self._log(f"完整性检查{status}: {stock_count}只股票", 'success' if passed else 'warning')
        for issue in issues:
            self._log(f"  - {issue}", 'warning')

        return passed, result

    def check_system_resources(self) -> Tuple[bool, Dict]:
        """检查系统资源"""
        self._log("检查系统资源")

        warnings = []
        critical = []

        try:
            usage = shutil.disk_usage(self.project_root / "data")
            percent = usage.used / usage.total * 100
            if percent > 95:
                critical.append(f"磁盘空间严重不足: {percent:.1f}%")
            elif percent > 85:
                warnings.append(f"磁盘空间不足: {percent:.1f}%")
            else:
                self._log(f"磁盘空间: {percent:.1f}% 已用")
        except Exception as e:
            warnings.append(f"磁盘检查失败: {e}")

        try:
            import psutil
            mem = psutil.virtual_memory()
            if mem.percent > 95:
                critical.append(f"内存严重不足: {mem.percent:.1f}%")
            elif mem.percent > 90:
                warnings.append(f"内存不足: {mem.percent:.1f}%")
            else:
                self._log(f"内存使用: {mem.percent:.1f}%")
        except:
            pass

        passed = len(critical) == 0
        return passed, {'passed': passed, 'warnings': warnings, 'critical': critical}

    def run_audit(self) -> Tuple[bool, bool]:
        """
        执行智能审计

        Returns:
            (是否通过, 是否熔断)
        """
        print("=" * 70)
        print("智能数据审计 - 带熔断机制")
        print("=" * 70)
        print(f"审计时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"目标日期: {self.target_date}")
        print(f"截止死线: {self.deadline}")
        print(f"最大重试: {self.max_retries}次")
        print(f"当前重试: {self.results['retry_count']}次")
        print()

        # 检查是否为交易日
        market_status = check_market_status()
        if not market_status['is_trading_day']:
            self._log("非交易日，跳过数据审计", 'info')
            return True, False

        # 检查是否超过死线
        if self._check_deadline():
            self._log(f"🚨 已超过截止死线 {self.deadline}，触发熔断！", 'circuit')
            self.results['circuit_breaker_triggered'] = True
            self.results['fallback_mode'] = True

        # 增加重试计数
        self.results['retry_count'] += 1
        self._save_retry_count()

        all_passed = True

        # 1. 系统资源检查
        passed, result = self.check_system_resources()
        self.results['checks']['system_resources'] = result
        all_passed = all_passed and passed

        # 2. 数据新鲜度检查
        passed, result = self.check_freshness()
        self.results['checks']['freshness'] = result
        all_passed = all_passed and passed

        # 3. 数据完整性检查
        passed, result = self.check_completeness()
        self.results['checks']['completeness'] = result
        all_passed = all_passed and passed

        # 保存结果
        self.results['passed'] = all_passed
        self._save_report()

        # 锁定版本（熔断模式下也锁定，但标记为降级）
        fresh_count = self.results['checks']['freshness'].get('fresh_stocks', 0)
        self.version_manager.lock_version(
            self.target_date,
            fresh_count,
            quality_passed=all_passed,
            fallback=self.results['fallback_mode']
        )

        # 输出汇总
        print()
        print("=" * 70)
        if all_passed:
            if self.results['fallback_mode']:
                print("⚠️  数据审计通过（熔断降级模式）")
            else:
                print("✅ 数据审计通过")
        else:
            print("❌ 数据审计未通过")
            if self.results['retry_count'] < self.max_retries and not self._check_deadline():
                print(f"   建议：进行第 {self.results['retry_count'] + 1} 次重试")
            else:
                print("   建议：已达到最大重试次数或超过死线，触发熔断")
        print("=" * 70)

        return all_passed, self.results['fallback_mode']

    def _save_report(self):
        """保存审计报告"""
        report_dir = self.project_root / "logs"
        report_dir.mkdir(exist_ok=True)

        # 生成审计看板
        dashboard = self._generate_dashboard()
        self.results['data_items'] = dashboard

        report_file = report_dir / f"data_audit_{self.target_date}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        self._log(f"审计报告已保存: {report_file}")

        # 保存审计看板
        dashboard_file = report_dir / f"audit_dashboard_{self.target_date}.json"
        with open(dashboard_file, 'w', encoding='utf-8') as f:
            json.dump(dashboard, f, ensure_ascii=False, indent=2)

    def _generate_dashboard(self) -> List[Dict]:
        """生成审计看板"""
        dashboard = []

        # 基础行情
        freshness = self.results['checks'].get('freshness', {})
        dashboard.append({
            'item': '个股行情',
            'status': 'success' if freshness.get('passed') else 'failed',
            'source': 'tushare',
            'records': freshness.get('fresh_stocks', 0),
            'coverage': freshness.get('coverage_rate', 0),
            'check_result': '正常' if freshness.get('passed') else '覆盖率不足'
        })

        # 完整性
        completeness = self.results['checks'].get('completeness', {})
        dashboard.append({
            'item': '数据完整性',
            'status': 'success' if completeness.get('passed') else 'warning',
            'source': 'local',
            'records': completeness.get('stock_count', 0),
            'coverage': completeness.get('stock_count', 0) / 5000 if completeness.get('stock_count') else 0,
            'check_result': '正常' if completeness.get('passed') else '数据量不足'
        })

        # 系统资源
        resources = self.results['checks'].get('system_resources', {})
        dashboard.append({
            'item': '系统资源',
            'status': 'success' if resources.get('passed') else 'failed',
            'source': 'system',
            'records': 0,
            'coverage': 0,
            'check_result': '正常' if resources.get('passed') else '资源不足'
        })

        return dashboard


def main():
    parser = argparse.ArgumentParser(description='智能数据审计 - 带熔断机制')
    parser.add_argument('--date', type=str, help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--threshold', type=float, default=0.85, help='新鲜度阈值 (0-1)，默认0.85')
    parser.add_argument('--max-retries', type=int, default=5, help='最大重试次数，默认5')
    parser.add_argument('--deadline', type=str, default="17:20", help='截止死线 (HH:MM)，默认17:20')
    parser.add_argument('--quiet', action='store_true', help='静默模式')

    args = parser.parse_args()

    auditor = SmartDataAuditor(
        target_date=args.date,
        freshness_threshold=args.threshold,
        max_retries=args.max_retries,
        deadline=args.deadline
    )

    passed, fallback = auditor.run_audit()

    # 返回码：0=通过, 1=未通过, 2=熔断模式通过
    if passed and fallback:
        sys.exit(2)  # 熔断模式通过
    elif passed:
        sys.exit(0)  # 正常通过
    else:
        sys.exit(1)  # 未通过


if __name__ == "__main__":
    main()
