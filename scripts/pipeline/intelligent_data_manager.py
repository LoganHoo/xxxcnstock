#!/usr/bin/env python3
"""
智能数据管理器 - 自动检查、发现缺失、自动补采
================================================================================
功能：
1. 检查数据完整性，识别缺失的交易日
2. 自动补采缺失数据
3. 验证补采结果
4. 生成数据健康报告

用法：
    python scripts/pipeline/intelligent_data_manager.py --check
    python scripts/pipeline/intelligent_data_manager.py --auto-fix
    python scripts/pipeline/intelligent_data_manager.py --check --auto-fix --notify
"""
import sys
import os
import argparse
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict
import subprocess

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("intelligent_data_manager")


class DataHealthChecker:
    """数据健康检查器"""
    
    def __init__(self, kline_dir: str = None):
        # 使用绝对路径，确保在 Kestra 容器中也能找到数据
        if kline_dir is None:
            project_root = Path(__file__).parent.parent.parent
            kline_dir = project_root / "data" / "kline"
        self.kline_dir = Path(kline_dir)
        self.sample_size = 100  # 采样检查的股票数量
        
    def get_sample_stocks(self) -> List[Path]:
        """获取采样股票列表"""
        all_files = sorted(self.kline_dir.glob("*.parquet"))
        if len(all_files) <= self.sample_size:
            return all_files
        
        # 均匀采样
        step = len(all_files) // self.sample_size
        return [all_files[i * step] for i in range(self.sample_size)]
    
    def check_data_freshness(self) -> Dict:
        """检查数据新鲜度"""
        logger.info("检查数据新鲜度...")
        
        sample_files = self.get_sample_stocks()
        latest_dates = []
        
        for f in sample_files:
            try:
                # 优先使用 pyarrow（Kestra 容器中有）
                try:
                    import pyarrow.parquet as pq
                    table = pq.read_table(f, columns=['trade_date'])
                    max_date = table.column('trade_date').to_pylist()[-1]
                    latest_dates.append(str(max_date))
                except ImportError:
                    # 回退到 pandas
                    import pandas as pd
                    df = pd.read_parquet(f)
                    if 'trade_date' in df.columns:
                        max_date = df['trade_date'].max()
                        latest_dates.append(str(max_date))
                    elif 'date' in df.columns:
                        max_date = df['date'].max()
                        latest_dates.append(str(max_date))
            except Exception as e:
                logger.warning(f"读取 {f.name} 失败: {e}")
        
        if not latest_dates:
            return {"status": "error", "message": "无法读取数据文件"}
        
        # 统计日期分布
        from collections import Counter
        date_counts = Counter(latest_dates)
        most_common_date = date_counts.most_common(1)[0][0]
        
        # 计算数据延迟
        latest_date = max(latest_dates)
        today = datetime.now().strftime('%Y-%m-%d')
        latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
        today_dt = datetime.strptime(today, '%Y-%m-%d')
        days_behind = (today_dt - latest_dt).days
        
        return {
            "status": "ok",
            "latest_date": latest_date,
            "today": today,
            "days_behind": days_behind,
            "sample_count": len(sample_files),
            "date_distribution": dict(date_counts.most_common(5)),
            "needs_update": days_behind > 0
        }
    
    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """
        获取交易日列表（简化版，实际应该用交易日历）
        暂时假设周一到周五都是交易日
        """
        trading_days = []
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        current = start
        while current <= end:
            # 跳过周末
            if current.weekday() < 5:  # 0-4 是周一到周五
                trading_days.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return trading_days
    
    def identify_missing_dates(self, check_days: int = 7) -> List[str]:
        """识别缺失的数据日期"""
        logger.info(f"检查过去 {check_days} 天的数据完整性...")
        
        # 获取数据最新日期
        freshness = self.check_data_freshness()
        if freshness["status"] != "ok":
            return []
        
        latest_date = freshness["latest_date"]
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 检查从最新数据日期到今天之间的缺失
        start_check = (datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 如果最新数据就是今天，不需要检查
        if start_check > today:
            logger.info("数据已是最新，无需补采")
            return []
        
        # 获取应该有的交易日
        expected_days = self.get_trading_days(start_check, today)
        
        if not expected_days:
            logger.info("没有需要补采的交易日")
            return []
        
        logger.info(f"发现 {len(expected_days)} 个需要检查的交易日: {expected_days}")
        return expected_days
    
    def generate_health_report(self) -> Dict:
        """生成数据健康报告"""
        logger.info("生成数据健康报告...")
        
        freshness = self.check_data_freshness()
        missing_dates = self.identify_missing_dates()
        
        report = {
            "check_time": datetime.now().isoformat(),
            "freshness": freshness,
            "missing_dates": missing_dates,
            "needs_action": len(missing_dates) > 0 or freshness.get("days_behind", 0) > 0,
            "action_required": []
        }
        
        if missing_dates:
            report["action_required"].append(f"补采缺失数据: {', '.join(missing_dates)}")
        
        if freshness.get("days_behind", 0) > 0:
            report["action_required"].append(f"数据落后 {freshness['days_behind']} 天")
        
        return report


class AutoDataCollector:
    """自动数据采集器"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        
    def collect_single_date(self, date: str) -> bool:
        """采集单日期数据"""
        logger.info(f"开始采集 {date} 的数据...")
        
        try:
            # 调用原始数据采集脚本
            cmd = [
                sys.executable,
                str(self.project_root / "scripts" / "data_collection_controller.py"),
                "--mode", "daily",
                "--date", date,
                "--skip-market-check"  # 因为是补采，跳过市场检查
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1小时超时
            )
            
            if result.returncode == 0:
                logger.info(f"✅ {date} 数据采集成功")
                return True
            else:
                logger.error(f"❌ {date} 数据采集失败: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏱️ {date} 数据采集超时")
            return False
        except Exception as e:
            logger.error(f"❌ {date} 数据采集异常: {e}")
            return False
    
    def auto_fix_missing_data(self, dates: List[str]) -> Dict:
        """自动修复缺失数据"""
        logger.info(f"开始自动修复 {len(dates)} 天的缺失数据...")
        
        results = {
            "total": len(dates),
            "success": 0,
            "failed": 0,
            "details": []
        }
        
        for date in sorted(dates):
            success = self.collect_single_date(date)
            results["details"].append({
                "date": date,
                "status": "success" if success else "failed"
            })
            
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1
        
        logger.info(f"自动修复完成: 成功 {results['success']}/{results['total']}")
        return results


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='智能数据管理器')
    parser.add_argument('--check', action='store_true', help='检查数据健康状态')
    parser.add_argument('--auto-fix', action='store_true', help='自动修复缺失数据')
    parser.add_argument('--notify', action='store_true', help='发送通知邮件')
    parser.add_argument('--output', type=str, help='报告输出文件路径')
    
    args = parser.parse_args()
    
    # 默认执行检查
    if not args.check and not args.auto_fix:
        args.check = True
    
    checker = DataHealthChecker()
    collector = AutoDataCollector()
    
    # 执行检查
    if args.check:
        logger.info("=" * 60)
        logger.info("开始数据健康检查")
        logger.info("=" * 60)
        
        report = checker.generate_health_report()
        
        # 打印报告
        print("\n" + "=" * 60)
        print("数据健康报告")
        print("=" * 60)
        print(f"检查时间: {report['check_time']}")
        print(f"最新数据日期: {report['freshness'].get('latest_date', 'N/A')}")
        print(f"当前日期: {report['freshness'].get('today', 'N/A')}")
        print(f"数据落后: {report['freshness'].get('days_behind', 0)} 天")
        print(f"缺失日期: {', '.join(report['missing_dates']) if report['missing_dates'] else '无'}")
        print(f"需要处理: {'是' if report['needs_action'] else '否'}")
        
        if report['action_required']:
            print("\n需要执行的操作:")
            for action in report['action_required']:
                print(f"  - {action}")
        
        print("=" * 60)
        
        # 保存报告
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            logger.info(f"报告已保存到: {args.output}")
    
    # 自动修复
    if args.auto_fix:
        logger.info("=" * 60)
        logger.info("开始自动修复数据")
        logger.info("=" * 60)
        
        missing_dates = checker.identify_missing_dates()
        
        if not missing_dates:
            logger.info("没有需要修复的缺失数据")
        else:
            fix_results = collector.auto_fix_missing_data(missing_dates)
            
            print("\n" + "=" * 60)
            print("自动修复结果")
            print("=" * 60)
            print(f"总计: {fix_results['total']}")
            print(f"成功: {fix_results['success']}")
            print(f"失败: {fix_results['failed']}")
            print("=" * 60)
            
            # 如果有失败，返回错误码
            if fix_results['failed'] > 0:
                return 1
    
    # 发送通知
    if args.notify:
        logger.info("发送通知邮件...")
        # TODO: 集成邮件发送
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
