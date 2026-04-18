#!/usr/bin/env python3
"""
数据采集主控脚本 - 统一入口
================================================================================
整合所有数据采集功能，提供统一的调用接口

使用方法:
    python scripts/data_collection_master.py --task all
    python scripts/data_collection_master.py --task kline
    python scripts/data_collection_master.py --task fundamental
    python scripts/data_collection_master.py --task check
    
    # 强制采集指定日期
    python scripts/data_collection_master.py --task kline --date 2026-04-17
================================================================================
"""
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.logger import setup_logger

logger = setup_logger(
    name="data_collection_master",
    level="INFO",
    log_file="system/data_collection.log"
)


class DataCollectionManager:
    """数据采集管理器"""
    
    def __init__(self):
        self.scripts_dir = PROJECT_ROOT / "scripts"
        self.pipeline_dir = self.scripts_dir / "pipeline"
        self.results = {}
    
    def run_script(self, script_name: str, args: list = None) -> bool:
        """运行指定脚本"""
        # 优先查找pipeline目录
        script_path = self.pipeline_dir / script_name
        if not script_path.exists():
            script_path = self.scripts_dir / script_name
            
        if not script_path.exists():
            logger.error(f"脚本不存在: {script_path}")
            return False
        
        cmd = [sys.executable, str(script_path)]
        if args:
            cmd.extend(args)
        
        logger.info(f"执行: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200  # 2小时超时
            )
            
            if result.returncode == 0:
                logger.info(f"✅ {script_name} 执行成功")
                return True
            else:
                logger.error(f"❌ {script_name} 执行失败")
                if result.stderr:
                    logger.error(result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏱️ {script_name} 执行超时")
            return False
        except Exception as e:
            logger.error(f"💥 {script_name} 执行异常: {e}")
            return False
    
    def collect_stock_list(self) -> bool:
        """采集股票列表"""
        logger.info("=" * 70)
        logger.info("📋 步骤1: 采集股票列表")
        logger.info("=" * 70)
        return self.run_script("fetch_stock_list.py")
    
    def collect_kline(self, date: str = None) -> bool:
        """
        采集K线数据
        
        Args:
            date: 指定日期 (YYYY-MM-DD)，None表示采集当日
        """
        logger.info("=" * 70)
        logger.info("📈 步骤2: 采集K线数据")
        logger.info("=" * 70)
        
        args = []
        if date:
            args = ['--date', date]
            logger.info(f"强制采集日期: {date}")
        
        # 使用新的 data_collect.py (pipeline版本)
        return self.run_script("data_collect.py", args)
    
    def collect_fundamental(self) -> bool:
        """采集基本面数据"""
        logger.info("=" * 70)
        logger.info("📊 步骤3: 采集基本面数据")
        logger.info("=" * 70)
        return self.run_script("fetch_fundamental_baostock.py")
    
    def collect_missing_fundamental(self) -> bool:
        """补充采集缺失的基本面数据"""
        logger.info("=" * 70)
        logger.info("📊 步骤4: 补充采集缺失基本面数据")
        logger.info("=" * 70)
        return self.run_script("fetch_missing_fundamental.py")
    
    def validate_data(self) -> bool:
        """验证数据完整性"""
        logger.info("=" * 70)
        logger.info("🔍 步骤5: 验证数据完整性")
        logger.info("=" * 70)
        return self.run_script("data_integrity_check.py")
    
    def run_all(self):
        """执行完整采集流程"""
        logger.info("\n" + "=" * 70)
        logger.info("🚀 启动完整数据采集流程")
        logger.info(f"⏰ 开始时间: {datetime.now().isoformat()}")
        logger.info("=" * 70)
        
        steps = [
            ("股票列表", self.collect_stock_list),
            ("K线数据", self.collect_kline),
            ("基本面数据", self.collect_fundamental),
            ("补充基本面", self.collect_missing_fundamental),
            ("数据验证", self.validate_data),
        ]
        
        results = {}
        for name, step_func in steps:
            try:
                success = step_func()
                results[name] = "✅ 成功" if success else "❌ 失败"
            except Exception as e:
                logger.exception(f"{name} 步骤异常")
                results[name] = f"💥 异常: {e}"
        
        # 输出总结
        logger.info("\n" + "=" * 70)
        logger.info("📊 数据采集总结")
        logger.info("=" * 70)
        for name, result in results.items():
            logger.info(f"  {name}: {result}")
        
        success_count = sum(1 for r in results.values() if "✅" in r)
        logger.info(f"\n完成度: {success_count}/{len(steps)}")
        logger.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='数据采集主控脚本')
    parser.add_argument(
        '--task',
        type=str,
        choices=['all', 'list', 'kline', 'fundamental', 'missing', 'check'],
        default='all',
        help='采集任务类型'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='指定日期 (YYYY-MM-DD)，仅对kline任务有效'
    )
    
    args = parser.parse_args()
    
    manager = DataCollectionManager()
    
    if args.task == 'all':
        manager.run_all()
    elif args.task == 'list':
        manager.collect_stock_list()
    elif args.task == 'kline':
        manager.collect_kline(date=args.date)
    elif args.task == 'fundamental':
        manager.collect_fundamental()
    elif args.task == 'missing':
        manager.collect_missing_fundamental()
    elif args.task == 'check':
        manager.validate_data()


if __name__ == "__main__":
    main()
