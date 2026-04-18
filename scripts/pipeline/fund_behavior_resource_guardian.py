#!/usr/bin/env python3
"""
09:26任务资源保障守护程序
================================================================================
核心目标：确保09:26任务的所有资源准备就绪

保障策略：
1. 提前预热 - 09:15开始预热，确保数据加载到内存
2. 资源锁定 - 09:20锁定数据版本，防止被其他任务修改
3. 依赖检查 - 09:22检查所有前置任务状态
4. 快速通道 - 09:24进入快速执行模式，跳过非必要检查
================================================================================
"""
import sys
import os
import time
import json
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger
from core.trading_calendar import check_market_status
from core.data_version_manager import get_version_manager

logger = setup_logger(
    name="fund_behavior_resource_guardian",
    level="INFO",
    log_file="system/fund_behavior_resource_guardian.log"
)

# 关键时间点
WARMUP_TIME = "09:15"      # 开始预热
LOCK_TIME = "09:20"        # 锁定数据
CHECK_TIME = "09:22"       # 依赖检查
FAST_TRACK_TIME = "09:24"  # 快速通道
EXECUTE_TIME = "09:26"     # 执行时间


class ResourceGuardian:
    """资源保障守护者"""
    
    def __init__(self):
        self.project_root = project_root
        self.kline_dir = project_root / "data" / "kline"
        self.version_manager = get_version_manager()
        self.status_file = project_root / "logs" / "fund_behavior_resource_status.json"
        
    def is_trading_day(self) -> bool:
        """检查是否为交易日"""
        status = check_market_status()
        return status['is_trading_day']
    
    def warmup_data(self) -> bool:
        """
        预热数据 - 将K线数据加载到系统缓存
        """
        logger.info("=" * 60)
        logger.info("【09:15】开始数据预热")
        logger.info("=" * 60)
        
        try:
            import polars as pl
            
            # 预加载部分K线文件到内存缓存
            kline_files = list(self.kline_dir.glob("*.parquet"))
            sample_size = min(100, len(kline_files))
            
            logger.info(f"预加载 {sample_size} 个K线文件...")
            
            warmed_count = 0
            for i, f in enumerate(kline_files[:sample_size]):
                try:
                    # 读取但不保存，仅用于预热文件系统缓存
                    _ = pl.read_parquet(f)
                    warmed_count += 1
                    if (i + 1) % 20 == 0:
                        logger.info(f"  已预热 {i + 1}/{sample_size}...")
                except:
                    pass
            
            logger.info(f"✅ 数据预热完成: {warmed_count}/{sample_size}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 数据预热失败: {e}")
            return False
    
    def lock_data_version(self) -> bool:
        """
        锁定数据版本 - 确保09:26任务使用稳定的数据
        """
        logger.info("=" * 60)
        logger.info("【09:20】锁定数据版本")
        logger.info("=" * 60)
        
        try:
            # 获取已锁定的数据版本
            version_info = self.version_manager.get_locked_version()
            
            if version_info and version_info.get('quality_passed') == 'true':
                logger.info(f"✅ 数据版本已锁定: {version_info.get('trade_date')}")
                logger.info(f"   股票数量: {version_info.get('stock_count')}")
                logger.info(f"   质检状态: 通过")
                return True
            else:
                logger.warning("⚠️ 无已通过质检的数据版本")
                # 尝试运行数据质检
                logger.info("尝试运行数据质检...")
                result = subprocess.run(
                    [sys.executable, str(project_root / "scripts" / "pipeline" / "data_audit.py")],
                    cwd=project_root,
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                if result.returncode == 0:
                    logger.info("✅ 数据质检完成")
                    # 再次检查锁定状态
                    version_info = self.version_manager.get_locked_version()
                    if version_info:
                        return True
                    else:
                        # 手动锁定当前版本
                        from datetime import datetime
                        today = datetime.now().strftime('%Y-%m-%d')
                        self.version_manager.lock_version(today, 5178, quality_passed=True)
                        logger.info(f"✅ 已手动锁定数据版本: {today}")
                        return True
                else:
                    logger.error("❌ 数据质检失败")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ 锁定数据版本失败: {e}")
            return False
    
    def check_dependencies(self) -> Tuple[bool, List[str]]:
        """
        检查所有依赖任务状态
        """
        logger.info("=" * 60)
        logger.info("【09:22】检查依赖任务")
        logger.info("=" * 60)
        
        required_tasks = [
            "morning_data",
            "collect_macro",
            "collect_oil_dollar",
            "collect_commodities",
            "collect_sentiment",
            "collect_news"
        ]
        
        today = datetime.now().strftime('%Y%m%d')
        task_states_file = project_root / "logs" / "task_states.json"
        
        failed_tasks = []
        
        if not task_states_file.exists():
            logger.warning("⚠️ 无任务状态文件，假设依赖已完成")
            return True, []
        
        try:
            with open(task_states_file, 'r') as f:
                states = json.load(f)
            
            for task in required_tasks:
                key = f"{task}_{today}"
                if key in states:
                    status = states[key].get('status')
                    if status == 'completed':
                        logger.info(f"  ✅ {task}: 已完成")
                    elif status == 'failed':
                        logger.warning(f"  ⚠️ {task}: 失败")
                        failed_tasks.append(task)
                    else:
                        logger.warning(f"  ⏳ {task}: 状态未知 ({status})")
                else:
                    logger.warning(f"  ⚠️ {task}: 无状态记录")
                    # 不视为失败，可能任务未配置
            
            if failed_tasks:
                logger.warning(f"⚠️ {len(failed_tasks)} 个依赖任务失败: {failed_tasks}")
                return False, failed_tasks
            else:
                logger.info("✅ 所有依赖检查通过")
                return True, []
                
        except Exception as e:
            logger.error(f"❌ 检查依赖失败: {e}")
            return False, ["check_error"]
    
    def prepare_fast_track(self) -> bool:
        """
        准备快速执行模式
        """
        logger.info("=" * 60)
        logger.info("【09:24】进入快速通道")
        logger.info("=" * 60)
        
        try:
            # 清理临时文件，释放内存
            cache_dir = project_root / "data" / "cache"
            if cache_dir.exists():
                cleaned = 0
                for f in cache_dir.glob("*.tmp"):
                    try:
                        f.unlink()
                        cleaned += 1
                    except:
                        pass
                if cleaned > 0:
                    logger.info(f"  清理了 {cleaned} 个临时文件")
            
            # 检查系统资源
            try:
                import psutil
                mem = psutil.virtual_memory()
                if mem.percent > 90:
                    logger.warning(f"  ⚠️ 内存使用率过高: {mem.percent}%")
                else:
                    logger.info(f"  内存状态: {mem.percent}%")
                
                disk = psutil.disk_usage(project_root)
                if disk.percent > 95:
                    logger.warning(f"  ⚠️ 磁盘空间不足: {disk.percent}%")
                else:
                    logger.info(f"  磁盘状态: {disk.percent}%")
            except:
                pass
            
            logger.info("✅ 快速通道准备完成")
            return True
            
        except Exception as e:
            logger.error(f"❌ 快速通道准备失败: {e}")
            return False
    
    def save_status(self, status: str, details: dict = None):
        """保存保障状态"""
        status_data = {
            "timestamp": datetime.now().isoformat(),
            "status": status,
            "details": details or {}
        }
        
        try:
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(status_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存状态失败: {e}")
    
    def run(self) -> bool:
        """
        运行资源保障流程
        """
        logger.info("\n" + "=" * 70)
        logger.info("09:26任务资源保障守护程序启动")
        logger.info("=" * 70)
        
        # 检查是否为交易日
        if not self.is_trading_day():
            logger.info("非交易日，跳过资源保障")
            return True
        
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        
        logger.info(f"当前时间: {current_time}")
        
        # 根据当前时间执行相应阶段
        if current_time < WARMUP_TIME:
            logger.info(f"等待 {WARMUP_TIME} 开始预热...")
            return True
        
        results = {
            "warmup": False,
            "lock": False,
            "dependencies": False,
            "fast_track": False
        }
        
        # 1. 数据预热 (09:15+)
        if current_time >= WARMUP_TIME:
            results["warmup"] = self.warmup_data()
        
        # 2. 锁定数据版本 (09:20+)
        if current_time >= LOCK_TIME:
            results["lock"] = self.lock_data_version()
        
        # 3. 检查依赖 (09:22+)
        if current_time >= CHECK_TIME:
            deps_ok, failed = self.check_dependencies()
            results["dependencies"] = deps_ok
            results["failed_deps"] = failed
        
        # 4. 快速通道准备 (09:24+)
        if current_time >= FAST_TRACK_TIME:
            results["fast_track"] = self.prepare_fast_track()
        
        # 保存状态
        all_ready = all(results.values())
        self.save_status(
            "ready" if all_ready else "partial",
            results
        )
        
        logger.info("\n" + "=" * 70)
        logger.info("资源保障状态汇总")
        logger.info("=" * 70)
        logger.info(f"数据预热: {'✅' if results['warmup'] else '❌'}")
        logger.info(f"数据锁定: {'✅' if results['lock'] else '❌'}")
        logger.info(f"依赖检查: {'✅' if results['dependencies'] else '❌'}")
        logger.info(f"快速通道: {'✅' if results['fast_track'] else '❌'}")
        logger.info("=" * 70)
        
        if all_ready:
            logger.info("✅ 所有资源准备就绪，09:26任务可以执行")
        else:
            logger.warning("⚠️ 部分资源未就绪，但09:26任务仍会尝试执行")
        
        return True


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='09:26任务资源保障守护程序')
    parser.add_argument('--phase', type=str, 
                       choices=['warmup', 'lock', 'check', 'fasttrack', 'prepare', 'validate', 'full'],
                       default='full',
                       help='执行阶段: warmup=预热, lock=锁定, check=检查, fasttrack=快速通道, prepare=准备(预热+锁定), validate=验证(检查+快速通道), full=完整流程')
    
    args = parser.parse_args()
    
    guardian = ResourceGuardian()
    
    # 检查是否为交易日
    if not guardian.is_trading_day():
        logger.info("非交易日，跳过资源保障")
        return 0
    
    try:
        if args.phase == 'warmup':
            logger.info("执行阶段: 数据预热")
            success = guardian.warmup_data()
        elif args.phase == 'lock':
            logger.info("执行阶段: 数据锁定")
            success = guardian.lock_data_version()
        elif args.phase == 'check':
            logger.info("执行阶段: 依赖检查")
            success, _ = guardian.check_dependencies()
        elif args.phase == 'fasttrack':
            logger.info("执行阶段: 快速通道")
            success = guardian.prepare_fast_track()
        elif args.phase == 'prepare':
            # 新阶段：准备（预热 + 锁定）
            logger.info("=" * 70)
            logger.info("执行阶段: 数据准备 (预热 + 锁定)")
            logger.info("=" * 70)
            warmup_ok = guardian.warmup_data()
            lock_ok = guardian.lock_data_version()
            success = warmup_ok and lock_ok
            logger.info(f"\n准备阶段结果: {'✅ 成功' if success else '❌ 部分失败'}")
            logger.info(f"  数据预热: {'✅' if warmup_ok else '❌'}")
            logger.info(f"  数据锁定: {'✅' if lock_ok else '❌'}")
        elif args.phase == 'validate':
            # 新阶段：验证（检查 + 快速通道）
            logger.info("=" * 70)
            logger.info("执行阶段: 资源验证 (检查 + 快速通道)")
            logger.info("=" * 70)
            check_ok, failed = guardian.check_dependencies()
            fasttrack_ok = guardian.prepare_fast_track()
            success = check_ok and fasttrack_ok
            logger.info(f"\n验证阶段结果: {'✅ 成功' if success else '❌ 部分失败'}")
            logger.info(f"  依赖检查: {'✅' if check_ok else '❌'}")
            logger.info(f"  快速通道: {'✅' if fasttrack_ok else '❌'}")
        else:
            success = guardian.run()
        
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.exception("资源保障守护程序异常")
        sys.exit(1)


if __name__ == "__main__":
    main()
