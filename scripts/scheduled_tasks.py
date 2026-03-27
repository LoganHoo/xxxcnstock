#!/usr/bin/env python3
"""
综合定时任务脚本
执行所有数据处理任务：数据采集、预计算、关键位计算、CVD计算、股票推荐
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import subprocess
import logging

PROJECT_ROOT = Path(__file__).parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'scheduled_tasks.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_script(script_name: str, description: str, timeout: int = 600) -> bool:
    """运行指定脚本"""
    script_path = PROJECT_ROOT / "scripts" / script_name
    
    if not script_path.exists():
        logger.error(f"脚本不存在: {script_path}")
        return False
    
    logger.info(f"\n{'='*60}")
    logger.info(f"执行: {description}")
    logger.info(f"脚本: {script_path}")
    logger.info(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.stdout:
            logger.info(f"输出:\n{result.stdout[-2000:]}")
        
        if result.returncode == 0:
            logger.info(f"✅ {description} 完成")
            return True
        else:
            logger.error(f"❌ {description} 失败，返回码: {result.returncode}")
            if result.stderr:
                logger.error(f"错误输出:\n{result.stderr[-1000:]}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {description} 超时（超过 {timeout} 秒）")
        return False
    except Exception as e:
        logger.error(f"❌ 执行 {description} 时发生错误: {e}")
        return False


def main():
    """主函数 - 按顺序执行所有定时任务"""
    start_time = datetime.now()
    
    logger.info("\n" + "="*70)
    logger.info("🚀 开始执行定时任务")
    logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    results = {}
    
    results['数据采集'] = run_script(
        "fetch_history_klines_parquet.py",
        "数据采集",
        timeout=3600
    )
    
    results['预计算评分'] = run_script(
        "precompute_enhanced_scores.py",
        "预计算技术指标评分",
        timeout=300
    )
    
    results['关键位CVD计算'] = run_script(
        "calculate_key_levels_cvd.py",
        "关键位与CVD指标计算",
        timeout=300
    )
    
    results['股票推荐'] = run_script(
        "tomorrow_picks.py",
        "股票推荐生成",
        timeout=300
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "="*70)
    logger.info("📊 执行结果汇总")
    logger.info("="*70)
    
    for task, success in results.items():
        status = "✅ 成功" if success else "❌ 失败"
        logger.info(f"  {task}: {status}")
    
    logger.info("")
    logger.info(f"总耗时: {duration:.1f} 秒")
    logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    all_success = all(results.values())
    return 0 if all_success else 1


if __name__ == '__main__':
    sys.exit(main())
