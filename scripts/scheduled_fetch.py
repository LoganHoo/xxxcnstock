"""
定时任务配置脚本
用于设置每日自动更新历史数据
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
        logging.FileHandler(LOG_DIR / 'scheduled_fetch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_daily_fetch():
    """执行每日数据采集"""
    logger.info("="*70)
    logger.info("开始执行定时数据采集任务")
    logger.info("="*70)
    
    try:
        script_path = PROJECT_ROOT / "scripts" / "fetch_history_klines_parquet.py"
        
        if not script_path.exists():
            logger.error(f"脚本不存在: {script_path}")
            return False
        
        logger.info(f"执行脚本: {script_path}")
        logger.info(f"工作目录: {PROJECT_ROOT}")
        
        result = subprocess.run(
            [sys.executable, str(script_path), "--mode", "full", "--days", "1095", "--rate-limit", "5.0"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        if result.returncode == 0:
            logger.info("数据采集成功完成")
            logger.info(f"输出:\n{result.stdout}")
            return True
        else:
            logger.error(f"数据采集失败，返回码: {result.returncode}")
            logger.error(f"错误输出:\n{result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("数据采集超时（超过1小时）")
        return False
    except Exception as e:
        logger.error(f"执行数据采集时发生错误: {e}")
        return False


def main():
    """主函数"""
    logger.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = run_daily_fetch()
    
    if success:
        logger.info("✅ 定时任务执行成功")
    else:
        logger.error("❌ 定时任务执行失败")
    
    logger.info("="*70)
    logger.info("")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
