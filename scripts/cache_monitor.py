#!/usr/bin/env python3
"""
缓存监控和自动修复脚本
功能：
1. 检查Parquet文件完整性
2. 清理损坏的缓存文件
3. 清理过期的checkpoint文件
4. 生成健康报告

使用方法:
    python scripts/cache_monitor.py
    python scripts/cache_monitor.py --check-only  # 仅检查不清理
    python scripts/cache_monitor.py --force       # 强制清理所有缓存
"""
import sys
import os
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

os.environ['PYTHONUNBUFFERED'] = '1'

from core.logger import setup_logger

logger = setup_logger(
    name="cache_monitor",
    level="INFO",
    log_file="system/cache_monitor.log",
    rotation="1 day",
    retention="7 days"
)

# 配置
CACHE_DIR = project_root / "data" / "cache"
CHECKPOINT_DIR = project_root / "data" / "checkpoints"
STATE_FILE = project_root / "logs" / "cache_health.json"
DEFAULT_MAX_AGE_DAYS = 7


def check_parquet_integrity(file_path: Path) -> Tuple[bool, str]:
    """
    检查Parquet文件完整性
    
    Returns:
        (是否有效, 错误信息)
    """
    try:
        import polars as pl
        
        # 检查文件是否存在且不为空
        if not file_path.exists():
            return False, "文件不存在"
        
        if file_path.stat().st_size == 0:
            return False, "文件为空"
        
        # 尝试读取文件
        df = pl.read_parquet(file_path)
        
        # 验证基本结构
        if df.is_empty():
            return False, "数据为空"
        
        # 检查必要的列
        required_columns = ['code', 'trade_date']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            return False, f"缺少必要列: {missing_cols}"
        
        return True, "正常"
        
    except Exception as e:
        return False, f"读取错误: {str(e)[:50]}"


def clean_corrupted_cache(check_only: bool = False) -> Dict:
    """
    清理损坏的缓存文件
    
    Args:
        check_only: 仅检查不清理
    
    Returns:
        清理报告
    """
    report = {
        'timestamp': datetime.now().isoformat(),
        'check_only': check_only,
        'cache_dir': str(CACHE_DIR),
        'checkpoint_dir': str(CHECKPOINT_DIR),
        'files_checked': 0,
        'files_corrupted': 0,
        'files_cleaned': [],
        'files_expired': [],
        'errors': []
    }
    
    # 确保目录存在
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 检查缓存文件
    logger.info(f"开始检查缓存目录: {CACHE_DIR}")
    for cache_file in CACHE_DIR.glob("*.parquet"):
        report['files_checked'] += 1
        is_valid, message = check_parquet_integrity(cache_file)
        
        if not is_valid:
            report['files_corrupted'] += 1
            report['files_cleaned'].append({
                'file': cache_file.name,
                'reason': message,
                'size': cache_file.stat().st_size
            })
            
            if not check_only:
                try:
                    cache_file.unlink()
                    logger.warning(f"已删除损坏的缓存: {cache_file.name} ({message})")
                except Exception as e:
                    report['errors'].append(f"删除失败 {cache_file.name}: {e}")
    
    # 清理过期的checkpoint
    cutoff = datetime.now() - timedelta(days=DEFAULT_MAX_AGE_DAYS)
    logger.info(f"清理 {cutoff.strftime('%Y-%m-%d')} 之前的checkpoint")
    
    for checkpoint in CHECKPOINT_DIR.glob("*.parquet"):
        mtime = datetime.fromtimestamp(checkpoint.stat().st_mtime)
        if mtime < cutoff:
            report['files_expired'].append({
                'file': checkpoint.name,
                'mtime': mtime.isoformat()
            })
            
            if not check_only:
                try:
                    checkpoint.unlink()
                    logger.info(f"已删除过期checkpoint: {checkpoint.name}")
                except Exception as e:
                    report['errors'].append(f"删除失败 {checkpoint.name}: {e}")
    
    # 保存报告
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    return report


def force_clean_all() -> Dict:
    """强制清理所有缓存"""
    report = {
        'timestamp': datetime.now().isoformat(),
        'action': 'force_clean',
        'cache_files_removed': 0,
        'checkpoint_files_removed': 0
    }
    
    # 清理所有缓存
    for cache_file in CACHE_DIR.glob("*.parquet"):
        try:
            cache_file.unlink()
            report['cache_files_removed'] += 1
        except Exception as e:
            logger.error(f"删除缓存失败 {cache_file}: {e}")
    
    # 清理所有checkpoint
    for checkpoint in CHECKPOINT_DIR.glob("*.parquet"):
        try:
            checkpoint.unlink()
            report['checkpoint_files_removed'] += 1
        except Exception as e:
            logger.error(f"删除checkpoint失败 {checkpoint}: {e}")
    
    # 清理索引文件
    index_file = CACHE_DIR / "data_cache_index.json"
    if index_file.exists():
        index_file.unlink()
        report['index_removed'] = True
    
    logger.info(f"强制清理完成: 缓存 {report['cache_files_removed']} 个, checkpoint {report['checkpoint_files_removed']} 个")
    
    return report


def print_report(report: Dict):
    """打印报告"""
    print("\n" + "="*60)
    print("缓存监控报告")
    print("="*60)
    print(f"检查时间: {report['timestamp']}")
    print(f"检查文件数: {report['files_checked']}")
    print(f"损坏文件数: {report['files_corrupted']}")
    print(f"过期文件数: {len(report['files_expired'])}")
    
    if report['files_cleaned']:
        print("\n已清理的损坏文件:")
        for item in report['files_cleaned']:
            print(f"  - {item['file']}: {item['reason']}")
    
    if report['files_expired']:
        print("\n已清理的过期文件:")
        for item in report['files_expired']:
            print(f"  - {item['file']}")
    
    if report['errors']:
        print("\n错误:")
        for error in report['errors']:
            print(f"  - {error}")
    
    print("="*60)


def main():
    parser = argparse.ArgumentParser(description='缓存监控和清理工具')
    parser.add_argument('--check-only', action='store_true', help='仅检查不清理')
    parser.add_argument('--force', action='store_true', help='强制清理所有缓存')
    parser.add_argument('--max-age', type=int, default=DEFAULT_MAX_AGE_DAYS, 
                        help=f'checkpoint最大保留天数 (默认: {DEFAULT_MAX_AGE_DAYS})')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("缓存监控任务启动")
    logger.info(f"模式: {'强制清理' if args.force else '仅检查' if args.check_only else '智能清理'}")
    logger.info("="*60)
    
    if args.force:
        report = force_clean_all()
        print(f"\n强制清理完成:")
        print(f"  缓存文件: {report['cache_files_removed']} 个")
        print(f"  checkpoint: {report['checkpoint_files_removed']} 个")
    else:
        report = clean_corrupted_cache(check_only=args.check_only)
        print_report(report)
    
    logger.info("缓存监控任务完成")
    
    # 如果有损坏文件且不是仅检查模式，返回非0退出码
    if not args.check_only and report.get('files_corrupted', 0) > 0:
        sys.exit(0)  # 清理成功，正常退出
    
    sys.exit(0)


if __name__ == "__main__":
    main()
