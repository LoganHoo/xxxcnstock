#!/usr/bin/env python3
"""
无效K线文件归档脚本

功能：
1. 识别无效K线文件（已退市、数据过旧、损坏）
2. 将无效文件归档到 backup/invalid_kline/ 目录
3. 生成归档报告

使用方式:
    python scripts/pipeline/archive_invalid_kline.py --dry-run
    python scripts/pipeline/archive_invalid_kline.py
"""
import sys
import argparse
import logging
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("archive_invalid_kline")

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import json
from core.delisting_guard import get_delisting_guard


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='无效K线文件归档')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='干运行模式（只分析不执行）'
    )
    parser.add_argument(
        '--max-age-days',
        type=int,
        default=365,
        help='最大数据年龄（天），超过视为无效（默认365）'
    )
    parser.add_argument(
        '--archive-dir',
        type=str,
        default='backup/invalid_kline',
        help='归档目录（默认 backup/invalid_kline）'
    )
    return parser.parse_args()


def is_valid_kline_file(kline_file: Path, max_age_days: int, target_date: datetime) -> Tuple[bool, str]:
    """
    检查K线文件是否有效
    
    Returns:
        (是否有效, 原因)
    """
    code = kline_file.stem
    
    try:
        # 尝试读取文件
        df = pl.read_parquet(kline_file)
        
        # Polars DataFrame 使用 len() 检查是否为空
        if len(df) == 0:
            return False, "文件为空"
        
        # 检查必要列
        if 'date' not in df.columns and 'trade_date' not in df.columns:
            return False, "缺少日期列"
        
        # 获取最新日期
        if 'date' in df.columns:
            latest_str = str(df['date'].max())
        else:
            latest_str = str(df['trade_date'].max())
        
        latest = datetime.strptime(latest_str, '%Y-%m-%d')
        days_diff = (target_date - latest).days
        
        # 检查数据年龄
        if days_diff > max_age_days:
            return False, f"数据过旧 ({days_diff} 天前)"
        
        # 检查数据行数
        if len(df) < 10:
            return False, f"数据行数不足 ({len(df)} 行)"
        
        return True, "有效"
        
    except Exception as e:
        return False, f"文件损坏: {e}"


def archive_invalid_kline(
    max_age_days: int = 365,
    archive_dir: str = 'backup/invalid_kline',
    dry_run: bool = False
) -> Dict:
    """
    归档无效K线文件
    
    Returns:
        归档统计
    """
    logger.info("=" * 60)
    logger.info("🗂️  无效K线文件归档")
    logger.info("=" * 60)
    logger.info(f"模式: {'干运行' if dry_run else '实际执行'}")
    logger.info(f"最大数据年龄: {max_age_days} 天")
    logger.info(f"归档目录: {archive_dir}")
    
    kline_dir = Path("data/kline")
    if not kline_dir.exists():
        logger.error(f"❌ K线目录不存在: {kline_dir}")
        return {}
    
    archive_path = Path(archive_dir)
    if not dry_run:
        archive_path.mkdir(parents=True, exist_ok=True)
    
    delisting_guard = get_delisting_guard()
    target_date = datetime.now()
    
    # 获取股票列表
    try:
        stock_list_df = pl.read_parquet("data/stock_list.parquet")
        valid_codes = set(stock_list_df['code'].to_list())
        logger.info(f"📋 有效股票列表: {len(valid_codes)} 只")
    except Exception as e:
        logger.error(f"❌ 读取股票列表失败: {e}")
        valid_codes = set()
    
    # 分类统计
    valid_files = []           # 有效文件
    delisted_files = []        # 已退市
    outdated_files = []        # 数据过旧
    corrupted_files = []       # 文件损坏
    not_in_list_files = []     # 不在股票列表中
    
    parquet_files = list(kline_dir.glob("*.parquet"))
    logger.info(f"📁 扫描K线文件: {len(parquet_files)} 个")
    
    for i, kline_file in enumerate(parquet_files, 1):
        code = kline_file.stem
        
        if i % 500 == 0:
            logger.info(f"  进度: {i}/{len(parquet_files)}")
        
        # 检查是否已退市
        if delisting_guard.is_delisted_by_code(code):
            delisted_files.append({
                'code': code,
                'file': kline_file,
                'reason': '已退市'
            })
            continue
        
        # 检查是否在股票列表中（仅当列表存在时）
        if valid_codes and code not in valid_codes:
            not_in_list_files.append({
                'code': code,
                'file': kline_file,
                'reason': '不在股票列表中'
            })
            continue
        
        # 检查文件有效性
        is_valid, reason = is_valid_kline_file(kline_file, max_age_days, target_date)
        
        if is_valid:
            valid_files.append({
                'code': code,
                'file': kline_file
            })
        else:
            if '损坏' in reason:
                corrupted_files.append({
                    'code': code,
                    'file': kline_file,
                    'reason': reason
                })
            else:
                outdated_files.append({
                    'code': code,
                    'file': kline_file,
                    'reason': reason
                })
    
    # 统计
    total_invalid = len(delisted_files) + len(outdated_files) + len(corrupted_files) + len(not_in_list_files)
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 分析结果")
    logger.info("=" * 60)
    logger.info(f"总文件数: {len(parquet_files)}")
    logger.info(f"✅ 有效: {len(valid_files)}")
    logger.info(f"🚫 已退市: {len(delisted_files)}")
    logger.info(f"⚠️  不在列表: {len(not_in_list_files)}")
    logger.info(f"⏰ 数据过旧: {len(outdated_files)}")
    logger.info(f"💥 文件损坏: {len(corrupted_files)}")
    logger.info(f"📦 待归档: {total_invalid}")
    
    # 执行归档
    if not dry_run and total_invalid > 0:
        logger.info("\n" + "=" * 60)
        logger.info("📦 开始归档")
        logger.info("=" * 60)
        
        archived_count = 0
        failed_count = 0
        
        all_invalid = delisted_files + not_in_list_files + outdated_files + corrupted_files
        
        for item in all_invalid:
            try:
                src_file = item['file']
                dst_file = archive_path / src_file.name
                
                # 移动文件
                shutil.move(str(src_file), str(dst_file))
                archived_count += 1
                
                if archived_count % 50 == 0:
                    logger.info(f"  已归档: {archived_count}/{total_invalid}")
                    
            except Exception as e:
                logger.error(f"❌ 归档失败 {item['code']}: {e}")
                failed_count += 1
        
        logger.info(f"✅ 归档完成: {archived_count} 个")
        if failed_count > 0:
            logger.warning(f"⚠️  失败: {failed_count} 个")
    
    # 生成报告
    result = {
        'archive_time': datetime.now().isoformat(),
        'mode': 'dry_run' if dry_run else 'actual',
        'max_age_days': max_age_days,
        'archive_dir': str(archive_path),
        'summary': {
            'total_files': len(parquet_files),
            'valid': len(valid_files),
            'delisted': len(delisted_files),
            'not_in_list': len(not_in_list_files),
            'outdated': len(outdated_files),
            'corrupted': len(corrupted_files),
            'total_invalid': total_invalid
        },
        'details': {
            'delisted': [{'code': x['code'], 'reason': x['reason']} for x in delisted_files],
            'not_in_list': [{'code': x['code'], 'reason': x['reason']} for x in not_in_list_files],
            'outdated': [{'code': x['code'], 'reason': x['reason']} for x in outdated_files],
            'corrupted': [{'code': x['code'], 'reason': x['reason']} for x in corrupted_files]
        }
    }
    
    # 保存报告
    report_path = Path("data/archive_invalid_kline_report.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n📄 报告已保存: {report_path}")
    
    return result


def main():
    """主函数"""
    args = parse_args()
    
    result = archive_invalid_kline(
        max_age_days=args.max_age_days,
        archive_dir=args.archive_dir,
        dry_run=args.dry_run
    )
    
    if not result:
        return 1
    
    # 返回码
    if args.dry_run:
        logger.info("\n🏃 干运行完成，未实际归档")
        return 0
    else:
        total_invalid = result['summary']['total_invalid']
        if total_invalid > 0:
            logger.info(f"\n✅ 归档完成，清理了 {total_invalid} 个无效文件")
        else:
            logger.info("\n✅ 没有无效文件需要归档")
        return 0


if __name__ == "__main__":
    sys.exit(main())
