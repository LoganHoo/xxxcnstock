#!/usr/bin/env python3
"""
数据新鲜度验证脚本

功能：
1. 验证所有股票数据是否最新
2. 识别数据过期的股票
3. 生成详细的新鲜度报告

使用方式:
    python scripts/pipeline/check_data_freshness.py
    python scripts/pipeline/check_data_freshness.py --date 2026-04-24
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("check_data_freshness")

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import pandas as pd
from core.delisting_guard import get_delisting_guard


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='数据新鲜度验证')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='目标日期 YYYY-MM-DD（默认今天）'
    )
    parser.add_argument(
        '--max-age-days',
        type=int,
        default=3,
        help='最大数据年龄（天），超过视为过期（默认3）'
    )
    return parser.parse_args()


def check_data_freshness(target_date: str, max_age_days: int = 3) -> Dict:
    """
    检查数据新鲜度
    
    Returns:
        {
            'total': 总股票数,
            'up_to_date': 最新数量,
            'outdated': 过期数量,
            'delisted': 已退市数量,
            'suspended': 停牌数量,
            'missing': 缺失数量,
            'up_to_date_rate': 最新率,
            'details': 详细列表
        }
    """
    logger.info("=" * 60)
    logger.info("🔍 数据新鲜度检查")
    logger.info("=" * 60)
    logger.info(f"目标日期: {target_date}")
    logger.info(f"最大数据年龄: {max_age_days} 天")
    
    # 获取股票列表
    try:
        stock_list_df = pl.read_parquet("data/stock_list.parquet")
        all_codes = set(stock_list_df['code'].to_list())
        logger.info(f"📋 股票列表: {len(all_codes)} 只")
    except Exception as e:
        logger.error(f"❌ 读取股票列表失败: {e}")
        return {}
    
    kline_dir = Path("data/kline")
    if not kline_dir.exists():
        logger.error(f"❌ K线目录不存在: {kline_dir}")
        return {}
    
    delisting_guard = get_delisting_guard()
    
    target = datetime.strptime(target_date, '%Y-%m-%d')
    cutoff_date = target - timedelta(days=max_age_days)
    
    up_to_date = []      # 最新
    outdated = []        # 过期
    delisted = []        # 已退市
    suspended = []       # 停牌
    missing = []         # 缺失
    error_files = []     # 读取错误
    
    # 遍历所有Parquet文件
    parquet_files = list(kline_dir.glob("*.parquet"))
    logger.info(f"📁 K线文件数: {len(parquet_files)}")
    
    for i, kline_file in enumerate(parquet_files, 1):
        code = kline_file.stem
        
        if i % 500 == 0:
            logger.info(f"  进度: {i}/{len(parquet_files)}")
        
        try:
            df = pl.read_parquet(kline_file)
            
            # 获取最新日期
            if 'date' in df.columns:
                latest_str = str(df['date'].max())
            elif 'trade_date' in df.columns:
                latest_str = str(df['trade_date'].max())
            else:
                logger.warning(f"⚠️ {code}: 未找到日期列")
                error_files.append((code, "未找到日期列"))
                continue
            
            latest = datetime.strptime(latest_str, '%Y-%m-%d')
            days_diff = (target - latest).days
            
            # 检查是否已退市
            if delisting_guard.is_delisted_by_code(code):
                delisted.append({
                    'code': code,
                    'latest_date': latest_str,
                    'days_diff': days_diff
                })
                continue
            
            # 检查是否在股票列表中
            if code not in all_codes:
                delisted.append({
                    'code': code,
                    'latest_date': latest_str,
                    'days_diff': days_diff,
                    'note': '不在股票列表中'
                })
                continue
            
            # 分类
            if latest_str == target_date:
                up_to_date.append({
                    'code': code,
                    'latest_date': latest_str,
                    'days_diff': 0
                })
            elif days_diff <= max_age_days:
                # 在允许范围内，可能是停牌
                suspended.append({
                    'code': code,
                    'latest_date': latest_str,
                    'days_diff': days_diff
                })
            else:
                # 数据过期
                outdated.append({
                    'code': code,
                    'latest_date': latest_str,
                    'days_diff': days_diff
                })
        
        except Exception as e:
            logger.warning(f"⚠️ {code}: 读取失败 - {e}")
            error_files.append((code, str(e)))
    
    # 检查缺失的股票（在列表中但没有K线文件）
    existing_codes = {f.stem for f in parquet_files}
    missing_codes = all_codes - existing_codes
    for code in missing_codes:
        if not delisting_guard.is_delisted_by_code(code):
            missing.append({
                'code': code,
                'latest_date': None,
                'days_diff': None
            })
    
    # 统计
    total = len(up_to_date) + len(outdated) + len(delisted) + len(suspended) + len(missing)
    up_to_date_rate = len(up_to_date) / total if total > 0 else 0
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 数据新鲜度报告")
    logger.info("=" * 60)
    logger.info(f"总股票数: {total}")
    logger.info(f"✅ 最新: {len(up_to_date)} ({up_to_date_rate:.1%})")
    logger.info(f"⚠️  过期: {len(outdated)}")
    logger.info(f"⏸️  停牌: {len(suspended)}")
    logger.info(f"🚫 退市: {len(delisted)}")
    logger.info(f"❌ 缺失: {len(missing)}")
    logger.info(f"⚠️  错误: {len(error_files)}")
    
    # 日期分布
    if up_to_date:
        logger.info(f"\n📅 最新日期分布（前10）:")
        date_counts = Counter([s['latest_date'] for s in up_to_date + suspended + outdated])
        for date, count in sorted(date_counts.items(), reverse=True)[:10]:
            status = '✅' if date == target_date else '⚠️'
            logger.info(f"  {status} {date}: {count} 只")
    
    # 过期股票示例
    if outdated:
        logger.info(f"\n⚠️ 过期股票示例（前10）:")
        for s in sorted(outdated, key=lambda x: x['days_diff'], reverse=True)[:10]:
            logger.info(f"  {s['code']}: {s['latest_date']} ({s['days_diff']} 天前)")
    
    return {
        'target_date': target_date,
        'check_time': datetime.now().isoformat(),
        'total': total,
        'up_to_date': len(up_to_date),
        'outdated': len(outdated),
        'delisted': len(delisted),
        'suspended': len(suspended),
        'missing': len(missing),
        'errors': len(error_files),
        'up_to_date_rate': up_to_date_rate,
        'details': {
            'up_to_date': up_to_date,
            'outdated': outdated,
            'delisted': delisted,
            'suspended': suspended,
            'missing': missing,
            'errors': error_files
        }
    }


def generate_report(result: Dict):
    """生成详细报告"""
    if not result:
        return
    
    # 保存JSON报告
    import json
    report_path = Path("data/data_freshness_report.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n📄 详细报告已保存: {report_path}")
    
    # 生成CSV报告（过期股票）
    if result['details']['outdated']:
        outdated_df = pd.DataFrame(result['details']['outdated'])
        csv_path = Path("data/outdated_stocks.csv")
        outdated_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"📄 过期股票列表: {csv_path}")


def main():
    """主函数"""
    args = parse_args()
    
    # 确定目标日期
    if args.date:
        target_date = args.date
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    # 执行检查
    result = check_data_freshness(target_date, args.max_age_days)
    
    # 生成报告
    if result:
        generate_report(result)
        
        # 返回码
        if result['up_to_date_rate'] >= 0.95:
            logger.info(f"\n✅ 数据新鲜度达标: {result['up_to_date_rate']:.1%}")
            return 0
        else:
            logger.warning(f"\n⚠️ 数据新鲜度不足: {result['up_to_date_rate']:.1%}")
            return 1
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
