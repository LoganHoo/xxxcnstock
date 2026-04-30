#!/usr/bin/env python3
"""
CVD (Cumulative Volume Delta) 累积成交量差指标计算
CVD = 当日成交量 × (收盘价 - 开盘价) / (最高价 - 最低价)
用于衡量资金流向和买卖压力
"""
import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("calculate_cvd")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='CVD指标计算')
    parser.add_argument('--date', type=str, default=None, help='目标日期 YYYY-MM-DD')
    parser.add_argument('--days', type=int, default=60, help='累积天数（默认60日）')
    parser.add_argument('--output', type=str, default=None, help='输出文件路径')
    return parser.parse_args()


def calculate_cvd_for_stock(df, days=60):
    """
    计算单只股票的CVD指标
    
    Args:
        df: DataFrame包含ohlcv数据
        days: 累积天数
    
    Returns:
        float: CVD值
    """
    if len(df) < days:
        days = len(df)
    
    if days == 0:
        return 0.0
    
    # 取最近N天数据
    recent_df = df.tail(days)
    
    cvd_sum = 0.0
    for row in recent_df.itertuples():
        open_price = getattr(row, 'open', 0)
        high_price = getattr(row, 'high', 0)
        low_price = getattr(row, 'low', 0)
        close_price = getattr(row, 'close', 0)
        volume = getattr(row, 'volume', 0)
        
        # 避免除零
        price_range = high_price - low_price
        if price_range == 0:
            continue
        
        # CVD公式
        delta = (close_price - open_price) / price_range
        cvd_sum += volume * delta
    
    return cvd_sum


def calculate_cvd_for_all_stocks(target_date: str, days: int = 60) -> dict:
    """
    计算所有股票的CVD指标
    
    Args:
        target_date: 目标日期
        days: 累积天数
    
    Returns:
        dict: {stock_code: cvd_value}
    """
    project_root = Path(__file__).parent.parent.parent
    kline_dir = project_root / "data" / "kline"
    
    if not kline_dir.exists():
        logger.error(f"K线数据目录不存在: {kline_dir}")
        return {}
    
    import polars as pl
    
    results = {}
    parquet_files = list(kline_dir.glob("*.parquet"))
    
    logger.info(f"开始计算 {len(parquet_files)} 只股票的CVD指标...")
    
    for i, file_path in enumerate(parquet_files):
        code = file_path.stem
        
        try:
            df = pl.read_parquet(file_path)
            
            # 转换为pandas便于处理
            df_pd = df.to_pandas()
            
            # 按日期排序
            if 'trade_date' in df_pd.columns:
                df_pd = df_pd.sort_values('trade_date')
            
            # 计算CVD
            cvd_value = calculate_cvd_for_stock(df_pd, days)
            results[code] = cvd_value
            
            if (i + 1) % 500 == 0:
                logger.info(f"已处理 {i + 1}/{len(parquet_files)} 只股票...")
                
        except Exception as e:
            logger.warning(f"计算 {code} CVD失败: {e}")
            continue
    
    logger.info(f"CVD计算完成，成功 {len(results)}/{len(parquet_files)} 只股票")
    return results


def save_cvd_results(results: dict, target_date: str, output_path: str = None):
    """保存CVD结果"""
    import json
    
    if output_path is None:
        project_root = Path(__file__).parent.parent.parent
        output_dir = project_root / "data" / "cvd"
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / f"cvd_{target_date}.json"
    
    # 按CVD值排序
    sorted_results = dict(sorted(results.items(), key=lambda x: abs(x[1]), reverse=True))
    
    output_data = {
        'calc_date': target_date,
        'calc_time': datetime.now().isoformat(),
        'total_stocks': len(results),
        'cvd_values': sorted_results
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"CVD结果已保存: {output_path}")
    
    # 输出Top10
    top10 = list(sorted_results.items())[:10]
    logger.info("CVD Top10 (资金净流入):")
    for code, cvd in top10:
        logger.info(f"  {code}: {cvd:,.0f}")


def main():
    """主函数"""
    args = parse_args()
    
    logger.info("=" * 60)
    logger.info("开始计算 CVD 指标")
    logger.info(f"累积天数: {args.days}")
    logger.info("=" * 60)
    
    # 确定目标日期
    if args.date:
        target_date = args.date
    else:
        from core.trading_calendar import get_recent_trade_dates
        trade_dates = get_recent_trade_dates(1)
        target_date = trade_dates[0] if trade_dates else datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"目标日期: {target_date}")
    
    # 计算CVD
    results = calculate_cvd_for_all_stocks(target_date, args.days)
    
    if not results:
        logger.error("CVD计算失败，无有效结果")
        return 1
    
    # 保存结果
    save_cvd_results(results, target_date, args.output)
    
    logger.info("=" * 60)
    logger.info("✅ CVD 指标计算完成")
    logger.info("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
