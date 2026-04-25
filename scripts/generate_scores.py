#!/usr/bin/env python3
"""
生成股票评分文件
基于K线数据计算综合评分
"""
import sys
from pathlib import Path
import polars as pl
import numpy as np
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger
from services.data_service.quality.ge_checkpoint_validators import (
    GECheckpointValidators,
    CheckStatus,
    GERetryConfig
)

logger = setup_logger("generate_scores")


def calculate_technical_score(df: pl.DataFrame) -> float:
    """计算技术面评分"""
    if len(df) < 20:
        return 50.0
    
    # 获取最新数据
    latest = df.tail(1).to_dicts()[0]
    close = latest.get('close', 0) or 0
    high = latest.get('high', 0) or 0
    low = latest.get('low', 0) or 0
    volume = latest.get('volume', 0) or 0
    
    # 处理None值
    close = float(close) if close is not None else 0
    volume = float(volume) if volume is not None else 0
    
    # 计算均线
    if len(df) >= 5:
        ma5 = df.tail(5)['close'].mean()
        ma5 = float(ma5) if ma5 is not None else close
    else:
        ma5 = close
    
    if len(df) >= 10:
        ma10 = df.tail(10)['close'].mean()
        ma10 = float(ma10) if ma10 is not None else close
    else:
        ma10 = close
    
    if len(df) >= 20:
        ma20 = df.tail(20)['close'].mean()
        ma20 = float(ma20) if ma20 is not None else close
    else:
        ma20 = close
    
    # 趋势得分 (价格在均线上方)
    trend_score = 0
    if close > ma5:
        trend_score += 20
    if close > ma10:
        trend_score += 15
    if close > ma20:
        trend_score += 15
    
    # 动量得分
    if len(df) >= 2:
        try:
            prev_close = df.tail(2).head(1)['close'].to_list()[0]
            prev_close = float(prev_close) if prev_close is not None else 0
            change_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
            if change_pct > 0:
                momentum_score = min(30, change_pct * 3)
            else:
                momentum_score = max(0, 15 + change_pct)
        except:
            momentum_score = 15
    else:
        momentum_score = 15
    
    # 成交量得分
    if len(df) >= 20:
        try:
            avg_volume = df.tail(20)['volume'].mean()
            avg_volume = float(avg_volume) if avg_volume is not None else 0
            if avg_volume > 0:
                volume_ratio = volume / avg_volume
                volume_score = min(20, volume_ratio * 10)
            else:
                volume_score = 10
        except:
            volume_score = 10
    else:
        volume_score = 10
    
    total_score = trend_score + momentum_score + volume_score
    return min(100, max(0, total_score))


def generate_scores():
    """生成所有股票评分"""
    logger.info("开始生成股票评分...")

    kline_dir = project_root / "data" / "kline"
    stock_list_path = project_root / "data" / "stock_list.parquet"

    if not stock_list_path.exists():
        logger.error("股票列表文件不存在")
        return

    # 读取股票列表
    stock_list = pl.read_parquet(stock_list_path)
    logger.info(f"股票列表: {len(stock_list)} 只")

    # 初始化GE检查点验证器（配置重试2次）
    retry_config = GERetryConfig(max_retries=2, retry_delay=0.5)
    checkpoint_validator = GECheckpointValidators(retry_config)

    scores = []
    total = len(stock_list)
    skipped_count = 0
    quality_check_results = []

    for i, row in enumerate(stock_list.iter_rows(named=True)):
        code = row.get('code')
        name = row.get('name', '')

        if i % 500 == 0:
            logger.info(f"处理进度: {i}/{total}")

        # 读取K线数据
        kline_path = kline_dir / f"{code}.parquet"
        if not kline_path.exists():
            continue

        try:
            df = pl.read_parquet(kline_path)
            if len(df) == 0:
                continue

            # 检查点3: 计算前检查
            pre_check = checkpoint_validator.pre_scoring_check(df, code)
            quality_check_results.append(pre_check.to_dict())

            if pre_check.status == CheckStatus.FAILED:
                logger.debug(f"{code}: {pre_check.message}")
                skipped_count += 1
                continue

            # 获取最新数据
            latest = df.tail(1).to_dicts()[0]
            latest_date = latest.get('trade_date', '')
            close = latest.get('close', 0)

            # 计算评分
            tech_score = calculate_technical_score(df)
            
            # 综合评分 (技术面 + 基本面模拟)
            enhanced_score = int(tech_score * 0.8 + np.random.normal(70, 10) * 0.2)
            enhanced_score = min(150, max(0, enhanced_score))
            
            # 计算涨跌幅
            if len(df) >= 2:
                prev_close = df.tail(2).head(1)['close'].to_list()[0]
                change_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
            else:
                change_pct = 0
            
            # 获取成交量
            volume = latest.get('volume', 0)
            
            scores.append({
                'code': code,
                'name': name,
                'price': close,
                'enhanced_score': enhanced_score,
                'change_pct': round(change_pct, 2),
                'volume': volume,
                'trade_date': latest_date
            })
            
        except Exception as e:
            logger.warning(f"处理 {code} 失败: {e}")
            continue
    
    logger.info(f"评分完成: {len(scores)} 只股票")
    logger.info(f"因质量检查跳过: {skipped_count} 只股票")

    # 保存评分
    if scores:
        scores_df = pl.DataFrame(scores)

        # 检查点4: 计算后验证
        post_check = checkpoint_validator.post_scoring_validation(scores_df)
        logger.info(f"计算后验证: {post_check.status.value} - {post_check.message}")

        if post_check.status == CheckStatus.FAILED:
            logger.error("评分结果验证失败，不保存结果")
            return

        output_path = project_root / "data" / "enhanced_scores_full.parquet"
        scores_df.write_parquet(output_path)
        logger.info(f"评分已保存: {output_path}")

        # 显示统计
        logger.info(f"评分统计:")
        logger.info(f"  平均: {scores_df['enhanced_score'].mean():.1f}")
        logger.info(f"  最高: {scores_df['enhanced_score'].max()}")
        logger.info(f"  最低: {scores_df['enhanced_score'].min()}")

        # Top 20
        top20 = scores_df.sort('enhanced_score', descending=True).head(20)
        logger.info(f"\nTop 20:")
        for row in top20.iter_rows(named=True):
            logger.info(f"  {row['code']} {row['name']}: {row['enhanced_score']}")

        # 保存质量检查结果
        if quality_check_results:
            failed_checks = [r for r in quality_check_results if r['status'] == 'failed']
            passed_checks = [r for r in quality_check_results if r['status'] == 'passed']
            warning_checks = [r for r in quality_check_results if r['status'] == 'warning']

            logger.info(f"\n质量检查统计:")
            logger.info(f"  通过: {len(passed_checks)}")
            logger.info(f"  警告: {len(warning_checks)}")
            logger.info(f"  失败: {len(failed_checks)}")


if __name__ == "__main__":
    generate_scores()
