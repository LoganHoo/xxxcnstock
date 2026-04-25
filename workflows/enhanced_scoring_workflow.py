#!/usr/bin/env python3
"""
增强版评分计算工作流

基于WorkflowExecutor框架，提供：
- 依赖检查
- 自动重试
- 断点续传
- 自动修复
- 发送报告
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import polars as pl
import numpy as np

from core.workflow_framework import (
    WorkflowExecutor, WorkflowStatus, DependencyCheck, DependencyStatus,
    RetryConfig, Checkpoint, workflow_step
)
from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.quality.ge_checkpoint_validators import GECheckpointValidators, CheckStatus, GERetryConfig


class EnhancedScoringWorkflow(WorkflowExecutor):
    """增强版评分计算工作流"""
    
    def __init__(self):
        super().__init__(
            workflow_name="scoring",
            retry_config=RetryConfig(max_retries=2, retry_delay=0.5, backoff_factor=2.0),
            enable_checkpoint=True,
            enable_auto_fix=True
        )
        
        retry_config = GERetryConfig(max_retries=2, retry_delay=0.5)
        self.checkpoint_validator = GECheckpointValidators(retry_config)
        
        self.data_dir = get_data_path()
        self.kline_dir = self.data_dir / "kline"
        self.scores_path = self.data_dir / "enhanced_scores_full.parquet"
        
        # 计算状态
        self.processed_codes = []
        self.failed_codes = []
        self.scores_results = []
        self.total_codes = 0
    
    def check_dependencies(self) -> List[DependencyCheck]:
        """检查依赖"""
        checks = []
        
        # 1. 检查K线数据目录
        if self.kline_dir.exists():
            kline_files = list(self.kline_dir.glob("*.parquet"))
            if len(kline_files) > 0:
                checks.append(DependencyCheck(
                    name="K线数据",
                    status=DependencyStatus.HEALTHY,
                    message=f"K线数据存在: {len(kline_files)}只股票"
                ))
            else:
                checks.append(DependencyCheck(
                    name="K线数据",
                    status=DependencyStatus.UNHEALTHY,
                    message="K线数据目录为空"
                ))
        else:
            checks.append(DependencyCheck(
                name="K线数据",
                status=DependencyStatus.UNHEALTHY,
                message=f"K线数据目录不存在: {self.kline_dir}"
            ))
        
        # 2. 检查股票列表
        stock_list_path = self.data_dir / "stock_list.parquet"
        if stock_list_path.exists():
            try:
                df = pl.read_parquet(stock_list_path)
                checks.append(DependencyCheck(
                    name="股票列表",
                    status=DependencyStatus.HEALTHY,
                    message=f"股票列表存在: {len(df)}只股票"
                ))
            except Exception as e:
                checks.append(DependencyCheck(
                    name="股票列表",
                    status=DependencyStatus.UNHEALTHY,
                    message=f"股票列表读取失败: {e}"
                ))
        else:
            checks.append(DependencyCheck(
                name="股票列表",
                status=DependencyStatus.UNHEALTHY,
                message="股票列表不存在"
            ))
        
        # 3. 检查磁盘空间
        try:
            import shutil
            stat = shutil.disk_usage(self.data_dir)
            free_gb = stat.free / (1024**3)
            
            if free_gb < 0.5:
                checks.append(DependencyCheck(
                    name="磁盘空间",
                    status=DependencyStatus.UNHEALTHY,
                    message=f"磁盘空间不足: {free_gb:.2f}GB"
                ))
            else:
                checks.append(DependencyCheck(
                    name="磁盘空间",
                    status=DependencyStatus.HEALTHY,
                    message=f"磁盘空间充足: {free_gb:.2f}GB"
                ))
        except Exception as e:
            checks.append(DependencyCheck(
                name="磁盘空间",
                status=DependencyStatus.UNKNOWN,
                message=f"无法检查磁盘空间: {e}"
            ))
        
        return checks
    
    def auto_fix_dependency(self, dependency: DependencyCheck) -> bool:
        """自动修复依赖问题"""
        self.logger.info(f"尝试自动修复: {dependency.name}")
        
        # 目前评分工作流没有可自动修复的问题
        return False
    
    def execute(
        self,
        date: Optional[str] = None,
        codes: Optional[List[str]] = None,
        resume: bool = False,
        checkpoint: Optional[Checkpoint] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行评分计算
        
        Args:
            date: 指定日期
            codes: 指定股票代码
            resume: 是否从断点恢复
            checkpoint: 断点信息
        """
        start_time = time.time()
        
        # 确定评分日期
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info(f"\n📅 评分日期: {date}")
        
        # 获取股票列表
        self.logger.info("\n📋 获取股票列表")
        try:
            stock_list_df = pl.read_parquet(self.data_dir / "stock_list.parquet")
            self.total_codes = len(stock_list_df)
            self.logger.info(f"   股票总数: {self.total_codes}")
        except Exception as e:
            return {
                'status': 'failed',
                'errors': [f"获取股票列表失败: {e}"],
                'summary': {'date': date}
            }
        
        # 确定要处理的股票
        if codes:
            target_codes = codes
        else:
            target_codes = stock_list_df['code'].tolist()
        
        # 如果从断点恢复
        if resume and checkpoint:
            completed = set(checkpoint.completed_items)
            target_codes = [c for c in target_codes if c not in completed]
            self.processed_codes = checkpoint.completed_items.copy()
            self.failed_codes = checkpoint.failed_items.copy()
            self.scores_results = checkpoint.metadata.get('scores', [])
            self.logger.info(f"   从断点恢复，跳过{len(completed)}只已处理股票")
        
        self.logger.info(f"   本次处理: {len(target_codes)}只股票")
        
        # 批量处理
        self.logger.info("\n🧮 开始计算评分")
        batch_size = 100
        total_batches = (len(target_codes) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, len(target_codes))
            batch_codes = target_codes[batch_start:batch_end]
            
            self.logger.info(f"\n   批次 {batch_idx + 1}/{total_batches}: {len(batch_codes)}只股票")
            
            for code in batch_codes:
                try:
                    # 计算单只股票评分
                    score_data = self._calculate_single_score(code, date, stock_list_df)
                    if score_data:
                        self.scores_results.append(score_data)
                        self.processed_codes.append(code)
                except Exception as e:
                    self.logger.debug(f"   ❌ {code}: 计算失败 - {e}")
                    self.failed_codes.append(code)
            
            # 保存断点
            checkpoint = Checkpoint(
                workflow_id=self.workflow_id,
                step_name="calculate_scores",
                step_index=batch_idx,
                completed_items=self.processed_codes.copy(),
                failed_items=self.failed_codes.copy(),
                metadata={
                    'date': date,
                    'total': self.total_codes,
                    'scores': self.scores_results.copy()
                }
            )
            self.save_checkpoint(checkpoint)
            
            self.logger.info(f"   进度: {len(self.processed_codes)}/{self.total_codes} "
                           f"({len(self.processed_codes)/self.total_codes*100:.1f}%)")
        
        # 检查点4: 计算后验证
        self.logger.info("\n🔍 检查点4: 计算后验证")
        
        if len(self.scores_results) == 0:
            return {
                'status': 'failed',
                'errors': ["没有成功计算任何股票的评分"],
                'summary': {'date': date}
            }
        
        # 创建评分DataFrame
        scores_df = pl.DataFrame(self.scores_results)
        
        validation = self.checkpoint_validator.post_scoring_validation(scores_df)
        self.logger.info(f"   结果: {validation.status.value} - {validation.message}")
        
        if validation.status == CheckStatus.FAILED:
            return {
                'status': 'partial',
                'errors': [f"计算后验证失败: {validation.message}"],
                'warnings': [f"失败股票: {len(self.failed_codes)}只"],
                'summary': {
                    'date': date,
                    'total': self.total_codes,
                    'processed': len(self.processed_codes),
                    'failed': len(self.failed_codes),
                    'success_rate': f"{len(self.processed_codes)/self.total_codes*100:.1f}%"
                }
            }
        
        # 保存评分结果
        self.logger.info("\n💾 保存评分结果")
        try:
            scores_df.write_parquet(self.scores_path)
            self.logger.info(f"   已保存: {self.scores_path}")
        except Exception as e:
            return {
                'status': 'partial',
                'errors': [f"保存评分结果失败: {e}"],
                'summary': {
                    'date': date,
                    'total': self.total_codes,
                    'processed': len(self.processed_codes)
                }
            }
        
        duration = time.time() - start_time
        
        # 确定最终状态
        if len(self.failed_codes) == 0:
            status = 'completed'
        elif len(self.processed_codes) > 0:
            status = 'partial'
        else:
            status = 'failed'
        
        return {
            'status': status,
            'errors': [f"失败股票: {self.failed_codes[:10]}..."] if self.failed_codes else [],
            'warnings': [f"部分股票计算失败: {len(self.failed_codes)}只"] if self.failed_codes > 0 else [],
            'summary': {
                'date': date,
                'total': self.total_codes,
                'processed': len(self.processed_codes),
                'failed': len(self.failed_codes),
                'duration_seconds': f"{duration:.2f}",
                'success_rate': f"{len(self.processed_codes)/self.total_codes*100:.1f}%",
                'avg_score': f"{scores_df['enhanced_score'].mean():.2f}",
                'max_score': f"{scores_df['enhanced_score'].max():.0f}",
                'min_score': f"{scores_df['enhanced_score'].min():.0f}"
            }
        }
    
    def _calculate_single_score(self, code: str, date: str, stock_list_df: pl.DataFrame) -> Optional[Dict]:
        """计算单只股票评分"""
        try:
            # 读取K线数据
            kline_path = self.kline_dir / f"{code}.parquet"
            if not kline_path.exists():
                return None
            
            df = pl.read_parquet(kline_path)
            
            if len(df) < 20:
                return None
            
            # 检查点3: 计算前检查
            pre_check = self.checkpoint_validator.pre_scoring_check(df, code)
            if pre_check.status == CheckStatus.FAILED:
                return None
            
            # 获取最新数据
            latest = df.tail(1).to_dicts()[0]
            close = float(latest.get('close', 0) or 0)
            volume = float(latest.get('volume', 0) or 0)
            
            # 计算均线
            ma5 = float(df.tail(5)['close'].mean() or close)
            ma10 = float(df.tail(10)['close'].mean() or close)
            ma20 = float(df.tail(20)['close'].mean() or close)
            
            # 趋势得分
            trend_score = 0
            if close > ma5:
                trend_score += 20
            if close > ma10:
                trend_score += 15
            if close > ma20:
                trend_score += 15
            
            # 动量得分
            if len(df) >= 2:
                prev_close = float(df.tail(2).head(1)['close'].to_list()[0] or 0)
                change_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
                if change_pct > 0:
                    momentum_score = min(30, change_pct * 3)
                else:
                    momentum_score = max(0, 15 + change_pct)
            else:
                momentum_score = 15
            
            # 成交量得分
            if len(df) >= 20:
                avg_volume = float(df.tail(20)['volume'].mean() or 0)
                if avg_volume > 0:
                    volume_ratio = volume / avg_volume
                    volume_score = min(20, volume_ratio * 10)
                else:
                    volume_score = 10
            else:
                volume_score = 10
            
            total_score = trend_score + momentum_score + volume_score
            total_score = min(100, max(0, total_score))
            
            # 获取股票名称
            name = ""
            try:
                name = stock_list_df.filter(pl.col('code') == code)['name'].to_list()[0]
            except:
                pass
            
            return {
                'code': code,
                'name': name,
                'enhanced_score': round(total_score, 2),
                'trade_date': date,
                'close': close,
                'volume': volume,
                'ma5': ma5,
                'ma10': ma10,
                'ma20': ma20
            }
            
        except Exception as e:
            raise Exception(f"计算失败: {e}")


def run_scoring(
    date: Optional[str] = None,
    codes: Optional[List[str]] = None,
    resume: bool = False
) -> Dict[str, Any]:
    """
    运行评分计算工作流
    
    Args:
        date: 指定日期
        codes: 指定股票代码
        resume: 是否从断点恢复
        
    Returns:
        执行结果
    """
    workflow = EnhancedScoringWorkflow()
    report = workflow.run(date=date, codes=codes, resume=resume)
    return report.to_dict()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="评分计算工作流")
    parser.add_argument("--date", help="指定日期 (YYYY-MM-DD)")
    parser.add_argument("--codes", nargs="+", help="指定股票代码")
    parser.add_argument("--resume", action="store_true", help="从断点恢复")
    
    args = parser.parse_args()
    
    result = run_scoring(
        date=args.date,
        codes=args.codes,
        resume=args.resume
    )
    
    print("\n" + "=" * 60)
    print("执行结果:")
    print("=" * 60)
    print(f"状态: {result['status']}")
    print(f"时长: {result['duration_seconds']:.2f}秒")
    
    if result['errors']:
        print(f"\n错误:")
        for error in result['errors']:
            print(f"  - {error}")
    
    if result['summary']:
        print(f"\n汇总:")
        for key, value in result['summary'].items():
            print(f"  {key}: {value}")
