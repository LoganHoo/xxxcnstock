#!/usr/bin/env python3
"""
增强版选股工作流

基于WorkflowExecutor框架，提供：
- 依赖检查
- 自动重试
- 断点续传
- 自动修复
- 发送报告
- 保存到MySQL
- 邮件通知
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import polars as pl

from core.workflow_framework import (
    WorkflowExecutor, WorkflowStatus, DependencyCheck, DependencyStatus,
    RetryConfig, Checkpoint, workflow_step
)
from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.quality.ge_checkpoint_validators import GECheckpointValidators, CheckStatus, GERetryConfig
from services.selection_report_service_sqlite import SelectionReportService


class EnhancedSelectionWorkflow(WorkflowExecutor):
    """增强版选股工作流"""
    
    def __init__(self):
        super().__init__(
            workflow_name="stock_selection",
            retry_config=RetryConfig(max_retries=3, retry_delay=1.0, backoff_factor=2.0),
            enable_checkpoint=True,
            enable_auto_fix=True
        )
        
        retry_config = GERetryConfig(max_retries=3, retry_delay=1.0)
        self.checkpoint_validator = GECheckpointValidators(retry_config)
        
        self.data_dir = get_data_path()
        self.scores_path = self.data_dir / "enhanced_scores_full.parquet"
        self.output_dir = self.data_dir / "selection_results"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 报告服务
        self.report_service = SelectionReportService()
        
        # 选股状态
        self.selected_stocks = []
        self.filtered_stocks = []
    
    def check_dependencies(self) -> List[DependencyCheck]:
        """检查依赖"""
        checks = []
        
        # 1. 检查评分数据
        if self.scores_path.exists():
            try:
                df = pl.read_parquet(self.scores_path)
                checks.append(DependencyCheck(
                    name="评分数据",
                    status=DependencyStatus.HEALTHY,
                    message=f"评分数据存在: {len(df)}只股票"
                ))
            except Exception as e:
                checks.append(DependencyCheck(
                    name="评分数据",
                    status=DependencyStatus.UNHEALTHY,
                    message=f"评分数据读取失败: {e}"
                ))
        else:
            checks.append(DependencyCheck(
                name="评分数据",
                status=DependencyStatus.UNHEALTHY,
                message="评分数据不存在，请先运行评分计算"
            ))
        
        # 2. 检查股票列表
        stock_list_path = self.data_dir / "stock_list.parquet"
        if stock_list_path.exists():
            checks.append(DependencyCheck(
                name="股票列表",
                status=DependencyStatus.HEALTHY,
                message="股票列表存在"
            ))
        else:
            checks.append(DependencyCheck(
                name="股票列表",
                status=DependencyStatus.UNHEALTHY,
                message="股票列表不存在"
            ))
        
        # 3. 检查输出目录
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            checks.append(DependencyCheck(
                name="输出目录",
                status=DependencyStatus.HEALTHY,
                message=f"输出目录就绪: {self.output_dir}"
            ))
        except Exception as e:
            checks.append(DependencyCheck(
                name="输出目录",
                status=DependencyStatus.UNHEALTHY,
                message=f"输出目录创建失败: {e}"
            ))
        
        return checks
    
    def auto_fix_dependency(self, dependency: DependencyCheck) -> bool:
        """自动修复依赖问题"""
        self.logger.info(f"尝试自动修复: {dependency.name}")
        
        if dependency.name == "输出目录":
            try:
                self.output_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"✅ 已创建输出目录: {self.output_dir}")
                return True
            except Exception as e:
                self.logger.error(f"❌ 创建输出目录失败: {e}")
                return False
        
        return False
    
    def execute(
        self,
        date: Optional[str] = None,
        top_n: int = 20,
        min_score: float = 60,
        resume: bool = False,
        checkpoint: Optional[Checkpoint] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行选股
        
        Args:
            date: 指定日期
            top_n: 选取前N只股票
            min_score: 最低评分
            resume: 是否从断点恢复
            checkpoint: 断点信息
        """
        start_time = time.time()
        
        # 确定选股日期
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info(f"\n📅 选股日期: {date}")
        self.logger.info(f"   选取数量: 前{top_n}名")
        self.logger.info(f"   最低评分: {min_score}")
        
        # 加载评分数据
        self.logger.info("\n📊 加载评分数据")
        try:
            scores_df = pl.read_parquet(self.scores_path)
            self.logger.info(f"   评分数据: {len(scores_df)}只股票")
        except Exception as e:
            return {
                'status': 'failed',
                'errors': [f"加载评分数据失败: {e}"],
                'summary': {'date': date}
            }
        
        # 检查点5: 选股前检查
        self.logger.info("\n🔍 检查点5: 选股前检查")
        pre_check = self.checkpoint_validator.pre_selection_check(scores_df, date=date)
        self.logger.info(f"   结果: {pre_check.status.value} - {pre_check.message}")
        
        if pre_check.status == CheckStatus.FAILED:
            return {
                'status': 'failed',
                'errors': [f"选股前检查失败: {pre_check.message}"],
                'summary': {'date': date}
            }
        
        # 加载股票列表用于过滤
        self.logger.info("\n📋 加载股票列表")
        try:
            stock_list_df = pl.read_parquet(self.data_dir / "stock_list.parquet")
            self.logger.info(f"   股票列表: {len(stock_list_df)}只股票")
        except Exception as e:
            return {
                'status': 'failed',
                'errors': [f"加载股票列表失败: {e}"],
                'summary': {'date': date}
            }
        
        # 合并数据
        self.logger.info("\n🔗 合并评分和股票信息")
        merged_df = scores_df.join(stock_list_df, on='code', how='left')
        
        # 保存断点
        checkpoint = Checkpoint(
            workflow_id=self.workflow_id,
            step_name="merge_data",
            step_index=0,
            completed_items=["merge_data"],
            failed_items=[],
            metadata={'date': date, 'total_stocks': len(merged_df)}
        )
        self.save_checkpoint(checkpoint)
        
        # 过滤ST和退市股票
        self.logger.info("\n🚫 过滤ST和退市股票")
        filtered_df = merged_df.filter(
            ~pl.col('name').str.contains('ST|退市', strict=False)
        )
        removed_count = len(merged_df) - len(filtered_df)
        self.logger.info(f"   过滤掉: {removed_count}只")
        self.logger.info(f"   剩余: {len(filtered_df)}只")
        
        # 按评分排序
        self.logger.info("\n📈 按评分排序")
        sorted_df = filtered_df.sort('enhanced_score', descending=True)
        
        # 选取前N只
        self.logger.info(f"\n🏆 选取前{top_n}只股票")
        selected_df = sorted_df.head(top_n)
        
        # 检查点6: 最终输出验证
        self.logger.info("\n🔍 检查点6: 最终输出验证")
        
        # 准备输出数据
        # 检查可用的列
        available_cols = selected_df.columns
        select_cols = ['code', 'name', 'enhanced_score', 'trade_date']
        
        # 价格列可能是 'close' 或 'price'
        if 'close' in available_cols:
            select_cols.append('close')
        elif 'price' in available_cols:
            select_cols.append('price')
        
        # 可选列
        for col in ['volume', 'ma5', 'ma10', 'ma20', 'change_pct']:
            if col in available_cols:
                select_cols.append(col)
        
        output_df = selected_df.select(select_cols)
        
        validation = self.checkpoint_validator.final_output_validation(output_df)
        self.logger.info(f"   结果: {validation.status.value} - {validation.message}")
        
        if validation.status == CheckStatus.FAILED:
            return {
                'status': 'failed',
                'errors': [f"最终输出验证失败: {validation.message}"],
                'summary': {'date': date}
            }
        
        # 保存结果
        self.logger.info("\n💾 保存选股结果")
        output_path = self.output_dir / f"selection_{date}.parquet"
        csv_path = self.output_dir / f"selection_{date}.csv"
        
        try:
            output_df.write_parquet(output_path)
            output_df.write_csv(csv_path)
            self.logger.info(f"   Parquet: {output_path}")
            self.logger.info(f"   CSV: {csv_path}")
        except Exception as e:
            return {
                'status': 'partial',
                'errors': [f"保存结果失败: {e}"],
                'summary': {'date': date}
            }
        
        # 生成选股报告
        self.logger.info("\n📄 生成选股报告")
        report_lines = [
            f"# 选股报告 - {date}",
            "",
            f"**选股日期**: {date}",
            f"**选取数量**: 前{top_n}名",
            f"**最低评分**: {min_score}",
            f"**候选池**: {len(filtered_df)}只股票",
            "",
            "## 选股结果",
            "",
            "| 排名 | 代码 | 名称 | 评分 | 收盘价 | 成交量 |",
            "|------|------|------|------|--------|--------|"
        ]
        
        for idx, row in enumerate(output_df.to_dicts(), 1):
            # 获取价格列（可能是 close 或 price）
            price = row.get('close', row.get('price', 0))
            volume = row.get('volume', 0)
            report_lines.append(
                f"| {idx} | {row['code']} | {row['name']} | "
                f"{row['enhanced_score']:.1f} | {price:.2f} | {volume:.0f} |"
            )
        
        report_lines.extend([
            "",
            "## 统计信息",
            "",
            f"- 平均评分: {output_df['enhanced_score'].mean():.2f}",
            f"- 最高评分: {output_df['enhanced_score'].max():.2f}",
            f"- 最低评分: {output_df['enhanced_score'].min():.2f}",
        ])
        
        report_md = "\n".join(report_lines)
        report_path = self.output_dir / f"report_{date}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_md)
        
        self.logger.info(f"   报告: {report_path}")
        
        # 准备结果数据
        results = output_df.to_dicts()
        for i, item in enumerate(results, 1):
            item['rank'] = i
        
        # 保存到MySQL
        self.logger.info("\n💾 保存到MySQL数据库")
        try:
            self.report_service.save_selection_results(date, results)
            self.logger.info("   数据库保存成功")
        except Exception as e:
            self.logger.error(f"   数据库保存失败: {e}")
        
        # 记录评分数据更新时间
        scores_date = output_df['trade_date'][0] if len(output_df) > 0 else date
        self.report_service.record_data_update(
            data_type="enhanced_scores",
            data_date=scores_date,
            record_count=len(scores_df),
            details={'selection_count': len(results), 'min_score': min_score}
        )
        
        # 发送邮件报告
        self.logger.info("\n📧 发送邮件报告")
        try:
            email_success = self.report_service.send_email_report(
                report_date=date,
                results=results,
                recipient="287363@qq.com"
            )
            if email_success:
                self.logger.info("   邮件发送成功")
            else:
                self.logger.warning("   邮件发送失败")
        except Exception as e:
            self.logger.error(f"   邮件发送异常: {e}")
        
        duration = time.time() - start_time
        
        # 清除断点
        self.clear_checkpoint()
        
        return {
            'status': 'completed',
            'summary': {
                'date': date,
                'total_candidates': len(filtered_df),
                'selected': len(output_df),
                'filtered_out': removed_count,
                'avg_score': f"{output_df['enhanced_score'].mean():.2f}",
                'max_score': f"{output_df['enhanced_score'].max():.2f}",
                'min_score': f"{output_df['enhanced_score'].min():.2f}",
                'duration_seconds': f"{duration:.2f}",
                'output_files': {
                    'parquet': str(output_path),
                    'csv': str(csv_path),
                    'report': str(report_path)
                },
                'data_date': scores_date,
                'last_update_time': datetime.now().isoformat()
            }
        }


def run_selection(
    date: Optional[str] = None,
    top_n: int = 20,
    min_score: float = 60,
    resume: bool = False
) -> Dict[str, Any]:
    """
    运行选股工作流
    
    Args:
        date: 指定日期
        top_n: 选取前N只股票
        min_score: 最低评分
        resume: 是否从断点恢复
        
    Returns:
        执行结果
    """
    workflow = EnhancedSelectionWorkflow()
    report = workflow.run(date=date, top_n=top_n, min_score=min_score, resume=resume)
    return report.to_dict()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="选股工作流")
    parser.add_argument("--date", help="指定日期 (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, default=20, help="选取前N只股票")
    parser.add_argument("--min-score", type=float, default=60, help="最低评分")
    parser.add_argument("--resume", action="store_true", help="从断点恢复")
    
    args = parser.parse_args()
    
    result = run_selection(
        date=args.date,
        top_n=args.top_n,
        min_score=args.min_score,
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
