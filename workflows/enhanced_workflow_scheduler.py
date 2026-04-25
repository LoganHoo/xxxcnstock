#!/usr/bin/env python3
"""
增强版工作流调度器

统一管理所有工作流的执行：
- 数据采集
- 评分计算
- 选股

提供：
- 顺序执行
- 断点续传
- 依赖管理
- 统一报告
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from core.logger import setup_logger
from core.paths import get_data_path
from workflows.enhanced_data_collection_workflow import EnhancedDataCollectionWorkflow
from workflows.enhanced_scoring_workflow import EnhancedScoringWorkflow
from workflows.enhanced_selection_workflow import EnhancedSelectionWorkflow


@dataclass
class PipelineReport:
    """流水线报告"""
    pipeline_name: str
    start_time: str
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    workflows: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "pending"
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'pipeline_name': self.pipeline_name,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'status': self.status,
            'workflows': self.workflows,
            'summary': self.summary
        }
    
    def to_markdown(self) -> str:
        """生成Markdown报告"""
        lines = [
            f"# {self.pipeline_name} 流水线报告",
            "",
            f"**开始时间**: {self.start_time}",
        ]
        
        if self.end_time:
            lines.append(f"**结束时间**: {self.end_time}")
        
        lines.extend([
            f"**执行时长**: {self.duration_seconds:.2f}秒",
            f"**最终状态**: {self.status.upper()}",
            "",
            "## 工作流执行详情",
            ""
        ])
        
        for wf in self.workflows:
            icon = "✅" if wf['status'] == 'completed' else "⚠️" if wf['status'] == 'partial' else "❌"
            lines.append(f"### {icon} {wf['name']}")
            lines.append(f"- **状态**: {wf['status']}")
            lines.append(f"- **时长**: {wf.get('duration', 0):.2f}秒")
            
            if wf.get('errors'):
                lines.append(f"- **错误**: {len(wf['errors'])}个")
            
            if wf.get('summary'):
                lines.append("- **汇总**:")
                for key, value in wf['summary'].items():
                    lines.append(f"  - {key}: {value}")
            
            lines.append("")
        
        if self.summary:
            lines.extend([
                "## 总体汇总",
                ""
            ])
            for key, value in self.summary.items():
                lines.append(f"- **{key}**: {value}")
            lines.append("")
        
        return "\n".join(lines)


class EnhancedWorkflowScheduler:
    """增强版工作流调度器"""
    
    def __init__(self):
        self.logger = setup_logger("workflow_scheduler")
        self.data_dir = get_data_path()
        self.report_dir = self.data_dir / "reports"
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        # 工作流实例
        self.collection_workflow = EnhancedDataCollectionWorkflow()
        self.scoring_workflow = EnhancedScoringWorkflow()
        self.selection_workflow = EnhancedSelectionWorkflow()
    
    def run_full_pipeline(
        self,
        date: Optional[str] = None,
        skip_collection: bool = False,
        skip_scoring: bool = False,
        skip_selection: bool = False,
        resume: bool = False,
        top_n: int = 20,
        min_score: float = 60
    ) -> PipelineReport:
        """
        运行完整流水线
        
        Args:
            date: 指定日期
            skip_collection: 跳过数据采集
            skip_scoring: 跳过评分计算
            skip_selection: 跳过选股
            resume: 从断点恢复
            top_n: 选取前N只股票
            min_score: 最低评分
            
        Returns:
            流水线报告
        """
        start_time = time.time()
        
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info("=" * 70)
        self.logger.info(f"🚀 启动完整流水线 - {date}")
        self.logger.info("=" * 70)
        
        report = PipelineReport(
            pipeline_name="完整流水线",
            start_time=datetime.now().isoformat()
        )
        
        all_success = True
        
        # 1. 数据采集
        if not skip_collection:
            self.logger.info("\n" + "=" * 70)
            self.logger.info("📥 阶段1: 数据采集")
            self.logger.info("=" * 70)
            
            try:
                wf_report = self.collection_workflow.run(date=date, resume=resume)
                
                report.workflows.append({
                    'name': '数据采集',
                    'status': wf_report.status.value,
                    'duration': wf_report.duration_seconds,
                    'errors': wf_report.errors,
                    'warnings': wf_report.warnings,
                    'summary': wf_report.summary
                })
                
                if wf_report.status.value in ['failed']:
                    all_success = False
                    self.logger.error("❌ 数据采集失败，停止流水线")
                    report.status = 'failed'
                    return self._finalize_pipeline_report(report, start_time)
                    
            except Exception as e:
                all_success = False
                self.logger.error(f"❌ 数据采集异常: {e}")
                report.workflows.append({
                    'name': '数据采集',
                    'status': 'failed',
                    'duration': 0,
                    'errors': [str(e)],
                    'summary': {}
                })
                report.status = 'failed'
                return self._finalize_pipeline_report(report, start_time)
        
        # 2. 评分计算
        if not skip_scoring:
            self.logger.info("\n" + "=" * 70)
            self.logger.info("🧮 阶段2: 评分计算")
            self.logger.info("=" * 70)
            
            try:
                wf_report = self.scoring_workflow.run(date=date, resume=resume)
                
                report.workflows.append({
                    'name': '评分计算',
                    'status': wf_report.status.value,
                    'duration': wf_report.duration_seconds,
                    'errors': wf_report.errors,
                    'warnings': wf_report.warnings,
                    'summary': wf_report.summary
                })
                
                if wf_report.status.value == 'failed':
                    all_success = False
                    self.logger.error("❌ 评分计算失败，停止流水线")
                    report.status = 'failed'
                    return self._finalize_pipeline_report(report, start_time)
                    
            except Exception as e:
                all_success = False
                self.logger.error(f"❌ 评分计算异常: {e}")
                report.workflows.append({
                    'name': '评分计算',
                    'status': 'failed',
                    'duration': 0,
                    'errors': [str(e)],
                    'summary': {}
                })
                report.status = 'failed'
                return self._finalize_pipeline_report(report, start_time)
        
        # 3. 选股
        if not skip_selection:
            self.logger.info("\n" + "=" * 70)
            self.logger.info("🎯 阶段3: 选股")
            self.logger.info("=" * 70)
            
            try:
                wf_report = self.selection_workflow.run(
                    date=date,
                    top_n=top_n,
                    min_score=min_score,
                    resume=resume
                )
                
                report.workflows.append({
                    'name': '选股',
                    'status': wf_report.status.value,
                    'duration': wf_report.duration_seconds,
                    'errors': wf_report.errors,
                    'warnings': wf_report.warnings,
                    'summary': wf_report.summary
                })
                
                if wf_report.status.value == 'failed':
                    all_success = False
                    
            except Exception as e:
                all_success = False
                self.logger.error(f"❌ 选股异常: {e}")
                report.workflows.append({
                    'name': '选股',
                    'status': 'failed',
                    'duration': 0,
                    'errors': [str(e)],
                    'summary': {}
                })
        
        # 确定最终状态
        if all_success:
            report.status = 'completed'
        else:
            report.status = 'partial'
        
        return self._finalize_pipeline_report(report, start_time)
    
    def _finalize_pipeline_report(self, report: PipelineReport, start_time: float) -> PipelineReport:
        """完成流水线报告"""
        report.end_time = datetime.now().isoformat()
        report.duration_seconds = time.time() - start_time
        
        # 生成汇总
        report.summary = {
            '总工作流数': len(report.workflows),
            '成功': len([w for w in report.workflows if w['status'] == 'completed']),
            '部分成功': len([w for w in report.workflows if w['status'] == 'partial']),
            '失败': len([w for w in report.workflows if w['status'] == 'failed']),
        }
        
        # 保存报告
        self._save_pipeline_report(report)
        
        # 发送通知
        self._send_pipeline_notification(report)
        
        return report
    
    def _save_pipeline_report(self, report: PipelineReport):
        """保存流水线报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # JSON格式
        json_path = self.report_dir / f"pipeline_report_{timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        
        # Markdown格式
        md_path = self.report_dir / f"pipeline_report_{timestamp}.md"
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(report.to_markdown())
        
        self.logger.info(f"\n📄 流水线报告已保存:")
        self.logger.info(f"   JSON: {json_path}")
        self.logger.info(f"   Markdown: {md_path}")
    
    def _send_pipeline_notification(self, report: PipelineReport):
        """发送流水线通知"""
        status_icon = {
            'completed': '✅',
            'partial': '⚠️',
            'failed': '❌'
        }.get(report.status, '❓')
        
        summary = f"""
{status_icon} 流水线执行完成

状态: {report.status.upper()}
时长: {report.duration_seconds:.2f}秒
工作流: {len(report.workflows)}个

执行详情:
"""
        for wf in report.workflows:
            icon = "✅" if wf['status'] == 'completed' else "⚠️" if wf['status'] == 'partial' else "❌"
            summary += f"  {icon} {wf['name']}: {wf['status']} ({wf.get('duration', 0):.2f}s)\n"
        
        self.logger.info(f"\n📧 流水线通知:\n{summary}")
        
        # TODO: 实现具体的通知发送（邮件、Webhook等）


def run_pipeline(
    date: Optional[str] = None,
    skip_collection: bool = False,
    skip_scoring: bool = False,
    skip_selection: bool = False,
    resume: bool = False,
    top_n: int = 20,
    min_score: float = 60
) -> Dict[str, Any]:
    """
    运行完整流水线
    
    Args:
        date: 指定日期
        skip_collection: 跳过数据采集
        skip_scoring: 跳过评分计算
        skip_selection: 跳过选股
        resume: 从断点恢复
        top_n: 选取前N只股票
        min_score: 最低评分
        
    Returns:
        执行结果
    """
    scheduler = EnhancedWorkflowScheduler()
    report = scheduler.run_full_pipeline(
        date=date,
        skip_collection=skip_collection,
        skip_scoring=skip_scoring,
        skip_selection=skip_selection,
        resume=resume,
        top_n=top_n,
        min_score=min_score
    )
    return report.to_dict()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="工作流调度器")
    parser.add_argument("--date", help="指定日期 (YYYY-MM-DD)")
    parser.add_argument("--skip-collection", action="store_true", help="跳过数据采集")
    parser.add_argument("--skip-scoring", action="store_true", help="跳过评分计算")
    parser.add_argument("--skip-selection", action="store_true", help="跳过选股")
    parser.add_argument("--resume", action="store_true", help="从断点恢复")
    parser.add_argument("--top-n", type=int, default=20, help="选取前N只股票")
    parser.add_argument("--min-score", type=float, default=60, help="最低评分")
    
    args = parser.parse_args()
    
    result = run_pipeline(
        date=args.date,
        skip_collection=args.skip_collection,
        skip_scoring=args.skip_scoring,
        skip_selection=args.skip_selection,
        resume=args.resume,
        top_n=args.top_n,
        min_score=args.min_score
    )
    
    print("\n" + "=" * 70)
    print("流水线执行结果")
    print("=" * 70)
    print(f"状态: {result['status']}")
    print(f"时长: {result['duration_seconds']:.2f}秒")
    print(f"\n工作流执行详情:")
    for wf in result['workflows']:
        print(f"  - {wf['name']}: {wf['status']} ({wf.get('duration', 0):.2f}s)")
    
    print(f"\n汇总:")
    for key, value in result['summary'].items():
        print(f"  {key}: {value}")
