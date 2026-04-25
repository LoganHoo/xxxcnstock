#!/usr/bin/env python3
"""
工作流执行框架

提供以下功能：
- 依赖性检查
- 自动重试
- 断点续传
- 自动修复
- 发送报告
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from functools import wraps
import hashlib

from core.logger import setup_logger
from core.paths import get_data_path


class WorkflowStatus(Enum):
    """工作流状态"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class DependencyStatus(Enum):
    """依赖状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class DependencyCheck:
    """依赖检查结果"""
    name: str
    status: DependencyStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    auto_fixed: bool = False
    fix_attempts: int = 0


@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 2.0
    retryable_exceptions: tuple = (Exception,)


@dataclass
class Checkpoint:
    """断点信息"""
    workflow_id: str
    step_name: str
    step_index: int
    completed_items: List[str]
    failed_items: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'workflow_id': self.workflow_id,
            'step_name': self.step_name,
            'step_index': self.step_index,
            'completed_items': self.completed_items,
            'failed_items': self.failed_items,
            'metadata': self.metadata,
            'timestamp': self.timestamp
        }


@dataclass
class WorkflowReport:
    """工作流报告"""
    workflow_name: str
    workflow_id: str
    status: WorkflowStatus
    start_time: str
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    dependency_checks: List[DependencyCheck] = field(default_factory=list)
    steps_results: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'workflow_name': self.workflow_name,
            'workflow_id': self.workflow_id,
            'status': self.status.value,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'dependency_checks': [
                {
                    'name': d.name,
                    'status': d.status.value,
                    'message': d.message,
                    'auto_fixed': d.auto_fixed,
                    'fix_attempts': d.fix_attempts
                }
                for d in self.dependency_checks
            ],
            'steps_results': self.steps_results,
            'errors': self.errors,
            'warnings': self.warnings,
            'summary': self.summary
        }
    
    def to_markdown(self) -> str:
        """生成Markdown格式报告"""
        lines = [
            f"# {self.workflow_name} 执行报告",
            "",
            f"**工作流ID**: `{self.workflow_id}`",
            f"**状态**: {self.status.value.upper()}",
            f"**开始时间**: {self.start_time}",
        ]
        
        if self.end_time:
            lines.append(f"**结束时间**: {self.end_time}")
        
        lines.extend([
            f"**执行时长**: {self.duration_seconds:.2f}秒",
            "",
            "## 依赖检查",
            ""
        ])
        
        for dep in self.dependency_checks:
            icon = "✅" if dep.status == DependencyStatus.HEALTHY else "⚠️" if dep.status == DependencyStatus.DEGRADED else "❌"
            lines.append(f"{icon} **{dep.name}**: {dep.status.value}")
            lines.append(f"   {dep.message}")
            if dep.auto_fixed:
                lines.append(f"   🛠️ 自动修复成功（尝试{dep.fix_attempts}次）")
            lines.append("")
        
        if self.errors:
            lines.extend([
                "## 错误",
                ""
            ])
            for error in self.errors:
                lines.append(f"- ❌ {error}")
            lines.append("")
        
        if self.warnings:
            lines.extend([
                "## 警告",
                ""
            ])
            for warning in self.warnings:
                lines.append(f"- ⚠️ {warning}")
            lines.append("")
        
        if self.summary:
            lines.extend([
                "## 汇总",
                ""
            ])
            for key, value in self.summary.items():
                lines.append(f"- **{key}**: {value}")
            lines.append("")
        
        return "\n".join(lines)


class WorkflowExecutor(ABC):
    """工作流执行器基类"""
    
    def __init__(
        self,
        workflow_name: str,
        retry_config: Optional[RetryConfig] = None,
        enable_checkpoint: bool = True,
        enable_auto_fix: bool = True
    ):
        self.workflow_name = workflow_name
        self.workflow_id = self._generate_workflow_id()
        self.retry_config = retry_config or RetryConfig()
        self.enable_checkpoint = enable_checkpoint
        self.enable_auto_fix = enable_auto_fix
        
        self.logger = setup_logger(f"workflow.{workflow_name}")
        self.report = WorkflowReport(
            workflow_name=workflow_name,
            workflow_id=self.workflow_id,
            status=WorkflowStatus.PENDING,
            start_time=datetime.now().isoformat()
        )
        
        # 断点目录
        self.checkpoint_dir = get_data_path() / "checkpoints" / workflow_name
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # 报告目录
        self.report_dir = get_data_path() / "reports"
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def _generate_workflow_id(self) -> str:
        """生成工作流ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        random_suffix = hashlib.md5(str(time.time()).encode()).hexdigest()[:6]
        return f"{self.workflow_name}_{timestamp}_{random_suffix}"
    
    def _get_checkpoint_path(self) -> Path:
        """获取断点文件路径"""
        return self.checkpoint_dir / f"{self.workflow_id}.json"
    
    def save_checkpoint(self, checkpoint: Checkpoint):
        """保存断点"""
        if not self.enable_checkpoint:
            return
        
        checkpoint_path = self._get_checkpoint_path()
        with open(checkpoint_path, 'w', encoding='utf-8') as f:
            json.dump(checkpoint.to_dict(), f, ensure_ascii=False, indent=2)
        
        self.logger.debug(f"断点已保存: {checkpoint.step_name}")
    
    def load_checkpoint(self, workflow_id: Optional[str] = None) -> Optional[Checkpoint]:
        """加载断点"""
        if not self.enable_checkpoint:
            return None
        
        # 如果指定了workflow_id，加载该断点
        if workflow_id:
            checkpoint_path = self.checkpoint_dir / f"{workflow_id}.json"
        else:
            # 否则加载最新的断点
            checkpoints = sorted(self.checkpoint_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            if not checkpoints:
                return None
            checkpoint_path = checkpoints[0]
            self.workflow_id = checkpoint_path.stem
        
        if not checkpoint_path.exists():
            return None
        
        try:
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            checkpoint = Checkpoint(
                workflow_id=data['workflow_id'],
                step_name=data['step_name'],
                step_index=data['step_index'],
                completed_items=data.get('completed_items', []),
                failed_items=data.get('failed_items', []),
                metadata=data.get('metadata', {}),
                timestamp=data.get('timestamp', datetime.now().isoformat())
            )
            
            self.logger.info(f"断点已加载: {checkpoint.step_name}, 已完成{len(checkpoint.completed_items)}项")
            return checkpoint
        except Exception as e:
            self.logger.error(f"加载断点失败: {e}")
            return None
    
    def clear_checkpoint(self):
        """清除断点"""
        checkpoint_path = self._get_checkpoint_path()
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            self.logger.debug(f"断点已清除: {checkpoint_path}")
    
    @abstractmethod
    def check_dependencies(self) -> List[DependencyCheck]:
        """
        检查依赖
        
        Returns:
            依赖检查结果列表
        """
        pass
    
    @abstractmethod
    def auto_fix_dependency(self, dependency: DependencyCheck) -> bool:
        """
        自动修复依赖问题
        
        Args:
            dependency: 需要修复的依赖
            
        Returns:
            是否修复成功
        """
        pass
    
    @abstractmethod
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行工作流
        
        Args:
            **kwargs: 执行参数
            
        Returns:
            执行结果
        """
        pass
    
    def run_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        带重试的执行
        
        Args:
            func: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            函数返回值
        """
        last_exception = None
        delay = self.retry_config.retry_delay
        
        for attempt in range(self.retry_config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except self.retry_config.retryable_exceptions as e:
                last_exception = e
                if attempt < self.retry_config.max_retries:
                    self.logger.warning(
                        f"执行失败，{delay}秒后重试 ({attempt + 1}/{self.retry_config.max_retries}): {e}"
                    )
                    time.sleep(delay)
                    delay *= self.retry_config.backoff_factor
                else:
                    self.logger.error(f"重试次数用尽，最终失败: {e}")
        
        raise last_exception
    
    def run(self, resume: bool = False, **kwargs) -> WorkflowReport:
        """
        运行工作流
        
        Args:
            resume: 是否从断点恢复
            **kwargs: 执行参数
            
        Returns:
            工作流报告
        """
        start_time = time.time()
        self.report.status = WorkflowStatus.RUNNING
        
        try:
            # 1. 依赖检查
            self.logger.info("=" * 60)
            self.logger.info(f"开始执行工作流: {self.workflow_name}")
            self.logger.info(f"工作流ID: {self.workflow_id}")
            self.logger.info("=" * 60)
            
            self.logger.info("\n🔍 步骤1: 依赖检查")
            dependencies = self.check_dependencies()
            self.report.dependency_checks = dependencies
            
            unhealthy_deps = [d for d in dependencies if d.status == DependencyStatus.UNHEALTHY]
            degraded_deps = [d for d in dependencies if d.status == DependencyStatus.DEGRADED]
            
            # 尝试自动修复
            if self.enable_auto_fix:
                for dep in unhealthy_deps + degraded_deps:
                    if not dep.auto_fixed:
                        self.logger.info(f"🛠️ 尝试自动修复: {dep.name}")
                        fixed = self.auto_fix_dependency(dep)
                        if fixed:
                            dep.auto_fixed = True
                            dep.fix_attempts += 1
                            dep.status = DependencyStatus.HEALTHY
                            dep.message = f"自动修复成功: {dep.message}"
                            self.logger.info(f"✅ 自动修复成功: {dep.name}")
            
            # 检查修复后的状态
            still_unhealthy = [d for d in dependencies if d.status == DependencyStatus.UNHEALTHY]
            if still_unhealthy:
                error_msg = f"依赖检查失败: {', '.join([d.name for d in still_unhealthy])}"
                self.logger.error(error_msg)
                self.report.errors.append(error_msg)
                self.report.status = WorkflowStatus.FAILED
                return self._finalize_report(start_time)
            
            # 2. 加载断点（如果需要恢复）
            checkpoint = None
            if resume:
                checkpoint = self.load_checkpoint()
                if checkpoint:
                    self.logger.info(f"\n📍 从断点恢复: {checkpoint.step_name}")
                    kwargs['checkpoint'] = checkpoint
            
            # 3. 执行工作流
            self.logger.info("\n🚀 步骤2: 执行工作流")
            result = self.execute(**kwargs)
            
            # 4. 处理结果
            if result.get('status') == 'completed':
                self.report.status = WorkflowStatus.COMPLETED
                self.logger.info("\n✅ 工作流执行成功")
            elif result.get('status') == 'partial':
                self.report.status = WorkflowStatus.PARTIAL
                self.logger.warning("\n⚠️ 工作流部分完成")
            else:
                self.report.status = WorkflowStatus.FAILED
                self.logger.error("\n❌ 工作流执行失败")
            
            if 'errors' in result:
                self.report.errors.extend(result['errors'])
            if 'warnings' in result:
                self.report.warnings.extend(result['warnings'])
            if 'summary' in result:
                self.report.summary = result['summary']
            
            # 5. 执行成功，清除断点
            if self.report.status in [WorkflowStatus.COMPLETED, WorkflowStatus.PARTIAL]:
                self.clear_checkpoint()
            
        except Exception as e:
            self.logger.error(f"\n❌ 工作流执行异常: {e}")
            self.logger.error(traceback.format_exc())
            self.report.status = WorkflowStatus.FAILED
            self.report.errors.append(str(e))
        
        return self._finalize_report(start_time)
    
    def _finalize_report(self, start_time: float) -> WorkflowReport:
        """完成报告"""
        self.report.end_time = datetime.now().isoformat()
        self.report.duration_seconds = time.time() - start_time
        
        # 保存报告
        self._save_report()
        
        # 发送报告
        self._send_report()
        
        return self.report
    
    def _save_report(self):
        """保存报告到文件"""
        report_path = self.report_dir / f"{self.workflow_id}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.report.to_dict(), f, ensure_ascii=False, indent=2)
        
        # 同时保存Markdown版本
        md_path = self.report_dir / f"{self.workflow_id}.md"
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(self.report.to_markdown())
        
        self.logger.info(f"\n📄 报告已保存: {report_path}")
    
    def _send_report(self):
        """发送报告"""
        # 这里可以实现多种通知方式：
        # - 邮件
        # - 企业微信/钉钉
        # - Slack
        # - Webhook
        
        status_icon = {
            WorkflowStatus.COMPLETED: "✅",
            WorkflowStatus.PARTIAL: "⚠️",
            WorkflowStatus.FAILED: "❌",
            WorkflowStatus.PAUSED: "⏸️"
        }.get(self.report.status, "❓")
        
        summary = f"""
{status_icon} {self.workflow_name} 执行完成

状态: {self.report.status.value}
时长: {self.report.duration_seconds:.2f}秒
错误: {len(self.report.errors)}个
警告: {len(self.report.warnings)}个

报告文件: {self.report_dir}/{self.workflow_id}.md
"""
        
        self.logger.info(f"\n📧 报告摘要:\n{summary}")
        
        # TODO: 实现具体的通知发送逻辑
        # 例如：发送邮件、Webhook等
        self._send_notification(summary)
    
    def _send_notification(self, message: str):
        """
        发送通知（子类可覆盖）
        
        Args:
            message: 通知消息
        """
        # 默认实现：仅记录日志
        # 子类可以覆盖此方法实现具体的通知逻辑
        pass


def workflow_step(step_name: str, retryable: bool = True):
    """
    工作流步骤装饰器
    
    Args:
        step_name: 步骤名称
        retryable: 是否可重试
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.logger.info(f"\n📌 执行步骤: {step_name}")
            step_start = time.time()
            
            try:
                if retryable and hasattr(self, 'run_with_retry'):
                    result = self.run_with_retry(func, self, *args, **kwargs)
                else:
                    result = func(self, *args, **kwargs)
                
                step_duration = time.time() - step_start
                self.report.steps_results.append({
                    'step_name': step_name,
                    'status': 'success',
                    'duration_seconds': step_duration,
                    'result': result
                })
                
                self.logger.info(f"✅ 步骤完成: {step_name} ({step_duration:.2f}s)")
                return result
                
            except Exception as e:
                step_duration = time.time() - step_start
                self.report.steps_results.append({
                    'step_name': step_name,
                    'status': 'failed',
                    'duration_seconds': step_duration,
                    'error': str(e)
                })
                
                self.logger.error(f"❌ 步骤失败: {step_name} - {e}")
                raise
        
        return wrapper
    return decorator
