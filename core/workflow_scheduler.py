#!/usr/bin/env python3
"""
工作流调度器 - 支持优先级和依赖管理

提供工作流的注册、调度、执行和重试功能
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import heapq
import uuid
import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.logger import setup_logger
from core.workflow_state_db import WorkflowStateDB, WorkflowExecutionStatus


class WorkflowPriority(Enum):
    """工作流优先级"""
    CRITICAL = 0   # 关键任务（如数据质检）
    HIGH = 1       # 高优先级（如选股）
    NORMAL = 2     # 普通优先级（如报表生成）
    LOW = 3        # 低优先级（如数据归档）


@dataclass
class WorkflowTask:
    """工作流任务"""
    name: str
    priority: WorkflowPriority
    dependencies: List[str] = field(default_factory=list)
    max_retries: int = 3
    retry_delay: int = 60  # 秒
    timeout: int = 3600    # 秒
    executor_factory: Optional[Callable] = None
    params: Dict[str, Any] = field(default_factory=dict)
    
    # 运行时状态
    execution_id: Optional[str] = None
    status: str = "pending"
    retry_count: int = 0
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: str = ""
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()


class WorkflowScheduler:
    """工作流调度器"""
    
    def __init__(self, db_path: Optional[str] = None, max_workers: int = 4):
        """
        初始化调度器
        
        Args:
            db_path: 状态数据库路径
            max_workers: 最大并发工作线程数
        """
        self.state_db = WorkflowStateDB(db_path)
        self.logger = setup_logger("workflow_scheduler")
        
        # 工作流注册表
        self._workflows: Dict[str, WorkflowTask] = {}
        self._lock = Lock()
        
        # 执行历史
        self._execution_history: Dict[str, List[str]] = {}  # workflow_name -> execution_ids
        
        # 线程池
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._max_workers = max_workers
        
        self.logger.info(f"工作流调度器初始化完成，最大并发数: {max_workers}")
    
    def register_workflow(
        self,
        name: str,
        priority: WorkflowPriority = WorkflowPriority.NORMAL,
        dependencies: Optional[List[str]] = None,
        max_retries: int = 3,
        retry_delay: int = 60,
        timeout: int = 3600,
        executor_factory: Optional[Callable] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> WorkflowTask:
        """
        注册工作流
        
        Args:
            name: 工作流名称
            priority: 优先级
            dependencies: 依赖的工作流名称列表
            max_retries: 最大重试次数
            retry_delay: 重试间隔（秒）
            timeout: 超时时间（秒）
            executor_factory: 执行器工厂函数
            params: 默认参数
            
        Returns:
            注册的工作流任务
        """
        with self._lock:
            if name in self._workflows:
                self.logger.warning(f"工作流 '{name}' 已存在，将被覆盖")
            
            task = WorkflowTask(
                name=name,
                priority=priority,
                dependencies=dependencies or [],
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout,
                executor_factory=executor_factory,
                params=params or {}
            )
            
            self._workflows[name] = task
            self.logger.info(f"工作流 '{name}' 注册成功，优先级: {priority.name}")
            return task
    
    def unregister_workflow(self, name: str) -> bool:
        """
        注销工作流
        
        Args:
            name: 工作流名称
            
        Returns:
            是否成功注销
        """
        with self._lock:
            if name in self._workflows:
                del self._workflows[name]
                self.logger.info(f"工作流 '{name}' 已注销")
                return True
            return False
    
    def get_workflow(self, name: str) -> Optional[WorkflowTask]:
        """
        获取工作流定义
        
        Args:
            name: 工作流名称
            
        Returns:
            工作流任务定义
        """
        return self._workflows.get(name)
    
    def list_workflows(self) -> List[WorkflowTask]:
        """
        列出所有注册的工作流
        
        Returns:
            工作流列表
        """
        return list(self._workflows.values())
    
    def check_dependencies(self, workflow_name: str) -> Dict[str, Any]:
        """
        检查工作流依赖状态
        
        Args:
            workflow_name: 工作流名称
            
        Returns:
            {
                "ready": bool,
                "missing": List[str],  # 未注册的依赖
                "failed": List[str],   # 执行失败的依赖
                "pending": List[str],  # 执行中的依赖
                "not_completed": List[str]  # 未完成的依赖
            }
        """
        task = self._workflows.get(workflow_name)
        if not task:
            return {
                "ready": False,
                "missing": [workflow_name],
                "failed": [],
                "pending": [],
                "not_completed": []
            }
        
        result = {
            "ready": True,
            "missing": [],
            "failed": [],
            "pending": [],
            "not_completed": []
        }
        
        for dep_name in task.dependencies:
            # 检查依赖是否注册
            if dep_name not in self._workflows:
                result["missing"].append(dep_name)
                result["ready"] = False
                continue
            
            # 检查依赖的最新执行状态
            latest = self.state_db.get_latest_execution(dep_name)
            if not latest:
                # 从未执行过
                result["not_completed"].append(dep_name)
                result["ready"] = False
            elif latest["status"] == WorkflowExecutionStatus.FAILED.value:
                result["failed"].append(dep_name)
                result["ready"] = False
            elif latest["status"] in [WorkflowExecutionStatus.PENDING.value, WorkflowExecutionStatus.RUNNING.value]:
                result["pending"].append(dep_name)
                result["ready"] = False
            elif latest["status"] != WorkflowExecutionStatus.SUCCESS.value:
                result["not_completed"].append(dep_name)
                result["ready"] = False
        
        return result
    
    def schedule_by_priority(self, available_workflows: Optional[List[str]] = None) -> List[str]:
        """
        按优先级调度工作流
        
        Args:
            available_workflows: 可选的工作流列表（None表示所有）
            
        Returns:
            按优先级排序的工作流名称列表
        """
        candidates = []
        
        workflow_names = available_workflows or list(self._workflows.keys())
        
        for name in workflow_names:
            task = self._workflows.get(name)
            if not task:
                continue
            
            # 检查依赖是否满足
            dep_status = self.check_dependencies(name)
            if not dep_status["ready"]:
                self.logger.debug(f"工作流 '{name}' 依赖未满足，跳过调度")
                continue
            
            # 检查是否已在运行
            latest = self.state_db.get_latest_execution(name)
            if latest and latest["status"] == WorkflowExecutionStatus.RUNNING.value:
                self.logger.debug(f"工作流 '{name}' 正在运行，跳过调度")
                continue
            
            # 加入候选队列 (priority, created_at, name)
            candidates.append((task.priority.value, task.created_at or "", name))
        
        # 按优先级排序
        candidates.sort(key=lambda x: (x[0], x[1]))
        
        scheduled = [name for _, _, name in candidates]
        self.logger.info(f"已调度 {len(scheduled)} 个工作流: {scheduled}")
        return scheduled
    
    def execute_workflow(
        self,
        workflow_name: str,
        params: Optional[Dict[str, Any]] = None,
        wait: bool = True
    ) -> Dict[str, Any]:
        """
        执行工作流
        
        Args:
            workflow_name: 工作流名称
            params: 执行参数（覆盖默认参数）
            wait: 是否等待执行完成
            
        Returns:
            执行结果
        """
        task = self._workflows.get(workflow_name)
        if not task:
            return {
                "success": False,
                "error": f"工作流 '{workflow_name}' 未注册"
            }
        
        # 检查依赖
        dep_status = self.check_dependencies(workflow_name)
        if not dep_status["ready"]:
            return {
                "success": False,
                "error": f"依赖未满足: {dep_status}",
                "dependencies": dep_status
            }
        
        # 生成执行ID
        execution_id = f"{workflow_name}_{uuid.uuid4().hex[:8]}_{int(time.time())}"
        task.execution_id = execution_id
        
        # 合并参数
        merged_params = {**task.params, **(params or {})}
        
        # 创建执行记录
        self.state_db.create_execution(
            workflow_name=workflow_name,
            execution_id=execution_id,
            params=merged_params
        )
        self.state_db.update_workflow_status(
            execution_id=execution_id,
            status=WorkflowExecutionStatus.RUNNING.value
        )
        
        task.status = "running"
        task.started_at = datetime.now().isoformat()
        
        self.logger.info(f"开始执行工作流 '{workflow_name}'，执行ID: {execution_id}")
        
        # 执行
        try:
            if task.executor_factory:
                executor = task.executor_factory()
                result = executor.run(**merged_params)
            else:
                # 默认执行逻辑
                result = self._default_execute(workflow_name, merged_params)
            
            # 更新成功状态
            task.status = "success"
            task.completed_at = datetime.now().isoformat()
            
            self.state_db.update_workflow_status(
                execution_id=execution_id,
                status=WorkflowExecutionStatus.SUCCESS.value,
                result=result
            )
            
            # 记录执行历史
            if workflow_name not in self._execution_history:
                self._execution_history[workflow_name] = []
            self._execution_history[workflow_name].append(execution_id)
            
            self.logger.info(f"工作流 '{workflow_name}' 执行成功")
            return {
                "success": True,
                "execution_id": execution_id,
                "result": result
            }
            
        except Exception as e:
            error_msg = str(e)
            task.status = "failed"
            task.error_message = error_msg
            task.retry_count += 1
            
            self.state_db.update_workflow_status(
                execution_id=execution_id,
                status=WorkflowExecutionStatus.FAILED.value,
                error_message=error_msg
            )
            
            self.logger.error(f"工作流 '{workflow_name}' 执行失败: {error_msg}")
            return {
                "success": False,
                "execution_id": execution_id,
                "error": error_msg
            }
    
    def _default_execute(self, workflow_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        默认执行逻辑
        
        Args:
            workflow_name: 工作流名称
            params: 执行参数
            
        Returns:
            执行结果
        """
        self.logger.info(f"执行工作流 '{workflow_name}' 的默认逻辑")
        return {
            "workflow": workflow_name,
            "params": params,
            "message": "默认执行完成"
        }
    
    def retry_failed_workflow(
        self,
        workflow_name: str,
        execution_id: Optional[str] = None,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        重试失败的工作流
        
        Args:
            workflow_name: 工作流名称
            execution_id: 指定执行ID（None表示最新失败）
            force: 是否强制重试（忽略重试次数限制）
            
        Returns:
            重试结果
        """
        task = self._workflows.get(workflow_name)
        if not task:
            return {
                "success": False,
                "error": f"工作流 '{workflow_name}' 未注册"
            }
        
        # 获取执行记录
        if execution_id:
            status = self.state_db.get_workflow_status(execution_id)
        else:
            executions = self.state_db.list_executions(workflow_name)
            failed_execs = [e for e in executions if e["status"] == WorkflowExecutionStatus.FAILED.value]
            if not failed_execs:
                return {
                    "success": False,
                    "error": f"工作流 '{workflow_name}' 没有失败的执行记录"
                }
            status = failed_execs[-1]
            execution_id = status["execution_id"]
        
        if not status:
            return {
                "success": False,
                "error": "未找到执行记录"
            }
        
        # 检查重试次数
        if not force and status["retry_count"] >= task.max_retries:
            return {
                "success": False,
                "error": f"已达到最大重试次数 ({task.max_retries})"
            }
        
        # 等待重试延迟
        if task.retry_delay > 0:
            self.logger.info(f"等待 {task.retry_delay} 秒后重试...")
            time.sleep(task.retry_delay)
        
        # 更新重试状态
        self.state_db.increment_retry_count(execution_id)
        self.state_db.update_workflow_status(
            execution_id=execution_id,
            status=WorkflowExecutionStatus.RETRY.value
        )
        
        # 重新执行
        self.logger.info(f"重试工作流 '{workflow_name}'，执行ID: {execution_id}")
        return self.execute_workflow(
            workflow_name=workflow_name,
            params=status.get("params", {})
        )
    
    def execute_batch(
        self,
        workflow_names: Optional[List[str]] = None,
        parallel: bool = True
    ) -> Dict[str, Any]:
        """
        批量执行工作流
        
        Args:
            workflow_names: 工作流名称列表（None表示所有可调度的工作流）
            parallel: 是否并行执行
            
        Returns:
            批量执行结果
        """
        if workflow_names is None:
            workflow_names = self.schedule_by_priority()
        
        if not workflow_names:
            return {
                "success": True,
                "message": "没有可执行的工作流",
                "results": {}
            }
        
        results = {}
        
        if parallel and len(workflow_names) > 1:
            # 并行执行
            self.logger.info(f"并行执行 {len(workflow_names)} 个工作流")
            
            futures = {}
            for name in workflow_names:
                future = self._executor.submit(self.execute_workflow, name, None, False)
                futures[future] = name
            
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    results[name] = {
                        "success": False,
                        "error": str(e)
                    }
        else:
            # 串行执行
            self.logger.info(f"串行执行 {len(workflow_names)} 个工作流")
            for name in workflow_names:
                results[name] = self.execute_workflow(name)
        
        success_count = sum(1 for r in results.values() if r.get("success"))
        
        return {
            "success": success_count == len(workflow_names),
            "total": len(workflow_names),
            "success_count": success_count,
            "failed_count": len(workflow_names) - success_count,
            "results": results
        }
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """
        获取调度器状态
        
        Returns:
            调度器状态信息
        """
        return {
            "registered_workflows": len(self._workflows),
            "max_workers": self._max_workers,
            "workflows": [
                {
                    "name": name,
                    "priority": task.priority.name,
                    "status": task.status,
                    "dependencies": task.dependencies,
                    "retry_count": task.retry_count
                }
                for name, task in self._workflows.items()
            ]
        }
    
    def shutdown(self):
        """关闭调度器"""
        self._executor.shutdown(wait=True)
        self.logger.info("工作流调度器已关闭")


if __name__ == "__main__":
    # 简单测试
    scheduler = WorkflowScheduler()
    
    # 注册测试工作流
    scheduler.register_workflow(
        name="test_workflow",
        priority=WorkflowPriority.HIGH,
        max_retries=2
    )
    
    # 查看状态
    status = scheduler.get_scheduler_status()
    print(f"调度器状态: {status}")
