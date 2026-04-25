#!/usr/bin/env python3
"""
工作流调度器测试
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import unittest
import tempfile
import shutil
from pathlib import Path
from time import sleep

from core.workflow_scheduler import (
    WorkflowScheduler,
    WorkflowTask,
    WorkflowPriority
)
from core.workflow_state_db import WorkflowExecutionStatus


class MockExecutor:
    """模拟执行器"""
    
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.run_count = 0
    
    def run(self, **kwargs):
        self.run_count += 1
        if self.should_fail:
            raise Exception("模拟执行失败")
        return {"status": "success", "params": kwargs}


class TestWorkflowScheduler(unittest.TestCase):
    """测试工作流调度器"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test_scheduler.db"
        self.scheduler = WorkflowScheduler(db_path=self.db_path, max_workers=2)
    
    def tearDown(self):
        """测试后清理"""
        self.scheduler.shutdown()
        shutil.rmtree(self.temp_dir)
    
    def test_register_workflow(self):
        """测试注册工作流"""
        task = self.scheduler.register_workflow(
            name="test_workflow",
            priority=WorkflowPriority.HIGH,
            dependencies=["dep1", "dep2"],
            max_retries=3,
            params={"key": "value"}
        )
        
        self.assertIsNotNone(task)
        self.assertEqual(task.name, "test_workflow")
        self.assertEqual(task.priority, WorkflowPriority.HIGH)
        self.assertEqual(task.dependencies, ["dep1", "dep2"])
        self.assertEqual(task.max_retries, 3)
        self.assertEqual(task.params["key"], "value")
        
        # 验证可以获取
        retrieved = self.scheduler.get_workflow("test_workflow")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "test_workflow")
    
    def test_unregister_workflow(self):
        """测试注销工作流"""
        self.scheduler.register_workflow(name="test_workflow")
        
        # 注销
        result = self.scheduler.unregister_workflow("test_workflow")
        self.assertTrue(result)
        
        # 验证已删除
        retrieved = self.scheduler.get_workflow("test_workflow")
        self.assertIsNone(retrieved)
        
        # 注销不存在的工作流
        result = self.scheduler.unregister_workflow("nonexistent")
        self.assertFalse(result)
    
    def test_list_workflows(self):
        """测试列出工作流"""
        self.scheduler.register_workflow(name="wf1", priority=WorkflowPriority.HIGH)
        self.scheduler.register_workflow(name="wf2", priority=WorkflowPriority.NORMAL)
        self.scheduler.register_workflow(name="wf3", priority=WorkflowPriority.LOW)
        
        workflows = self.scheduler.list_workflows()
        self.assertEqual(len(workflows), 3)
        
        names = [w.name for w in workflows]
        self.assertIn("wf1", names)
        self.assertIn("wf2", names)
        self.assertIn("wf3", names)
    
    def test_check_dependencies_no_deps(self):
        """测试检查依赖 - 无依赖"""
        self.scheduler.register_workflow(name="test_workflow")
        
        status = self.scheduler.check_dependencies("test_workflow")
        self.assertTrue(status["ready"])
        self.assertEqual(status["missing"], [])
        self.assertEqual(status["failed"], [])
    
    def test_check_dependencies_missing(self):
        """测试检查依赖 - 依赖未注册"""
        self.scheduler.register_workflow(
            name="test_workflow",
            dependencies=["missing_dep"]
        )
        
        status = self.scheduler.check_dependencies("test_workflow")
        self.assertFalse(status["ready"])
        self.assertIn("missing_dep", status["missing"])
    
    def test_check_dependencies_not_completed(self):
        """测试检查依赖 - 依赖未完成"""
        self.scheduler.register_workflow(name="dep_workflow")
        self.scheduler.register_workflow(
            name="test_workflow",
            dependencies=["dep_workflow"]
        )
        
        # 依赖从未执行
        status = self.scheduler.check_dependencies("test_workflow")
        self.assertFalse(status["ready"])
        self.assertIn("dep_workflow", status["not_completed"])
    
    def test_check_dependencies_success(self):
        """测试检查依赖 - 依赖成功"""
        # 注册并执行依赖工作流
        self.scheduler.register_workflow(name="dep_workflow")
        result = self.scheduler.execute_workflow("dep_workflow")
        self.assertTrue(result["success"])
        
        # 注册主工作流
        self.scheduler.register_workflow(
            name="test_workflow",
            dependencies=["dep_workflow"]
        )
        
        # 检查依赖
        status = self.scheduler.check_dependencies("test_workflow")
        self.assertTrue(status["ready"])
    
    def test_schedule_by_priority(self):
        """测试按优先级调度"""
        # 注册不同优先级的工作流
        self.scheduler.register_workflow(name="wf_low", priority=WorkflowPriority.LOW)
        self.scheduler.register_workflow(name="wf_high", priority=WorkflowPriority.HIGH)
        self.scheduler.register_workflow(name="wf_normal", priority=WorkflowPriority.NORMAL)
        self.scheduler.register_workflow(name="wf_critical", priority=WorkflowPriority.CRITICAL)
        
        # 调度
        scheduled = self.scheduler.schedule_by_priority()
        
        # 验证顺序（按优先级）
        self.assertEqual(scheduled[0], "wf_critical")
        self.assertEqual(scheduled[1], "wf_high")
        self.assertEqual(scheduled[2], "wf_normal")
        self.assertEqual(scheduled[3], "wf_low")
    
    def test_execute_workflow_success(self):
        """测试执行工作流 - 成功"""
        mock_executor = MockExecutor(should_fail=False)
        
        self.scheduler.register_workflow(
            name="test_workflow",
            executor_factory=lambda: mock_executor,
            params={"default": "value"}
        )
        
        result = self.scheduler.execute_workflow(
            "test_workflow",
            params={"extra": "param"}
        )
        
        self.assertTrue(result["success"])
        self.assertIn("execution_id", result)
        self.assertEqual(mock_executor.run_count, 1)
    
    def test_execute_workflow_failure(self):
        """测试执行工作流 - 失败"""
        mock_executor = MockExecutor(should_fail=True)
        
        self.scheduler.register_workflow(
            name="test_workflow",
            executor_factory=lambda: mock_executor,
            max_retries=0
        )
        
        result = self.scheduler.execute_workflow("test_workflow")
        
        self.assertFalse(result["success"])
        self.assertIn("error", result)
        self.assertEqual(mock_executor.run_count, 1)
    
    def test_execute_workflow_not_found(self):
        """测试执行工作流 - 不存在"""
        result = self.scheduler.execute_workflow("nonexistent")
        
        self.assertFalse(result["success"])
        self.assertIn("未注册", result["error"])
    
    def test_execute_workflow_deps_not_ready(self):
        """测试执行工作流 - 依赖未满足"""
        self.scheduler.register_workflow(
            name="test_workflow",
            dependencies=["missing_dep"]
        )
        
        result = self.scheduler.execute_workflow("test_workflow")
        
        self.assertFalse(result["success"])
        self.assertIn("依赖未满足", result["error"])
    
    def test_retry_failed_workflow(self):
        """测试重试失败的工作流"""
        mock_executor = MockExecutor(should_fail=True)
        
        self.scheduler.register_workflow(
            name="test_workflow",
            executor_factory=lambda: mock_executor,
            max_retries=3,
            retry_delay=0  # 测试时无延迟
        )
        
        # 第一次执行失败
        result = self.scheduler.execute_workflow("test_workflow")
        self.assertFalse(result["success"])
        
        # 修改执行器为成功
        mock_executor.should_fail = False
        
        # 重试
        result = self.scheduler.retry_failed_workflow("test_workflow")
        self.assertTrue(result["success"])
        self.assertEqual(mock_executor.run_count, 2)
    
    def test_retry_exceeded(self):
        """测试重试次数超限"""
        mock_executor = MockExecutor(should_fail=True)

        self.scheduler.register_workflow(
            name="test_workflow",
            executor_factory=lambda: mock_executor,
            max_retries=1,
            retry_delay=0
        )

        # 执行并失败
        result = self.scheduler.execute_workflow("test_workflow")
        self.assertFalse(result["success"])
        execution_id = result["execution_id"]

        # 第一次重试（应该成功触发重试，但执行失败）
        result = self.scheduler.retry_failed_workflow("test_workflow", execution_id=execution_id)
        # 重试次数检查通过，但执行失败
        self.assertFalse(result["success"])

        # 检查重试次数已达到上限
        status = self.scheduler.state_db.get_workflow_status(execution_id)
        self.assertEqual(status["retry_count"], 1)

        # 第二次重试同一执行ID（应该被拒绝）
        result = self.scheduler.retry_failed_workflow("test_workflow", execution_id=execution_id)
        self.assertFalse(result["success"])
        self.assertIn("最大重试次数", result["error"])
    
    def test_execute_batch(self):
        """测试批量执行"""
        execution_count = {"count": 0}
        
        def create_executor():
            class CounterExecutor:
                def run(self, **kwargs):
                    execution_count["count"] += 1
                    return {"count": execution_count["count"]}
            return CounterExecutor()
        
        self.scheduler.register_workflow(name="wf1", executor_factory=create_executor)
        self.scheduler.register_workflow(name="wf2", executor_factory=create_executor)
        self.scheduler.register_workflow(name="wf3", executor_factory=create_executor)
        
        # 串行执行
        result = self.scheduler.execute_batch(
            workflow_names=["wf1", "wf2", "wf3"],
            parallel=False
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 3)
        self.assertEqual(result["success_count"], 3)
        self.assertEqual(execution_count["count"], 3)
    
    def test_get_scheduler_status(self):
        """测试获取调度器状态"""
        self.scheduler.register_workflow(
            name="wf1",
            priority=WorkflowPriority.HIGH,
            dependencies=["dep1"]
        )
        self.scheduler.register_workflow(name="wf2", priority=WorkflowPriority.NORMAL)
        
        status = self.scheduler.get_scheduler_status()
        
        self.assertEqual(status["registered_workflows"], 2)
        self.assertEqual(status["max_workers"], 2)
        self.assertEqual(len(status["workflows"]), 2)
        
        # 验证工作流详情
        wf_names = [w["name"] for w in status["workflows"]]
        self.assertIn("wf1", wf_names)
        self.assertIn("wf2", wf_names)


if __name__ == "__main__":
    unittest.main()
