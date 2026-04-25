#!/usr/bin/env python3
"""
工作流状态数据库测试
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import unittest
import tempfile
import shutil
from pathlib import Path

from core.workflow_state_db import WorkflowStateDB, WorkflowExecutionStatus


class TestWorkflowStateDB(unittest.TestCase):
    """测试工作流状态数据库"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test_workflow_state.db"
        self.db = WorkflowStateDB(self.db_path)
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir)
    
    def test_create_execution(self):
        """测试创建工作流执行记录"""
        exec_id = self.db.create_execution(
            "test_workflow",
            "test_exec_001",
            {"param1": "value1", "param2": 123}
        )
        
        self.assertIsNotNone(exec_id)
        self.assertIsInstance(exec_id, int)
        
        # 验证记录
        status = self.db.get_workflow_status("test_exec_001")
        self.assertIsNotNone(status)
        self.assertEqual(status['workflow_name'], "test_workflow")
        self.assertEqual(status['execution_id'], "test_exec_001")
        self.assertEqual(status['status'], "pending")
        self.assertEqual(status['params']['param1'], "value1")
    
    def test_update_execution_status(self):
        """测试更新执行状态"""
        # 创建记录
        self.db.create_execution("test_workflow", "test_exec_002")
        
        # 更新为成功状态
        self.db.update_execution_status(
            "test_exec_002",
            "success",
            {"output": "result"},
            duration_seconds=120
        )
        
        # 验证
        status = self.db.get_workflow_status("test_exec_002")
        self.assertEqual(status['status'], "success")
        self.assertEqual(status['duration_seconds'], 120)
        self.assertEqual(status['result']['output'], "result")
    
    def test_increment_retry_count(self):
        """测试增加重试次数"""
        self.db.create_execution("test_workflow", "test_exec_003")
        
        # 增加重试次数
        retry_count = self.db.increment_retry_count("test_exec_003")
        self.assertEqual(retry_count, 1)
        
        retry_count = self.db.increment_retry_count("test_exec_003")
        self.assertEqual(retry_count, 2)
        
        # 验证
        status = self.db.get_workflow_status("test_exec_003")
        self.assertEqual(status['retry_count'], 2)
    
    def test_get_latest_execution(self):
        """测试获取最新执行记录"""
        # 创建多个执行记录
        self.db.create_execution("workflow_a", "exec_a_001")
        self.db.create_execution("workflow_a", "exec_a_002")
        self.db.create_execution("workflow_b", "exec_b_001")
        
        # 获取最新
        latest = self.db.get_latest_execution("workflow_a")
        self.assertIsNotNone(latest)
        self.assertEqual(latest['execution_id'], "exec_a_002")
    
    def test_list_executions(self):
        """测试列出执行记录"""
        # 创建多个记录
        self.db.create_execution("workflow_a", "exec_001")
        self.db.create_execution("workflow_a", "exec_002")
        self.db.create_execution("workflow_b", "exec_003")
        
        # 列出所有
        all_execs = self.db.list_executions()
        self.assertEqual(len(all_execs), 3)
        
        # 按工作流过滤
        filtered = self.db.list_executions(workflow_name="workflow_a")
        self.assertEqual(len(filtered), 2)
        
        # 按状态过滤
        self.db.update_execution_status("exec_001", "success")
        by_status = self.db.list_executions(status="success")
        self.assertEqual(len(by_status), 1)
    
    def test_checkpoint_management(self):
        """测试检查点管理"""
        self.db.create_execution("test_workflow", "test_exec_004")
        
        # 添加检查点
        self.db.add_checkpoint("test_exec_004", "data_quality_check", "PASS",
                              {"score": 95, "issues": []})
        self.db.add_checkpoint("test_exec_004", "validation_check", "PASS",
                              {"validated": True})
        
        # 获取检查点
        checkpoints = self.db.get_checkpoints("test_exec_004")
        self.assertEqual(len(checkpoints), 2)
        self.assertEqual(checkpoints[0]['checkpoint_name'], "data_quality_check")
        self.assertEqual(checkpoints[0]['status'], "PASS")
        self.assertEqual(checkpoints[0]['details']['score'], 95)
    
    def test_dependency_check(self):
        """测试依赖检查记录"""
        self.db.add_dependency_check("test_workflow", "stock_list", "healthy",
                                    "股票列表正常")
        self.db.add_dependency_check("test_workflow", "kline_data", "unhealthy",
                                    "K线数据缺失")
        
        deps = self.db.get_dependency_status("test_workflow")
        self.assertEqual(len(deps), 2)
        self.assertEqual(deps[0]['dependency_name'], "stock_list")
        self.assertEqual(deps[1]['dependency_name'], "kline_data")
    
    def test_statistics(self):
        """测试统计功能"""
        # 创建多个执行记录
        self.db.create_execution("workflow_a", "exec_001")
        self.db.update_execution_status("exec_001", "success", duration_seconds=100)
        
        self.db.create_execution("workflow_a", "exec_002")
        self.db.update_execution_status("exec_002", "success", duration_seconds=200)
        
        self.db.create_execution("workflow_a", "exec_003")
        self.db.update_execution_status("exec_003", "failed")
        
        # 获取统计
        stats = self.db.get_statistics("workflow_a")
        self.assertEqual(stats['total_executions'], 3)
        self.assertEqual(stats['status_counts']['success'], 2)
        self.assertEqual(stats['status_counts']['failed'], 1)
        self.assertAlmostEqual(stats['success_rate'], 66.67, places=1)
        self.assertEqual(stats['avg_duration_seconds'], 150)
    
    def test_cleanup_old_records(self):
        """测试清理旧记录"""
        # 创建记录
        self.db.create_execution("test_workflow", "exec_old")
        self.db.create_execution("test_workflow", "exec_new")
        
        # 清理（保留0天，即清理所有）
        self.db.cleanup_old_records(days=0)
        
        # 验证
        execs = self.db.list_executions()
        self.assertEqual(len(execs), 0)


class TestWorkflowStateDBEdgeCases(unittest.TestCase):
    """测试边界情况"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"
        self.db = WorkflowStateDB(self.db_path)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_get_nonexistent_execution(self):
        """测试获取不存在的执行记录"""
        status = self.db.get_workflow_status("nonexistent")
        self.assertIsNone(status)
    
    def test_get_nonexistent_latest(self):
        """测试获取不存在的最新记录"""
        latest = self.db.get_latest_execution("nonexistent_workflow")
        self.assertIsNone(latest)
    
    def test_empty_statistics(self):
        """测试空统计"""
        stats = self.db.get_statistics("nonexistent")
        self.assertEqual(stats['total_executions'], 0)
        self.assertEqual(stats['success_rate'], 0)


if __name__ == "__main__":
    unittest.main()
