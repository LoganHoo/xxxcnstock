"""每日工作流程集成测试"""
import pytest
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDailyWorkflow:
    """每日工作流程测试"""
    
    def test_data_collect_task(self):
        """测试数据采集任务"""
        from scripts.daily_tasks.task_data_collect import DataCollectTask
        task = DataCollectTask()
        assert task.name == "data_collect"
        assert task.description == "采集实时行情和K线数据"
    
    def test_data_audit_task(self):
        """测试数据验证任务"""
        from scripts.daily_tasks.task_data_audit import DataAuditTask
        task = DataAuditTask()
        assert task.name == "data_audit"
        assert task.description == "验证数据完整性和新鲜度"
    
    def test_daily_review_task(self):
        """测试当日复盘任务"""
        from scripts.daily_tasks.task_daily_review import DailyReviewTask
        task = DailyReviewTask()
        assert task.name == "daily_review"
        assert task.description == "生成当日复盘报告"
    
    def test_stock_pick_task(self):
        """测试次日选股任务"""
        from scripts.daily_tasks.task_stock_pick import StockPickTask
        task = StockPickTask()
        assert task.name == "stock_pick"
        assert task.description == "生成次日选股报告"
    
    def test_morning_push_task(self):
        """测试早间推送任务"""
        from scripts.daily_tasks.task_morning_push import MorningPushTask
        task = MorningPushTask()
        assert task.name == "morning_push"
        assert task.description == "推送次日操作报告"
    
    def test_open_process_task(self):
        """测试开盘处理任务"""
        from scripts.daily_tasks.task_open_process import OpenProcessTask
        task = OpenProcessTask()
        assert task.name == "open_process"
        assert task.description == "处理一字涨停股票"
    
    def test_kafka_producer_available(self):
        """测试Kafka生产者可用"""
        from unittest.mock import patch, MagicMock
        with patch('services.notify_service.channels.kafka_producer.get_settings') as mock_settings:
            mock_settings.return_value = MagicMock(
                KAFKA_BROKER="49.233.10.199:9092",
                KAFKA_STOCK_PICKS_TOPIC="xcnstock_stock_picks",
                KAFKA_LIMIT_UP_TOPIC="xcnstock_limit_up",
                KAFKA_ENABLED=False
            )
            from services.notify_service.channels.kafka_producer import get_kafka_producer
            import services.notify_service.channels.kafka_producer as module
            module._producer_instance = None
            producer = get_kafka_producer()
            assert producer is not None
    
    def test_base_task_is_abstract(self):
        """测试BaseTask是抽象类"""
        from scripts.daily_tasks.base_task import BaseTask
        import inspect
        assert inspect.isabstract(BaseTask)
    
    def test_all_tasks_inherit_from_base(self):
        """测试所有任务继承自BaseTask"""
        from scripts.daily_tasks.base_task import BaseTask
        from scripts.daily_tasks.task_data_collect import DataCollectTask
        from scripts.daily_tasks.task_data_audit import DataAuditTask
        from scripts.daily_tasks.task_daily_review import DailyReviewTask
        from scripts.daily_tasks.task_stock_pick import StockPickTask
        from scripts.daily_tasks.task_morning_push import MorningPushTask
        from scripts.daily_tasks.task_open_process import OpenProcessTask
        
        assert issubclass(DataCollectTask, BaseTask)
        assert issubclass(DataAuditTask, BaseTask)
        assert issubclass(DailyReviewTask, BaseTask)
        assert issubclass(StockPickTask, BaseTask)
        assert issubclass(MorningPushTask, BaseTask)
        assert issubclass(OpenProcessTask, BaseTask)
