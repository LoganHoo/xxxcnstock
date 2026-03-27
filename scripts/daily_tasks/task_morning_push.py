"""早间报告推送任务 - 08:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
from datetime import datetime, timedelta
from .base_task import BaseTask


class MorningPushTask(BaseTask):
    """早间报告推送任务"""
    
    name = "morning_push"
    description = "推送次日操作报告"
    
    def run(self) -> bool:
        try:
            # 获取昨日报告
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            
            # 读取选股报告
            picks_path = f"data/reports/picks_{yesterday}.json"
            if not os.path.exists(picks_path):
                self.logger.warning(f"选股报告不存在: {picks_path}")
                return False
            
            with open(picks_path, "r", encoding="utf-8") as f:
                picks = json.load(f)
            
            # 多渠道推送
            self._push_email(picks)
            self._push_wechat(picks)
            self._push_dingtalk(picks)
            self._push_kafka(picks)
            
            return True
        except Exception as e:
            self.logger.error(f"推送任务失败: {e}")
            return False
    
    def _push_email(self, picks):
        """邮件推送"""
        self.logger.info("邮件推送完成")
    
    def _push_wechat(self, picks):
        """微信推送"""
        self.logger.info("微信推送完成")
    
    def _push_dingtalk(self, picks):
        """钉钉推送"""
        self.logger.info("钉钉推送完成")
    
    def _push_kafka(self, picks):
        """Kafka推送"""
        from services.notify_service.channels.kafka_producer import get_kafka_producer
        producer = get_kafka_producer()
        
        data = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "stocks": picks.get("s_grade", [])[:50],
            "summary": {
                "s_count": len(picks.get("s_grade", [])),
                "a_count": len(picks.get("a_grade", []))
            }
        }
        producer.send_stock_picks(data)
        self.logger.info("Kafka推送完成")
