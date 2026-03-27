"""Kafka消息生产者"""
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from core.config import get_settings
from core.logger import setup_logger

logger = setup_logger("kafka_producer", log_file="system/kafka.log")


class KafkaProducer:
    """Kafka消息生产者"""
    
    def __init__(self):
        settings = get_settings()
        self.broker = settings.KAFKA_BROKER
        self.stock_picks_topic = settings.KAFKA_STOCK_PICKS_TOPIC
        self.limit_up_topic = settings.KAFKA_LIMIT_UP_TOPIC
        self.enabled = settings.KAFKA_ENABLED
        self._producer = None
        
        if self.enabled:
            self._connect()
    
    def _connect(self):
        """连接Kafka"""
        try:
            from kafka import KafkaProducer as _KafkaProducer
            self._producer = _KafkaProducer(
                bootstrap_servers=self.broker,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            logger.info(f"Kafka连接成功: {self.broker}")
        except Exception as e:
            logger.error(f"Kafka连接失败: {e}")
            self._producer = None
    
    def send_stock_picks(self, data: Dict[str, Any]) -> bool:
        """发送股票推荐数据"""
        if not self._producer:
            logger.warning("Kafka未连接，跳过发送")
            return False
        
        try:
            data["sent_at"] = datetime.now().isoformat()
            future = self._producer.send(self.stock_picks_topic, data)
            self._producer.flush()
            logger.info(f"股票推荐已发送到Kafka: {len(data.get('stocks', []))}只")
            return True
        except Exception as e:
            logger.error(f"发送股票推荐失败: {e}")
            return False
    
    def send_limit_up(self, data: Dict[str, Any]) -> bool:
        """发送打板股票数据"""
        if not self._producer:
            logger.warning("Kafka未连接，跳过发送")
            return False
        
        try:
            data["sent_at"] = datetime.now().isoformat()
            future = self._producer.send(self.limit_up_topic, data)
            self._producer.flush()
            logger.info(f"打板数据已发送到Kafka: {len(data.get('stocks', []))}只")
            return True
        except Exception as e:
            logger.error(f"发送打板数据失败: {e}")
            return False
    
    def close(self):
        """关闭连接"""
        if self._producer:
            self._producer.close()
            logger.info("Kafka连接已关闭")


# 单例
_producer_instance: Optional[KafkaProducer] = None

def get_kafka_producer() -> KafkaProducer:
    """获取Kafka生产者单例"""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = KafkaProducer()
    return _producer_instance
