"""Kafka生产者测试"""
from unittest.mock import patch, MagicMock


class TestKafkaProducer:
    """Kafka生产者测试"""
    
    def test_kafka_producer_init(self):
        """测试生产者初始化"""
        with patch('services.notify_service.channels.kafka_producer.get_settings') as mock_settings:
            mock_settings.return_value = MagicMock(
                KAFKA_BROKER="49.233.10.199:9092",
                KAFKA_STOCK_PICKS_TOPIC="xcnstock_stock_picks",
                KAFKA_LIMIT_UP_TOPIC="xcnstock_limit_up",
                KAFKA_ENABLED=False  # 禁用连接
            )
            from services.notify_service.channels.kafka_producer import KafkaProducer
            producer = KafkaProducer()
            assert producer is not None
            assert producer.broker == "49.233.10.199:9092"
    
    def test_send_stock_picks_disabled(self):
        """测试禁用状态发送股票推荐"""
        with patch('services.notify_service.channels.kafka_producer.get_settings') as mock_settings:
            mock_settings.return_value = MagicMock(
                KAFKA_BROKER="49.233.10.199:9092",
                KAFKA_STOCK_PICKS_TOPIC="xcnstock_stock_picks",
                KAFKA_LIMIT_UP_TOPIC="xcnstock_limit_up",
                KAFKA_ENABLED=False
            )
            from services.notify_service.channels.kafka_producer import KafkaProducer
            producer = KafkaProducer()
            data = {
                "date": "2026-03-17",
                "stocks": [{"code": "600721", "name": "百花医药", "score": 92.0}]
            }
            result = producer.send_stock_picks(data)
            # 禁用状态下返回False
            assert result is False
    
    def test_send_limit_up_disabled(self):
        """测试禁用状态发送打板数据"""
        with patch('services.notify_service.channels.kafka_producer.get_settings') as mock_settings:
            mock_settings.return_value = MagicMock(
                KAFKA_BROKER="49.233.10.199:9092",
                KAFKA_STOCK_PICKS_TOPIC="xcnstock_stock_picks",
                KAFKA_LIMIT_UP_TOPIC="xcnstock_limit_up",
                KAFKA_ENABLED=False
            )
            from services.notify_service.channels.kafka_producer import KafkaProducer
            producer = KafkaProducer()
            data = {
                "date": "2026-03-17",
                "stocks": [{"code": "002235", "name": "安妮股份"}]
            }
            result = producer.send_limit_up(data)
            assert result is False
    
    def test_get_kafka_producer_singleton(self):
        """测试单例模式"""
        with patch('services.notify_service.channels.kafka_producer.get_settings') as mock_settings:
            mock_settings.return_value = MagicMock(
                KAFKA_BROKER="49.233.10.199:9092",
                KAFKA_STOCK_PICKS_TOPIC="xcnstock_stock_picks",
                KAFKA_LIMIT_UP_TOPIC="xcnstock_limit_up",
                KAFKA_ENABLED=False
            )
            from services.notify_service.channels.kafka_producer import get_kafka_producer
            # 重置单例
            import services.notify_service.channels.kafka_producer as module
            module._producer_instance = None
            
            producer1 = get_kafka_producer()
            producer2 = get_kafka_producer()
            assert producer1 is producer2
