from functools import lru_cache
from pydantic import ConfigDict
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """全局配置"""
    
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    # 应用配置
    APP_NAME: str = "XCNStock"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    
    # 服务端口
    GATEWAY_PORT: int = 8000
    DATA_SERVICE_PORT: int = 8001
    STOCK_SERVICE_PORT: int = 8002
    LIMIT_SERVICE_PORT: int = 8003
    NOTIFY_SERVICE_PORT: int = 8004
    
    # 数据路径
    DATA_DIR: str = "data"
    LOG_DIR: str = "logs"
    
    # 通知配置
    WECHAT_SEND_KEY: Optional[str] = None
    DINGTALK_WEBHOOK: Optional[str] = None
    DINGTALK_SECRET: Optional[str] = None
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: int = 465
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    EMAIL_RECIPIENTS: str = ""
    
    # 调度配置
    SCHEDULE_ENABLED: bool = True
    
    # Redis配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    
    # Kafka配置
    KAFKA_BROKER: str = "localhost:9092"
    KAFKA_STOCK_PICKS_TOPIC: str = "xcnstock_stock_picks"
    KAFKA_LIMIT_UP_TOPIC: str = "xcnstock_limit_up"
    KAFKA_ENABLED: bool = True


@lru_cache()
def get_settings() -> Settings:
    """获取配置单例"""
    return Settings()
