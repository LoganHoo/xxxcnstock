from functools import lru_cache
from pydantic import ConfigDict
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """全局配置"""
    
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
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
    EMAIL_SMTP_SERVER: Optional[str] = None
    EMAIL_SMTP_PORT: int = 465
    EMAIL_USERNAME: Optional[str] = None
    EMAIL_PASSWORD: Optional[str] = None
    EMAIL_RECIPIENTS: str = ""
    EMAIL_TIMEOUT: int = 30

    # 邮件API配置
    EMAIL_USE_API: bool = True
    EMAIL_API_URL: str = "http://192.168.1.168:2000/send_email"

    # 调度配置
    SCHEDULE_ENABLED: bool = True

    # 数据库配置
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: Optional[str] = None
    DB_NAME: str = "xcn_db"
    DB_CHARSET: str = "utf8mb4"
    DB_POOL_SIZE: int = 10
    DB_POOL_RECYCLE: int = 3600

    # Redis配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    REDIS_POOL_SIZE: int = 10
    REDIS_SOCKET_TIMEOUT: int = 5
    
    # Kafka配置
    KAFKA_BASE_URL: str = "http://49.233.10.199:8082"
    KAFKA_CLUSTER_NAME: str = "CentOS-Kafka-Cluster"
    KAFKA_TOPIC_NAME: str = "nextaiconin.xsignal.keyprice"
    KAFKA_TIMEOUT: int = 30
    KAFKA_ENABLED: bool = True

    # AI配置
    AI_API_URL: str = "http://192.168.1.168:2000"
    AI_TIMEOUT: int = 100
    AI_FALLBACK_ENABLED: bool = True

    # Gemini AI配置
    GEMINI_API_KEY: Optional[str] = None
    GEMINI_MODEL: str = "gemini-3.1-pro-preview"
    GEMINI_MODEL_FALLBACK1: str = "gemini-3-flash"
    GEMINI_MODEL_FALLBACK2: str = "gemini-2.5-flash"
    GEMINI_TIMEOUT: int = 30
    GEMINI_MAX_HISTORY: int = 10
    GEMINI_HISTORY_WINDOW: int = 30
    GEMINI_CONFIDENCE_THRESHOLD: float = 0.6
    GEMINI_ENABLED: bool = False
    GEMINI_ANALYSIS_ENABLED: bool = False
    GEMINI_CALL_INTERVAL: int = 60

    # Nacos配置
    NACOS_SERVER_ADDR: str = "49.233.10.199:8188"
    NACOS_NAMESPACE: str = ""
    NACOS_USERNAME: str = "nacos"
    NACOS_PASSWORD: Optional[str] = None
    NACOS_TIMEOUT: int = 10


@lru_cache()
def get_settings() -> Settings:
    """获取配置单例"""
    return Settings()


class NacosClientSingleton:
    """Nacos客户端单例"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            import nacos
            settings = get_settings()
            cls._instance = nacos.NacosClient(
                settings.NACOS_SERVER_ADDR,
                namespace=settings.NACOS_NAMESPACE,
                username=settings.NACOS_USERNAME,
                password=settings.NACOS_PASSWORD or "",
                timeout=settings.NACOS_TIMEOUT
            )
        return cls._instance
    
    @classmethod
    def get_client(cls):
        """获取Nacos客户端实例"""
        return cls()


def get_nacos_client():
    """获取Nacos客户端单例"""
    return NacosClientSingleton.get_client()
