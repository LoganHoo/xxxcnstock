#!/usr/bin/env python3
"""数据库连接池管理器"""
import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()


class DatabasePoolManager:
    """数据库连接池管理器 - 单例模式"""
    
    _instance = None
    _pools = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_pool(self, db_name: str, conn_str: str, pool_size: int = 5, max_overflow: int = 10):
        """获取数据库连接池"""
        if db_name not in self._pools:
            engine = create_engine(
                conn_str,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            self._pools[db_name] = {
                'engine': engine,
                'Session': sessionmaker(bind=engine)
            }
        return self._pools[db_name]
    
    def get_session(self, db_name: str):
        """获取数据库会话"""
        if db_name in self._pools:
            return self._pools[db_name]['Session']()
        raise ValueError(f"Database {db_name} not initialized")
    
    def close_all(self):
        """关闭所有连接池"""
        for db_name, pool_info in self._pools.items():
            pool_info['engine'].dispose()
        self._pools.clear()


# 全局实例
db_pool = DatabasePoolManager()


def get_db_pool():
    """获取全局数据库连接池实例"""
    return db_pool
