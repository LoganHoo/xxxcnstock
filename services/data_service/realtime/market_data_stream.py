#!/usr/bin/env python3
"""
实时行情数据接入

功能：
- WebSocket实时行情订阅
- 行情数据缓存
- 行情推送
- 断线重连

支持数据源：
- Tushare实时行情
- 其他WebSocket数据源
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
import json
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable, Set
from dataclasses import dataclass, asdict
from enum import Enum

import pandas as pd
import polars as pl
import websockets
from websockets.exceptions import ConnectionClosed

from core.logger import setup_logger


class MarketDataType(Enum):
    """行情数据类型"""
    TICK = "tick"           # 逐笔成交
    QUOTE = "quote"         # 五档行情
    KLINE_1MIN = "1min"     # 1分钟K线
    KLINE_5MIN = "5min"     # 5分钟K线


@dataclass
class TickData:
    """逐笔成交数据"""
    code: str
    time: str
    price: float
    volume: int
    amount: float
    type: str  # 'B'买入, 'S'卖出
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class QuoteData:
    """五档行情数据"""
    code: str
    time: str
    
    # 卖盘五档
    ask1_price: float
    ask1_volume: int
    ask2_price: float
    ask2_volume: int
    ask3_price: float
    ask3_volume: int
    ask4_price: float
    ask4_volume: int
    ask5_price: float
    ask5_volume: int
    
    # 买盘五档
    bid1_price: float
    bid1_volume: int
    bid2_price: float
    bid2_volume: int
    bid3_price: float
    bid3_volume: int
    bid4_price: float
    bid4_volume: int
    bid5_price: float
    bid5_volume: int
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class KlineData:
    """K线数据"""
    code: str
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    
    def to_dict(self) -> Dict:
        return asdict(self)


class RealtimeMarketDataStream:
    """实时行情数据流"""
    
    def __init__(self):
        self.logger = setup_logger("realtime_market")
        
        # WebSocket连接
        self.ws = None
        self.ws_url = None
        self.is_connected = False
        
        # 订阅管理
        self.subscribed_codes: Set[str] = set()
        self.subscribed_types: Set[MarketDataType] = set()
        
        # 数据缓存
        self.tick_cache: Dict[str, List[TickData]] = {}
        self.quote_cache: Dict[str, QuoteData] = {}
        self.kline_cache: Dict[str, List[KlineData]] = {}
        
        # 回调函数
        self.tick_callbacks: List[Callable[[TickData], None]] = []
        self.quote_callbacks: List[Callable[[QuoteData], None]] = []
        self.kline_callbacks: List[Callable[[KlineData], None]] = []
        
        # 控制标志
        self._running = False
        self._reconnect_interval = 5  # 重连间隔5秒
        self._max_reconnect = 10      # 最大重连次数
        
        # 统计
        self.stats = {
            'messages_received': 0,
            'ticks_received': 0,
            'quotes_received': 0,
            'klines_received': 0,
            'reconnect_count': 0
        }
    
    async def connect(self, ws_url: str):
        """
        连接WebSocket
        
        Args:
            ws_url: WebSocket地址
        """
        self.ws_url = ws_url
        
        try:
            self.logger.info(f"连接WebSocket: {ws_url}")
            self.ws = await websockets.connect(ws_url)
            self.is_connected = True
            self.logger.info("WebSocket连接成功")
            
            # 恢复订阅
            await self._resubscribe()
            
            # 启动消息接收
            asyncio.create_task(self._receive_messages())
            
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {e}")
            self.is_connected = False
            await self._reconnect()
    
    async def disconnect(self):
        """断开连接"""
        self._running = False
        self.is_connected = False
        
        if self.ws:
            await self.ws.close()
            self.ws = None
        
        self.logger.info("WebSocket已断开")
    
    async def subscribe(self, codes: List[str], data_types: List[MarketDataType]):
        """
        订阅行情
        
        Args:
            codes: 股票代码列表
            data_types: 数据类型列表
        """
        if not self.is_connected:
            self.logger.warning("WebSocket未连接，无法订阅")
            return
        
        # 更新订阅列表
        self.subscribed_codes.update(codes)
        self.subscribed_types.update(data_types)
        
        # 发送订阅请求
        subscribe_msg = {
            "action": "subscribe",
            "codes": codes,
            "types": [t.value for t in data_types]
        }
        
        try:
            await self.ws.send(json.dumps(subscribe_msg))
            self.logger.info(f"订阅行情: {len(codes)} 只股票, {len(data_types)} 种类型")
        except Exception as e:
            self.logger.error(f"订阅失败: {e}")
    
    async def unsubscribe(self, codes: List[str]):
        """
        取消订阅
        
        Args:
            codes: 股票代码列表
        """
        if not self.is_connected:
            return
        
        # 更新订阅列表
        self.subscribed_codes.difference_update(codes)
        
        # 发送取消订阅请求
        unsubscribe_msg = {
            "action": "unsubscribe",
            "codes": codes
        }
        
        try:
            await self.ws.send(json.dumps(unsubscribe_msg))
            self.logger.info(f"取消订阅: {codes}")
        except Exception as e:
            self.logger.error(f"取消订阅失败: {e}")
    
    async def _receive_messages(self):
        """接收消息循环"""
        self._running = True
        
        while self._running and self.is_connected:
            try:
                message = await self.ws.recv()
                self.stats['messages_received'] += 1
                
                # 解析消息
                await self._parse_message(message)
                
            except ConnectionClosed:
                self.logger.warning("WebSocket连接已关闭")
                self.is_connected = False
                await self._reconnect()
                break
            except Exception as e:
                self.logger.error(f"接收消息失败: {e}")
    
    async def _parse_message(self, message: str):
        """解析行情消息"""
        try:
            data = json.loads(message)
            
            msg_type = data.get('type')
            
            if msg_type == 'tick':
                tick = TickData(**data['data'])
                await self._handle_tick(tick)
                
            elif msg_type == 'quote':
                quote = QuoteData(**data['data'])
                await self._handle_quote(quote)
                
            elif msg_type == 'kline':
                kline = KlineData(**data['data'])
                await self._handle_kline(kline)
                
        except Exception as e:
            self.logger.error(f"解析消息失败: {e}")
    
    async def _handle_tick(self, tick: TickData):
        """处理逐笔成交"""
        # 缓存
        if tick.code not in self.tick_cache:
            self.tick_cache[tick.code] = []
        
        self.tick_cache[tick.code].append(tick)
        
        # 限制缓存大小
        if len(self.tick_cache[tick.code]) > 1000:
            self.tick_cache[tick.code] = self.tick_cache[tick.code][-500:]
        
        self.stats['ticks_received'] += 1
        
        # 触发回调
        for callback in self.tick_callbacks:
            try:
                callback(tick)
            except Exception as e:
                self.logger.error(f"Tick回调执行失败: {e}")
    
    async def _handle_quote(self, quote: QuoteData):
        """处理五档行情"""
        # 缓存
        self.quote_cache[quote.code] = quote
        
        self.stats['quotes_received'] += 1
        
        # 触发回调
        for callback in self.quote_callbacks:
            try:
                callback(quote)
            except Exception as e:
                self.logger.error(f"Quote回调执行失败: {e}")
    
    async def _handle_kline(self, kline: KlineData):
        """处理K线数据"""
        # 缓存
        if kline.code not in self.kline_cache:
            self.kline_cache[kline.code] = []
        
        self.kline_cache[kline.code].append(kline)
        
        self.stats['klines_received'] += 1
        
        # 触发回调
        for callback in self.kline_callbacks:
            try:
                callback(kline)
            except Exception as e:
                self.logger.error(f"Kline回调执行失败: {e}")
    
    async def _reconnect(self):
        """断线重连"""
        if self.stats['reconnect_count'] >= self._max_reconnect:
            self.logger.error("重连次数超过上限，放弃重连")
            return
        
        self.stats['reconnect_count'] += 1
        
        self.logger.info(f"{self._reconnect_interval}秒后尝试重连...")
        await asyncio.sleep(self._reconnect_interval)
        
        if self.ws_url:
            await self.connect(self.ws_url)
    
    async def _resubscribe(self):
        """恢复订阅"""
        if self.subscribed_codes and self.subscribed_types:
            await self.subscribe(
                list(self.subscribed_codes),
                list(self.subscribed_types)
            )
    
    def on_tick(self, callback: Callable[[TickData], None]):
        """注册Tick回调"""
        self.tick_callbacks.append(callback)
    
    def on_quote(self, callback: Callable[[QuoteData], None]):
        """注册Quote回调"""
        self.quote_callbacks.append(callback)
    
    def on_kline(self, callback: Callable[[KlineData], None]):
        """注册Kline回调"""
        self.kline_callbacks.append(callback)
    
    def get_latest_quote(self, code: str) -> Optional[QuoteData]:
        """获取最新五档行情"""
        return self.quote_cache.get(code)
    
    def get_recent_ticks(self, code: str, n: int = 100) -> List[TickData]:
        """获取最近N笔成交"""
        ticks = self.tick_cache.get(code, [])
        return ticks[-n:] if len(ticks) > n else ticks
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        return self.stats.copy()


class MarketDataMonitor:
    """行情数据监控器"""
    
    def __init__(self, stream: RealtimeMarketDataStream):
        self.logger = setup_logger("market_monitor")
        self.stream = stream
        
        # 监控配置
        self.price_change_threshold = 0.05  # 5%涨跌幅预警
        self.volume_surge_threshold = 3.0   # 成交量放大3倍预警
        
        # 监控状态
        self.price_alerts: Dict[str, List[Dict]] = {}
        self.volume_alerts: Dict[str, List[Dict]] = {}
        
        # 注册回调
        self.stream.on_quote(self._check_price_alert)
    
    def _check_price_alert(self, quote: QuoteData):
        """检查价格异动"""
        # TODO: 实现价格异动检测
        pass
    
    def get_alerts(self, code: Optional[str] = None) -> Dict:
        """获取预警信息"""
        if code:
            return {
                'price': self.price_alerts.get(code, []),
                'volume': self.volume_alerts.get(code, [])
            }
        
        return {
            'price': self.price_alerts,
            'volume': self.volume_alerts
        }


# 模拟数据源（用于测试）
class MockMarketDataSource:
    """模拟行情数据源"""
    
    def __init__(self):
        self.logger = setup_logger("mock_market")
        self.clients = []
    
    async def start_server(self, host: str = "localhost", port: int = 8765):
        """启动模拟服务器"""
        import websockets
        
        async def handler(websocket, path):
            self.clients.append(websocket)
            self.logger.info(f"客户端连接: {websocket.remote_address}")
            
            try:
                async for message in websocket:
                    data = json.loads(message)
                    
                    if data.get('action') == 'subscribe':
                        # 开始发送模拟数据
                        asyncio.create_task(
                            self._send_mock_data(websocket, data.get('codes', []))
                        )
                        
            except ConnectionClosed:
                self.logger.info("客户端断开")
            finally:
                self.clients.remove(websocket)
        
        self.logger.info(f"启动模拟服务器: ws://{host}:{port}")
        await websockets.serve(handler, host, port)
    
    async def _send_mock_data(self, websocket, codes: List[str]):
        """发送模拟数据"""
        while True:
            for code in codes:
                # 模拟五档行情
                quote = {
                    "type": "quote",
                    "data": {
                        "code": code,
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "ask1_price": 10.0,
                        "ask1_volume": 100,
                        "bid1_price": 9.99,
                        "bid1_volume": 200,
                        # ... 其他档位
                    }
                }
                
                try:
                    await websocket.send(json.dumps(quote))
                except:
                    return
                
                await asyncio.sleep(1)  # 每秒发送一次


# 便捷函数
def create_realtime_stream() -> RealtimeMarketDataStream:
    """创建实时行情流"""
    return RealtimeMarketDataStream()


async def subscribe_market_data(codes: List[str], data_types: List[MarketDataType]):
    """便捷函数：订阅行情"""
    stream = create_realtime_stream()
    
    # 连接（使用模拟服务器）
    await stream.connect("ws://localhost:8765")
    
    # 订阅
    await stream.subscribe(codes, data_types)
    
    return stream


if __name__ == "__main__":
    # 测试
    async def test():
        # 创建行情流
        stream = create_realtime_stream()
        
        # 注册回调
        def on_tick(tick):
            print(f"Tick: {tick.code} @ {tick.price}")
        
        stream.on_tick(on_tick)
        
        # 连接（实际使用时替换为真实WebSocket地址）
        # await stream.connect("ws://real-market-data.com/ws")
        
        print("实时行情测试完成")
    
    asyncio.run(test())
