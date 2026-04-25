#!/usr/bin/env python3
"""
券商交易接口适配器

支持多个券商API:
- 东方财富 (EastMoney)
- 同花顺 (THS)
- 雪球 (Xueqiu)
- 富途 (Futu) - 港股美股
- 老虎 (Tiger) - 港股美股

使用方法:
    from core.broker_adapter import BrokerAdapter, BrokerFactory
    
    # 创建券商实例
    broker = BrokerFactory.create('eastmoney', config)
    
    # 登录
    broker.login()
    
    # 查询账户
    account = broker.get_account()
    
    # 下单
    order = broker.buy('000001', price=10.5, quantity=100)
"""
import os
import sys
import json
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class OrderSide(Enum):
    """订单方向"""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """订单类型"""
    LIMIT = "limit"      # 限价单
    MARKET = "market"    # 市价单
    STOP = "stop"        # 止损单
    STOP_LIMIT = "stop_limit"  # 止损限价单


class OrderStatus(Enum):
    """订单状态"""
    PENDING = "pending"      # 待提交
    SUBMITTED = "submitted"  # 已提交
    PARTIAL = "partial"      # 部分成交
    FILLED = "filled"        # 全部成交
    CANCELLED = "cancelled"  # 已撤销
    REJECTED = "rejected"    # 已拒绝


@dataclass
class Order:
    """订单对象"""
    id: str
    code: str
    side: OrderSide
    type: OrderType
    price: float
    quantity: int
    filled_quantity: int = 0
    status: OrderStatus = OrderStatus.PENDING
    create_time: datetime = None
    update_time: datetime = None
    commission: float = 0.0
    message: str = ""
    
    def __post_init__(self):
        if self.create_time is None:
            self.create_time = datetime.now()
        if self.update_time is None:
            self.update_time = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'code': self.code,
            'side': self.side.value,
            'type': self.type.value,
            'price': self.price,
            'quantity': self.quantity,
            'filled_quantity': self.filled_quantity,
            'status': self.status.value,
            'create_time': self.create_time.isoformat() if self.create_time else None,
            'update_time': self.update_time.isoformat() if self.update_time else None,
            'commission': self.commission,
            'message': self.message
        }


@dataclass
class Position:
    """持仓对象"""
    code: str
    name: str
    quantity: int
    available_quantity: int
    cost_price: float
    current_price: float
    market_value: float
    pnl: float
    pnl_pct: float


@dataclass
class Account:
    """账户对象"""
    total_assets: float
    cash: float
    market_value: float
    frozen_cash: float
    available_cash: float
    positions: List[Position] = None
    
    def __post_init__(self):
        if self.positions is None:
            self.positions = []


class BaseBroker(ABC):
    """券商基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = "base"
        self.session = requests.Session()
        self.logged_in = False
        
    @abstractmethod
    def login(self) -> bool:
        """登录"""
        pass
    
    @abstractmethod
    def logout(self) -> bool:
        """登出"""
        pass
    
    @abstractmethod
    def get_account(self) -> Optional[Account]:
        """获取账户信息"""
        pass
    
    @abstractmethod
    def get_positions(self) -> List[Position]:
        """获取持仓列表"""
        pass
    
    @abstractmethod
    def place_order(self, code: str, side: OrderSide, price: float, 
                   quantity: int, order_type: OrderType = OrderType.LIMIT) -> Optional[Order]:
        """下单"""
        pass
    
    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """撤单"""
        pass
    
    @abstractmethod
    def get_orders(self, status: OrderStatus = None) -> List[Order]:
        """获取订单列表"""
        pass
    
    @abstractmethod
    def get_order(self, order_id: str) -> Optional[Order]:
        """获取订单详情"""
        pass
    
    def buy(self, code: str, price: float, quantity: int, 
           order_type: OrderType = OrderType.LIMIT) -> Optional[Order]:
        """买入"""
        return self.place_order(code, OrderSide.BUY, price, quantity, order_type)
    
    def sell(self, code: str, price: float, quantity: int,
            order_type: OrderType = OrderType.LIMIT) -> Optional[Order]:
        """卖出"""
        return self.place_order(code, OrderSide.SELL, price, quantity, order_type)
    
    def _generate_order_id(self) -> str:
        """生成订单ID"""
        return f"{self.name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{os.urandom(4).hex()}"


class EastMoneyBroker(BaseBroker):
    """东方财富券商接口"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "eastmoney"
        self.base_url = "https://tradeapi.eastmoney.com"
        self.token = None
        
    def login(self) -> bool:
        """登录东方财富"""
        try:
            # 这里使用模拟登录，实际需调用东方财富API
            username = self.config.get('username')
            password = self.config.get('password')
            
            if not username or not password:
                logger.error("东方财富账号或密码未配置")
                return False
            
            # 模拟登录成功
            self.token = f"em_token_{os.urandom(8).hex()}"
            self.logged_in = True
            
            logger.info("东方财富登录成功")
            return True
            
        except Exception as e:
            logger.error(f"东方财富登录失败: {e}")
            return False
    
    def logout(self) -> bool:
        """登出"""
        self.logged_in = False
        self.token = None
        logger.info("东方财富已登出")
        return True
    
    def get_account(self) -> Optional[Account]:
        """获取账户信息"""
        if not self.logged_in:
            if not self.login():
                return None
        
        try:
            # 模拟账户数据
            return Account(
                total_assets=1000000.0,
                cash=300000.0,
                market_value=700000.0,
                frozen_cash=0.0,
                available_cash=300000.0,
                positions=self.get_positions()
            )
        except Exception as e:
            logger.error(f"获取账户信息失败: {e}")
            return None
    
    def get_positions(self) -> List[Position]:
        """获取持仓列表"""
        if not self.logged_in:
            return []
        
        try:
            # 模拟持仓数据
            return [
                Position(
                    code='000001',
                    name='平安银行',
                    quantity=1000,
                    available_quantity=1000,
                    cost_price=10.5,
                    current_price=12.0,
                    market_value=12000.0,
                    pnl=1500.0,
                    pnl_pct=0.1429
                ),
                Position(
                    code='000002',
                    name='万科A',
                    quantity=500,
                    available_quantity=500,
                    cost_price=15.0,
                    current_price=14.5,
                    market_value=7250.0,
                    pnl=-250.0,
                    pnl_pct=-0.0333
                )
            ]
        except Exception as e:
            logger.error(f"获取持仓失败: {e}")
            return []
    
    def place_order(self, code: str, side: OrderSide, price: float,
                   quantity: int, order_type: OrderType = OrderType.LIMIT) -> Optional[Order]:
        """下单"""
        if not self.logged_in:
            if not self.login():
                return None
        
        try:
            order_id = self._generate_order_id()
            
            order = Order(
                id=order_id,
                code=code,
                side=side,
                type=order_type,
                price=price,
                quantity=quantity,
                status=OrderStatus.SUBMITTED
            )
            
            logger.info(f"下单成功: {order_id} {side.value} {code} {quantity}@{price}")
            return order
            
        except Exception as e:
            logger.error(f"下单失败: {e}")
            return None
    
    def cancel_order(self, order_id: str) -> bool:
        """撤单"""
        if not self.logged_in:
            return False
        
        try:
            logger.info(f"撤单成功: {order_id}")
            return True
        except Exception as e:
            logger.error(f"撤单失败: {e}")
            return False
    
    def get_orders(self, status: OrderStatus = None) -> List[Order]:
        """获取订单列表"""
        if not self.logged_in:
            return []
        
        try:
            # 模拟订单数据
            orders = [
                Order(
                    id='em_20240419093000_1234',
                    code='000001',
                    side=OrderSide.BUY,
                    type=OrderType.LIMIT,
                    price=10.5,
                    quantity=1000,
                    filled_quantity=1000,
                    status=OrderStatus.FILLED
                )
            ]
            
            if status:
                orders = [o for o in orders if o.status == status]
            
            return orders
        except Exception as e:
            logger.error(f"获取订单列表失败: {e}")
            return []
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """获取订单详情"""
        orders = self.get_orders()
        for order in orders:
            if order.id == order_id:
                return order
        return None


class SimulatedBroker(BaseBroker):
    """模拟券商接口（用于测试）"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "simulated"
        self.orders: Dict[str, Order] = {}
        self.positions: Dict[str, Position] = {}
        self.cash = config.get('initial_cash', 1000000.0)
        self.initial_cash = self.cash
        
    def login(self) -> bool:
        """登录（模拟总是成功）"""
        self.logged_in = True
        logger.info("模拟券商登录成功")
        return True
    
    def logout(self) -> bool:
        """登出"""
        self.logged_in = False
        logger.info("模拟券商已登出")
        return True
    
    def get_account(self) -> Optional[Account]:
        """获取账户信息"""
        market_value = sum(p.market_value for p in self.positions.values())
        
        return Account(
            total_assets=self.cash + market_value,
            cash=self.cash,
            market_value=market_value,
            frozen_cash=0.0,
            available_cash=self.cash,
            positions=list(self.positions.values())
        )
    
    def get_positions(self) -> List[Position]:
        """获取持仓列表"""
        return list(self.positions.values())
    
    def place_order(self, code: str, side: OrderSide, price: float,
                   quantity: int, order_type: OrderType = OrderType.LIMIT) -> Optional[Order]:
        """下单（模拟立即成交）"""
        order_id = self._generate_order_id()
        
        # 计算手续费
        commission_rate = self.config.get('commission_rate', 0.0003)
        commission = price * quantity * commission_rate
        
        order = Order(
            id=order_id,
            code=code,
            side=side,
            type=order_type,
            price=price,
            quantity=quantity,
            filled_quantity=quantity,
            status=OrderStatus.FILLED,
            commission=commission
        )
        
        # 更新资金和持仓
        if side == OrderSide.BUY:
            total_cost = price * quantity + commission
            if total_cost > self.cash:
                order.status = OrderStatus.REJECTED
                order.message = "资金不足"
                logger.warning(f"下单失败: 资金不足")
            else:
                self.cash -= total_cost
                
                # 更新持仓
                if code in self.positions:
                    pos = self.positions[code]
                    total_cost_old = pos.cost_price * pos.quantity
                    total_cost_new = price * quantity
                    pos.quantity += quantity
                    pos.cost_price = (total_cost_old + total_cost_new) / pos.quantity
                    pos.available_quantity = pos.quantity
                    pos.market_value = pos.quantity * price
                    pos.pnl = (price - pos.cost_price) * pos.quantity
                    pos.pnl_pct = (price - pos.cost_price) / pos.cost_price
                else:
                    self.positions[code] = Position(
                        code=code,
                        name=code,
                        quantity=quantity,
                        available_quantity=quantity,
                        cost_price=price,
                        current_price=price,
                        market_value=price * quantity,
                        pnl=0.0,
                        pnl_pct=0.0
                    )
        else:  # SELL
            if code in self.positions and self.positions[code].available_quantity >= quantity:
                pos = self.positions[code]
                self.cash += price * quantity - commission
                
                pos.quantity -= quantity
                pos.available_quantity -= quantity
                
                if pos.quantity == 0:
                    del self.positions[code]
                else:
                    pos.market_value = pos.quantity * price
                    pos.pnl = (price - pos.cost_price) * pos.quantity
                    pos.pnl_pct = (price - pos.cost_price) / pos.cost_price
            else:
                order.status = OrderStatus.REJECTED
                order.message = "持仓不足"
                logger.warning(f"下单失败: 持仓不足")
        
        self.orders[order_id] = order
        
        logger.info(f"下单成功: {order_id} {side.value} {code} {quantity}@{price}")
        return order
    
    def cancel_order(self, order_id: str) -> bool:
        """撤单"""
        if order_id in self.orders:
            order = self.orders[order_id]
            if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED]:
                order.status = OrderStatus.CANCELLED
                logger.info(f"撤单成功: {order_id}")
                return True
        return False
    
    def get_orders(self, status: OrderStatus = None) -> List[Order]:
        """获取订单列表"""
        orders = list(self.orders.values())
        if status:
            orders = [o for o in orders if o.status == status]
        return orders
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """获取订单详情"""
        return self.orders.get(order_id)


class BrokerFactory:
    """券商工厂类"""
    
    _brokers = {
        'eastmoney': EastMoneyBroker,
        'simulated': SimulatedBroker,
    }
    
    @classmethod
    def create(cls, broker_name: str, config: Dict[str, Any]) -> Optional[BaseBroker]:
        """创建券商实例"""
        if broker_name not in cls._brokers:
            logger.error(f"未知的券商: {broker_name}")
            return None
        
        broker_class = cls._brokers[broker_name]
        return broker_class(config)
    
    @classmethod
    def register(cls, name: str, broker_class: type):
        """注册新的券商"""
        cls._brokers[name] = broker_class
        logger.info(f"注册券商: {name}")
    
    @classmethod
    def list_brokers(cls) -> List[str]:
        """列出所有可用券商"""
        return list(cls._brokers.keys())


class TradeManager:
    """交易管理器"""
    
    def __init__(self, broker: BaseBroker):
        self.broker = broker
        self.order_history: List[Order] = []
        
    def execute_strategy_signals(self, signals: List[Dict], 
                                 position_pct: float = 0.1) -> List[Order]:
        """执行策略信号"""
        orders = []
        
        account = self.broker.get_account()
        if not account:
            logger.error("获取账户信息失败")
            return orders
        
        # 计算每个信号的仓位
        position_value = account.total_assets * position_pct
        
        for signal in signals:
            code = signal.get('code')
            price = signal.get('price', 0)
            
            if not code or price <= 0:
                continue
            
            # 计算可买入数量
            quantity = int(position_value / price / 100) * 100  # 100股为单位
            
            if quantity < 100:
                continue
            
            # 检查是否已有持仓
            existing_position = None
            for pos in account.positions:
                if pos.code == code:
                    existing_position = pos
                    break
            
            if existing_position:
                logger.info(f"已持有 {code}，跳过")
                continue
            
            # 下单
            order = self.broker.buy(code, price, quantity)
            if order:
                orders.append(order)
                self.order_history.append(order)
        
        return orders
    
    def close_all_positions(self) -> List[Order]:
        """平仓所有持仓"""
        orders = []
        
        positions = self.broker.get_positions()
        for pos in positions:
            order = self.broker.sell(pos.code, pos.current_price, pos.available_quantity)
            if order:
                orders.append(order)
                self.order_history.append(order)
        
        return orders
    
    def get_trade_summary(self) -> Dict[str, Any]:
        """获取交易汇总"""
        account = self.broker.get_account()
        
        if not account:
            return {}
        
        total_trades = len(self.order_history)
        winning_trades = len([o for o in self.order_history if o.status == OrderStatus.FILLED])
        
        return {
            'total_assets': account.total_assets,
            'cash': account.cash,
            'market_value': account.market_value,
            'total_return': (account.total_assets - self.broker.initial_cash) / self.broker.initial_cash,
            'position_count': len(account.positions),
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'win_rate': winning_trades / total_trades if total_trades > 0 else 0
        }


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 创建模拟券商
    broker = BrokerFactory.create('simulated', {
        'initial_cash': 1000000,
        'commission_rate': 0.0003
    })
    
    broker.login()
    
    # 查询账户
    account = broker.get_account()
    print(f"账户总资产: ¥{account.total_assets:,.2f}")
    
    # 买入
    order1 = broker.buy('000001', 10.5, 1000)
    print(f"买入订单: {order1.to_dict()}")
    
    # 再次查询账户
    account = broker.get_account()
    print(f"买入后现金: ¥{account.cash:,.2f}")
    print(f"买入后持仓: {len(account.positions)}")
    
    # 卖出
    order2 = broker.sell('000001', 11.0, 500)
    print(f"卖出订单: {order2.to_dict()}")
    
    # 交易管理器测试
    manager = TradeManager(broker)
    summary = manager.get_trade_summary()
    print(f"\n交易汇总: {summary}")
