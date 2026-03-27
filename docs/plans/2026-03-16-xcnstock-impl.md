# XCNStock 股票分析系统实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建基于第一性原理的A股专业交易系统，集成选股分析、打板策略、智能通知。

**Architecture:** 微服务架构，5个独立服务（Gateway、Data、Stock、Limit、Notify）通过HTTP通信，数据存储使用Parquet格式。

**Tech Stack:** FastAPI, akshare, pandas, numpy, APScheduler, Parquet, httpx

---

## Phase 1: 项目基础设施

### Task 1: 创建项目目录结构

**Files:**
- Create: 目录结构

**Step 1: 创建核心目录**

```bash
mkdir -p gateway\routers
mkdir -p services\data_service\fetchers
mkdir -p services\data_service\processors
mkdir -p services\data_service\storage
mkdir -p services\stock_service\filters
mkdir -p services\limit_service\analyzers
mkdir -p services\notify_service\channels
mkdir -p services\notify_service\templates
mkdir -p core
mkdir -p data\realtime data\kline\daily data\financial data\fundflow data\limitup data\sector data\northbound
mkdir -p logs\system logs\signals logs\trades logs\alerts logs\access
mkdir -p tests
mkdir -p scripts
mkdir -p config
```

**Step 2: 创建 __init__.py 文件**

```bash
type nul > gateway\__init__.py
type nul > gateway\routers\__init__.py
type nul > services\__init__.py
type nul > services\data_service\__init__.py
type nul > services\data_service\fetchers\__init__.py
type nul > services\data_service\processors\__init__.py
type nul > services\data_service\storage\__init__.py
type nul > services\stock_service\__init__.py
type nul > services\stock_service\filters\__init__.py
type nul > services\limit_service\__init__.py
type nul > services\limit_service\analyzers\__init__.py
type nul > services\notify_service\__init__.py
type nul > services\notify_service\channels\__init__.py
type nul > services\notify_service\templates\__init__.py
type nul > core\__init__.py
type nul > tests\__init__.py
```

---

### Task 2: 创建依赖配置文件

**Files:**
- Create: `D:\workstation\xcnstock\requirements.txt`
- Create: `D:\workstation\xcnstock\pyproject.toml`

**Step 1: 创建 requirements.txt**

```text
fastapi>=0.100.0
uvicorn>=0.23.0
httpx>=0.24.0
akshare>=1.12.0
pandas>=2.0.0
numpy>=1.24.0
pyarrow>=14.0.0
apscheduler>=3.10.0
pydantic>=2.0.0
pyyaml>=6.0
python-dotenv>=1.0.0
jinja2>=3.0.0
requests>=2.31.0
pytest>=7.0.0
pytest-asyncio>=0.21.0
```

**Step 2: 创建 pyproject.toml**

```toml
[project]
name = "xcnstock"
version = "0.1.0"
description = "A股专业交易系统 - 选股分析与打板策略"
readme = "README.md"
requires-python = ">=3.10"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py310"
```

---

### Task 3: 创建核心配置模块

**Files:**
- Create: `D:\workstation\xcnstock\core\config.py`
- Test: `D:\workstation\xcnstock\tests\test_config.py`

**Step 1: 写失败测试**

```python
# tests/test_config.py
import pytest
from core.config import Settings, get_settings


def test_settings_default_values():
    """测试配置默认值"""
    settings = Settings()
    assert settings.APP_NAME == "XCNStock"
    assert settings.DEBUG is False


def test_settings_environment_override(monkeypatch):
    """测试环境变量覆盖"""
    monkeypatch.setenv("DEBUG", "true")
    settings = Settings()
    assert settings.DEBUG is True


def test_get_settings_singleton():
    """测试单例模式"""
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_config.py -v
```

Expected: FAIL - ModuleNotFoundError

**Step 3: 实现核心配置**

```python
# core/config.py
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """全局配置"""
    
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
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """获取配置单例"""
    return Settings()
```

**Step 4: 更新 requirements.txt 添加 pydantic-settings**

```text
pydantic-settings>=2.0.0
```

**Step 5: 运行测试验证通过**

```bash
pytest tests/test_config.py -v
```

Expected: PASS

---

### Task 4: 创建日志模块

**Files:**
- Create: `D:\workstation\xcnstock\core\logger.py`
- Test: `D:\workstation\xcnstock\tests\test_logger.py`

**Step 1: 写失败测试**

```python
# tests/test_logger.py
import pytest
import logging
from core.logger import setup_logger, get_signal_logger


def test_setup_logger():
    """测试日志器设置"""
    logger = setup_logger("test_logger")
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO


def test_get_signal_logger():
    """测试信号日志器"""
    logger = get_signal_logger()
    assert logger.name == "signal"
    assert len(logger.handlers) > 0
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_logger.py -v
```

**Step 3: 实现日志模块**

```python
# core/logger.py
import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from core.config import get_settings


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_file: str = None
) -> logging.Logger:
    """设置日志器"""
    settings = get_settings()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 控制台输出
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # 文件输出
    if log_file:
        log_path = Path(settings.LOG_DIR) / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            backupCount=30,
            encoding="utf-8"
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(console_format)
        logger.addHandler(file_handler)
    
    return logger


def get_signal_logger() -> logging.Logger:
    """获取信号专用日志器"""
    settings = get_settings()
    logger = logging.getLogger("signal")
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    # JSON格式文件处理器
    log_path = Path(settings.LOG_DIR) / "signals" / "signals.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        backupCount=180,
        encoding="utf-8"
    )
    json_format = logging.Formatter(
        '{"time": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}'
    )
    file_handler.setFormatter(json_format)
    logger.addHandler(file_handler)
    
    return logger


def get_alert_logger() -> logging.Logger:
    """获取告警日志器"""
    return setup_logger("alert", log_file="alerts/error.log")
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_logger.py -v
```

---

### Task 5: 创建数据模型

**Files:**
- Create: `D:\workstation\xcnstock\core\models.py`
- Test: `D:\workstation\xcnstock\tests\test_models.py`

**Step 1: 写失败测试**

```python
# tests/test_models.py
import pytest
from datetime import datetime
from core.models import StockQuote, LimitUpSignal, SignalLevel


def test_stock_quote_creation():
    """测试股票行情模型"""
    quote = StockQuote(
        code="000001",
        name="平安银行",
        price=10.5,
        change_pct=2.5,
        volume=1000000,
        turnover_rate=5.5
    )
    assert quote.code == "000001"
    assert quote.name == "平安银行"
    assert quote.price == 10.5


def test_limit_up_signal_creation():
    """测试涨停信号模型"""
    signal = LimitUpSignal(
        code="000001",
        name="平安银行",
        change_pct=10.0,
        limit_time="09:30:00",
        seal_amount=100000000,
        signal_level=SignalLevel.S
    )
    assert signal.signal_level == SignalLevel.S
    assert signal.continuous_limit == 1


def test_signal_level_enum():
    """测试信号等级枚举"""
    assert SignalLevel.S.value == "S"
    assert SignalLevel.A.value == "A"
    assert SignalLevel.B.value == "B"
    assert SignalLevel.C.value == "C"
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_models.py -v
```

**Step 3: 实现数据模型**

```python
# core/models.py
from enum import Enum
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class SignalLevel(str, Enum):
    """信号等级"""
    S = "S"  # 最高优先级
    A = "A"  # 高优先级
    B = "B"  # 中等优先级
    C = "C"  # 低优先级


class StockQuote(BaseModel):
    """股票行情"""
    code: str = Field(..., description="股票代码")
    name: str = Field(..., description="股票名称")
    price: float = Field(..., description="当前价格")
    change_pct: float = Field(..., description="涨跌幅%")
    volume: int = Field(default=0, description="成交量")
    turnover_rate: float = Field(default=0, description="换手率%")
    amount: float = Field(default=0, description="成交额")
    high: float = Field(default=0, description="最高价")
    low: float = Field(default=0, description="最低价")
    open: float = Field(default=0, description="开盘价")
    pre_close: float = Field(default=0, description="昨收")
    timestamp: datetime = Field(default_factory=datetime.now, description="时间戳")


class StockScore(BaseModel):
    """股票评分"""
    code: str
    name: str
    total_score: float = Field(..., ge=0, le=100)
    fundamental_score: float = Field(default=0, ge=0, le=100)
    volume_price_score: float = Field(default=0, ge=0, le=100)
    fund_flow_score: float = Field(default=0, ge=0, le=100)
    sentiment_score: float = Field(default=0, ge=0, le=100)
    reasons: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


class LimitUpSignal(BaseModel):
    """涨停信号"""
    code: str
    name: str
    change_pct: float
    limit_time: str = Field(..., description="涨停时间")
    seal_amount: float = Field(..., description="封单金额")
    seal_ratio: float = Field(default=0, description="封单/流通市值比")
    continuous_limit: int = Field(default=1, description="连板数")
    open_count: int = Field(default=0, description="开板次数")
    reasons: List[str] = Field(default_factory=list)
    signal_level: SignalLevel
    next_day_predict: str = Field(default="", description="次日预判")
    suggestion: str = Field(default="", description="操作建议")
    timestamp: datetime = Field(default_factory=datetime.now)


class StockSelectionSignal(BaseModel):
    """选股信号"""
    code: str
    name: str
    score: StockScore
    current_price: float
    change_pct: float
    signal_type: str = Field(default="选股", description="信号类型")
    signal_level: SignalLevel
    reasons: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


class NotificationMessage(BaseModel):
    """通知消息"""
    title: str
    content: str
    level: SignalLevel
    channels: List[str] = Field(default_factory=lambda: ["wechat", "dingtalk"])
    timestamp: datetime = Field(default_factory=datetime.now)
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_models.py -v
```

---

## Phase 2: 数据服务 (Data Service)

### Task 6: 创建行情数据获取器

**Files:**
- Create: `D:\workstation\xcnstock\services\data_service\fetchers\quote.py`
- Test: `D:\workstation\xcnstock\tests\test_quote_fetcher.py`

**Step 1: 写失败测试**

```python
# tests/test_quote_fetcher.py
import pytest
from unittest.mock import patch, MagicMock
from services.data_service.fetchers.quote import QuoteFetcher


def test_quote_fetcher_init():
    """测试行情获取器初始化"""
    fetcher = QuoteFetcher()
    assert fetcher is not None


@pytest.mark.asyncio
async def test_fetch_realtime_quotes():
    """测试获取实时行情"""
    fetcher = QuoteFetcher()
    
    with patch('akshare.stock_zh_a_spot_em') as mock_akshare:
        mock_akshare.return_value = MagicMock()
        mock_akshare.return_value.iterrows.return_value = []
        
        result = await fetcher.fetch_realtime_quotes()
        assert isinstance(result, list)


@pytest.mark.asyncio
async def test_fetch_kline():
    """测试获取K线数据"""
    fetcher = QuoteFetcher()
    
    with patch('akshare.stock_zh_a_hist') as mock_akshare:
        mock_akshare.return_value = MagicMock()
        result = await fetcher.fetch_kline("000001")
        assert result is not None
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_quote_fetcher.py -v
```

**Step 3: 实现行情获取器**

```python
# services/data_service/fetchers/quote.py
import akshare as ak
import pandas as pd
from typing import List, Optional
from datetime import datetime
import logging

from core.models import StockQuote
from core.logger import setup_logger

logger = setup_logger("quote_fetcher", log_file="system/quote.log")


class QuoteFetcher:
    """行情数据获取器"""
    
    def __init__(self):
        self._cache = {}
    
    async def fetch_realtime_quotes(self) -> List[StockQuote]:
        """
        获取A股实时行情
        使用 akshare.stock_zh_a_spot_em 接口
        """
        try:
            logger.info("开始获取实时行情数据")
            df = ak.stock_zh_a_spot_em()
            
            quotes = []
            for _, row in df.iterrows():
                try:
                    quote = StockQuote(
                        code=str(row.get("代码", "")),
                        name=str(row.get("名称", "")),
                        price=float(row.get("最新价", 0) or 0),
                        change_pct=float(row.get("涨跌幅", 0) or 0),
                        volume=int(row.get("成交量", 0) or 0),
                        turnover_rate=float(row.get("换手率", 0) or 0),
                        amount=float(row.get("成交额", 0) or 0),
                        high=float(row.get("最高", 0) or 0),
                        low=float(row.get("最低", 0) or 0),
                        open=float(row.get("今开", 0) or 0),
                        pre_close=float(row.get("昨收", 0) or 0),
                    )
                    quotes.append(quote)
                except Exception as e:
                    logger.warning(f"解析行情数据失败: {row.get('代码')}, {e}")
                    continue
            
            logger.info(f"获取实时行情完成，共 {len(quotes)} 条")
            return quotes
            
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            return []
    
    async def fetch_kline(
        self, 
        code: str, 
        period: str = "daily",
        start_date: str = None,
        end_date: str = None
    ) -> Optional[pd.DataFrame]:
        """
        获取K线数据
        
        Args:
            code: 股票代码
            period: 周期 daily/weekly/monthly
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        try:
            logger.info(f"获取K线数据: {code}")
            
            if not start_date:
                start_date = "20200101"
            if not end_date:
                end_date = datetime.now().strftime("%Y%m%d")
            
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            if df is not None and not df.empty:
                df.columns = ["date", "open", "close", "high", "low", "volume", "amount", "amplitude", "change_pct", "change_amt", "turnover"]
                logger.info(f"获取K线数据成功: {code}, {len(df)} 条")
            
            return df
            
        except Exception as e:
            logger.error(f"获取K线数据失败: {code}, {e}")
            return None
    
    async def fetch_quote_by_code(self, code: str) -> Optional[StockQuote]:
        """获取单只股票行情"""
        quotes = await self.fetch_realtime_quotes()
        for quote in quotes:
            if quote.code == code:
                return quote
        return None
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_quote_fetcher.py -v
```

---

### Task 7: 创建涨停数据获取器

**Files:**
- Create: `D:\workstation\xcnstock\services\data_service\fetchers\limitup.py`
- Test: `D:\workstation\xcnstock\tests\test_limitup_fetcher.py`

**Step 1: 写失败测试**

```python
# tests/test_limitup_fetcher.py
import pytest
from unittest.mock import patch, MagicMock
from services.data_service.fetchers.limitup import LimitUpFetcher


def test_limitup_fetcher_init():
    """测试涨停获取器初始化"""
    fetcher = LimitUpFetcher()
    assert fetcher is not None


@pytest.mark.asyncio
async def test_fetch_limit_up_pool():
    """测试获取涨停池"""
    fetcher = LimitUpFetcher()
    
    with patch('akshare.stock_zt_pool_em') as mock_akshare:
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.iterrows.return_value = []
        mock_akshare.return_value = mock_df
        
        result = await fetcher.fetch_limit_up_pool()
        assert isinstance(result, list)
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_limitup_fetcher.py -v
```

**Step 3: 实现涨停获取器**

```python
# services/data_service/fetchers/limitup.py
import akshare as ak
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime
import logging

from core.logger import setup_logger

logger = setup_logger("limitup_fetcher", log_file="system/limitup.log")


class LimitUpStock:
    """涨停股票数据"""
    def __init__(
        self,
        code: str,
        name: str,
        change_pct: float,
        limit_time: str,
        seal_amount: float,
        open_count: int,
        continuous_limit: int,
        sector: str = ""
    ):
        self.code = code
        self.name = name
        self.change_pct = change_pct
        self.limit_time = limit_time
        self.seal_amount = seal_amount  # 封单金额(万)
        self.open_count = open_count  # 开板次数
        self.continuous_limit = continuous_limit  # 连板数
        self.sector = sector


class LimitUpFetcher:
    """涨停数据获取器"""
    
    async def fetch_limit_up_pool(self, date: str = None) -> List[LimitUpStock]:
        """
        获取涨停池数据
        
        Args:
            date: 日期 YYYYMMDD，默认今天
        """
        try:
            if not date:
                date = datetime.now().strftime("%Y%m%d")
            
            logger.info(f"获取涨停池数据: {date}")
            df = ak.stock_zt_pool_em(date=date)
            
            if df is None or df.empty:
                logger.warning(f"涨停池数据为空: {date}")
                return []
            
            stocks = []
            for _, row in df.iterrows():
                try:
                    stock = LimitUpStock(
                        code=str(row.get("代码", "")),
                        name=str(row.get("名称", "")),
                        change_pct=float(row.get("涨跌幅", 0) or 0),
                        limit_time=str(row.get("涨停时间", "")),
                        seal_amount=float(row.get("封单资金", 0) or 0) / 10000,  # 转万
                        open_count=int(row.get("开板次数", 0) or 0),
                        continuous_limit=int(row.get("连板数", 1) or 1),
                        sector=str(row.get("所属行业", ""))
                    )
                    stocks.append(stock)
                except Exception as e:
                    logger.warning(f"解析涨停数据失败: {row.get('代码')}, {e}")
                    continue
            
            logger.info(f"获取涨停池完成: {len(stocks)} 只")
            return stocks
            
        except Exception as e:
            logger.error(f"获取涨停池失败: {e}")
            return []
    
    async def fetch_limit_up_strong(self, date: str = None) -> List[LimitUpStock]:
        """获取强势涨停股（首板封板早、封单大）"""
        stocks = await self.fetch_limit_up_pool(date)
        
        strong_stocks = [
            s for s in stocks
            if s.open_count == 0 
            and s.limit_time < "10:00:00"
            and s.seal_amount > 5000  # 封单>5000万
        ]
        
        logger.info(f"强势涨停股: {len(strong_stocks)} 只")
        return strong_stocks
    
    async def fetch_continuous_limit_up(self, min_boards: int = 2, date: str = None) -> List[LimitUpStock]:
        """获取连板股"""
        stocks = await self.fetch_limit_up_pool(date)
        
        continuous = [s for s in stocks if s.continuous_limit >= min_boards]
        logger.info(f"{min_boards}连板以上: {len(continuous)} 只")
        return continuous
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_limitup_fetcher.py -v
```

---

### Task 8: 创建数据存储管理器

**Files:**
- Create: `D:\workstation\xcnstock\services\data_service\storage\parquet_manager.py`
- Test: `D:\workstation\xcnstock\tests\test_parquet_manager.py`

**Step 1: 写失败测试**

```python
# tests/test_parquet_manager.py
import pytest
import pandas as pd
from pathlib import Path
from services.data_service.storage.parquet_manager import ParquetManager


def test_parquet_manager_init():
    """测试存储管理器初始化"""
    manager = ParquetManager()
    assert manager.data_dir is not None


def test_save_and_read_parquet(tmp_path):
    """测试保存和读取Parquet"""
    manager = ParquetManager(data_dir=str(tmp_path))
    
    df = pd.DataFrame({
        "code": ["000001", "000002"],
        "name": ["平安银行", "万科A"],
        "price": [10.5, 15.2]
    })
    
    # 保存
    manager.save(df, "test/quotes.parquet")
    
    # 读取
    result = manager.read("test/quotes.parquet")
    assert len(result) == 2
    assert result.iloc[0]["code"] == "000001"


def test_append_parquet(tmp_path):
    """测试追加数据"""
    manager = ParquetManager(data_dir=str(tmp_path))
    
    df1 = pd.DataFrame({"code": ["000001"], "price": [10.0]})
    df2 = pd.DataFrame({"code": ["000002"], "price": [15.0]})
    
    manager.save(df1, "test/quotes.parquet")
    manager.append(df2, "test/quotes.parquet")
    
    result = manager.read("test/quotes.parquet")
    assert len(result) == 2
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_parquet_manager.py -v
```

**Step 3: 实现存储管理器**

```python
# services/data_service/storage/parquet_manager.py
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from typing import Optional
from datetime import datetime
import logging

from core.config import get_settings
from core.logger import setup_logger

logger = setup_logger("parquet_manager", log_file="system/storage.log")


class ParquetManager:
    """Parquet存储管理器"""
    
    def __init__(self, data_dir: str = None):
        settings = get_settings()
        self.data_dir = Path(data_dir or settings.DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def save(self, df: pd.DataFrame, relative_path: str) -> bool:
        """
        保存DataFrame到Parquet文件
        
        Args:
            df: DataFrame数据
            relative_path: 相对于data_dir的路径
        """
        try:
            file_path = self.data_dir / relative_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            df.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"保存数据成功: {file_path}, {len(df)} 条")
            return True
            
        except Exception as e:
            logger.error(f"保存数据失败: {relative_path}, {e}")
            return False
    
    def append(self, df: pd.DataFrame, relative_path: str) -> bool:
        """
        追加数据到Parquet文件
        
        Args:
            df: 要追加的DataFrame
            relative_path: 文件路径
        """
        try:
            file_path = self.data_dir / relative_path
            
            if file_path.exists():
                existing_df = pd.read_parquet(file_path)
                df = pd.concat([existing_df, df], ignore_index=True)
            
            return self.save(df, relative_path)
            
        except Exception as e:
            logger.error(f"追加数据失败: {relative_path}, {e}")
            return False
    
    def read(self, relative_path: str) -> Optional[pd.DataFrame]:
        """
        读取Parquet文件
        
        Args:
            relative_path: 文件路径
        Returns:
            DataFrame 或 None
        """
        try:
            file_path = self.data_dir / relative_path
            
            if not file_path.exists():
                logger.warning(f"文件不存在: {file_path}")
                return None
            
            df = pd.read_parquet(file_path)
            logger.info(f"读取数据成功: {file_path}, {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"读取数据失败: {relative_path}, {e}")
            return None
    
    def save_daily_data(
        self, 
        df: pd.DataFrame, 
        data_type: str,
        date: str = None
    ) -> bool:
        """
        按日期保存数据
        
        Args:
            df: 数据
            data_type: 数据类型 (kline, limitup, fundflow等)
            date: 日期 YYYYMMDD
        """
        if not date:
            date = datetime.now().strftime("%Y%m%d")
        
        path = f"{data_type}/{date}.parquet"
        return self.save(df, path)
    
    def get_latest_data(self, data_type: str) -> Optional[pd.DataFrame]:
        """获取最新一天的数据"""
        type_dir = self.data_dir / data_type
        
        if not type_dir.exists():
            return None
        
        files = sorted(type_dir.glob("*.parquet"), reverse=True)
        
        if not files:
            return None
        
        return pd.read_parquet(files[0])
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_parquet_manager.py -v
```

---

### Task 9: 创建数据服务主程序

**Files:**
- Create: `D:\workstation\xcnstock\services\data_service\main.py`
- Create: `D:\workstation\xcnstock\services\data_service\scheduler.py`

**Step 1: 实现调度器**

```python
# services/data_service/scheduler.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import logging

from core.config import get_settings
from core.logger import setup_logger
from services.data_service.fetchers.quote import QuoteFetcher
from services.data_service.fetchers.limitup import LimitUpFetcher
from services.data_service.storage.parquet_manager import ParquetManager

logger = setup_logger("scheduler", log_file="system/scheduler.log")


class DataScheduler:
    """数据调度器"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.settings = get_settings()
        self.quote_fetcher = QuoteFetcher()
        self.limitup_fetcher = LimitUpFetcher()
        self.storage = ParquetManager()
    
    async def job_realtime_quotes(self):
        """定时任务：获取实时行情"""
        logger.info("执行定时任务：获取实时行情")
        try:
            quotes = await self.quote_fetcher.fetch_realtime_quotes()
            if quotes:
                import pandas as pd
                df = pd.DataFrame([q.model_dump() for q in quotes])
                self.storage.save(df, f"realtime/{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        except Exception as e:
            logger.error(f"实时行情任务失败: {e}")
    
    async def job_limit_up_pool(self):
        """定时任务：获取涨停池"""
        logger.info("执行定时任务：获取涨停池")
        try:
            stocks = await self.limitup_fetcher.fetch_limit_up_pool()
            if stocks:
                import pandas as pd
                df = pd.DataFrame([{
                    "code": s.code,
                    "name": s.name,
                    "change_pct": s.change_pct,
                    "limit_time": s.limit_time,
                    "seal_amount": s.seal_amount,
                    "open_count": s.open_count,
                    "continuous_limit": s.continuous_limit,
                    "sector": s.sector
                } for s in stocks])
                self.storage.save_daily_data(df, "limitup")
        except Exception as e:
            logger.error(f"涨停池任务失败: {e}")
    
    async def job_daily_kline(self):
        """定时任务：获取日K线"""
        logger.info("执行定时任务：获取日K线")
        # 获取所有股票代码，逐个获取K线
        # 简化版本：只获取涨停股的K线
        try:
            limit_stocks = await self.limitup_fetcher.fetch_limit_up_pool()
            for stock in limit_stocks[:50]:  # 限制数量
                df = await self.quote_fetcher.fetch_kline(stock.code)
                if df is not None:
                    self.storage.save(df, f"kline/daily/{stock.code}.parquet")
        except Exception as e:
            logger.error(f"日K线任务失败: {e}")
    
    def setup_jobs(self):
        """配置定时任务"""
        # 盘中实时行情（每分钟）
        self.scheduler.add_job(
            self.job_realtime_quotes,
            CronTrigger(
                day_of_week="mon-fri",
                hour="9-11,13-15",
                minute="*/1"
            ),
            id="realtime_quotes",
            replace_existing=True
        )
        
        # 涨停池监控（盘中每分钟）
        self.scheduler.add_job(
            self.job_limit_up_pool,
            CronTrigger(
                day_of_week="mon-fri",
                hour="9-11,13-15",
                minute="*/1"
            ),
            id="limit_up_pool",
            replace_existing=True
        )
        
        # 日K线（每日16:00）
        self.scheduler.add_job(
            self.job_daily_kline,
            CronTrigger(
                day_of_week="mon-fri",
                hour=16,
                minute=0
            ),
            id="daily_kline",
            replace_existing=True
        )
        
        logger.info("定时任务配置完成")
    
    def start(self):
        """启动调度器"""
        if self.settings.SCHEDULE_ENABLED:
            self.setup_jobs()
            self.scheduler.start()
            logger.info("数据调度器已启动")
        else:
            logger.info("调度器未启用")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        logger.info("数据调度器已停止")
```

**Step 2: 实现主程序**

```python
# services/data_service/main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from services.data_service.scheduler import DataScheduler
from services.data_service.fetchers.quote import QuoteFetcher
from services.data_service.fetchers.limitup import LimitUpFetcher

settings = get_settings()
logger = setup_logger("data_service", log_file="system/data_service.log")

# 全局实例
data_scheduler = DataScheduler()
quote_fetcher = QuoteFetcher()
limitup_fetcher = LimitUpFetcher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期"""
    logger.info("数据服务启动中...")
    data_scheduler.start()
    yield
    logger.info("数据服务关闭中...")
    data_scheduler.stop()


app = FastAPI(
    title="XCNStock Data Service",
    description="A股数据服务 - 行情获取与存储",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "data_service"}


@app.get("/api/v1/quote/realtime")
async def get_realtime_quotes():
    """获取实时行情"""
    quotes = await quote_fetcher.fetch_realtime_quotes()
    return {"count": len(quotes), "data": [q.model_dump() for q in quotes[:100]]}


@app.get("/api/v1/quote/kline/{code}")
async def get_kline(code: str):
    """获取K线数据"""
    df = await quote_fetcher.fetch_kline(code)
    if df is not None:
        return {"code": code, "count": len(df), "data": df.to_dict(orient="records")[-100:]}
    return {"code": code, "count": 0, "data": []}


@app.get("/api/v1/limitup/today")
async def get_today_limit_up():
    """获取今日涨停池"""
    stocks = await limitup_fetcher.fetch_limit_up_pool()
    return {
        "count": len(stocks),
        "data": [{
            "code": s.code,
            "name": s.name,
            "change_pct": s.change_pct,
            "limit_time": s.limit_time,
            "seal_amount": s.seal_amount,
            "continuous_limit": s.continuous_limit
        } for s in stocks]
    }


@app.get("/api/v1/scheduler/jobs")
async def get_scheduler_jobs():
    """获取调度任务状态"""
    jobs = data_scheduler.scheduler.get_jobs()
    return {
        "jobs": [{
            "id": job.id,
            "next_run": str(job.next_run_time)
        } for job in jobs]
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.DATA_SERVICE_PORT,
        reload=True
    )
```

---

## Phase 3: 选股服务 (Stock Service)

### Task 10: 创建基本面筛选器

**Files:**
- Create: `D:\workstation\xcnstock\services\stock_service\filters\fundamental.py`
- Test: `D:\workstation\xcnstock\tests\test_fundamental_filter.py`

**Step 1: 写失败测试**

```python
# tests/test_fundamental_filter.py
import pytest
from services.stock_service.filters.fundamental import FundamentalFilter


def test_fundamental_filter_init():
    """测试基本面筛选器初始化"""
    f = FundamentalFilter()
    assert f is not None


def test_fundamental_filter_score():
    """测试基本面评分"""
    f = FundamentalFilter()
    
    # 优质股票数据
    good_data = {
        "pe": 20.0,
        "pb": 3.0,
        "roe": 15.0,
        "revenue_growth": 20.0,
        "profit_growth": 25.0,
        "debt_ratio": 40.0
    }
    
    score = f.calculate_score(good_data)
    assert 0 <= score <= 100
    assert score > 60  # 优质股应该得分高


def test_fundamental_filter_pass():
    """测试筛选通过条件"""
    f = FundamentalFilter()
    
    good_data = {
        "pe": 25.0,
        "pb": 2.5,
        "roe": 18.0,
        "revenue_growth": 25.0,
        "profit_growth": 30.0,
        "debt_ratio": 35.0
    }
    
    result = f.filter(good_data)
    assert result["passed"] is True
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_fundamental_filter.py -v
```

**Step 3: 实现基本面筛选器**

```python
# services/stock_service/filters/fundamental.py
from typing import Dict, List
import logging

from core.logger import setup_logger

logger = setup_logger("fundamental_filter")


class FundamentalFilter:
    """基本面筛选器
    
    基于第一性原理：股票内在价值 = 盈利能力 + 成长性 + 财务健康
    """
    
    # 筛选条件配置
    CONDITIONS = {
        "pe": {"min": 0, "max": 50, "weight": 0.15},      # 市盈率
        "pb": {"min": 0, "max": 10, "weight": 0.10},      # 市净率
        "roe": {"min": 10, "max": 100, "weight": 0.20},   # ROE
        "revenue_growth": {"min": 15, "max": 200, "weight": 0.15},  # 营收增长
        "profit_growth": {"min": 10, "max": 200, "weight": 0.20},   # 利润增长
        "debt_ratio": {"min": 0, "max": 60, "weight": 0.20}  # 负债率
    }
    
    def calculate_score(self, data: Dict) -> float:
        """
        计算基本面评分
        
        Args:
            data: 包含 pe, pb, roe, revenue_growth, profit_growth, debt_ratio
        Returns:
            评分 0-100
        """
        total_score = 0.0
        total_weight = 0.0
        
        for metric, config in self.CONDITIONS.items():
            value = data.get(metric)
            if value is None:
                continue
            
            weight = config["weight"]
            min_val = config["min"]
            max_val = config["max"]
            
            # 计算该项得分（0-100）
            if metric == "debt_ratio":
                # 负债率越低越好
                if value <= min_val:
                    item_score = 100
                elif value >= max_val:
                    item_score = 0
                else:
                    item_score = 100 - (value - min_val) / (max_val - min_val) * 100
            elif metric in ["pe", "pb"]:
                # PE/PB合理区间得分高
                mid = (min_val + max_val) / 2
                if min_val < value < max_val:
                    # 越接近中间值越好
                    item_score = 100 - abs(value - mid) / mid * 50
                else:
                    item_score = 0
            else:
                # ROE、增长率等越高越好
                if value >= max_val:
                    item_score = 100
                elif value <= min_val:
                    item_score = 0
                else:
                    item_score = (value - min_val) / (max_val - min_val) * 100
            
            total_score += item_score * weight
            total_weight += weight
        
        if total_weight > 0:
            return round(total_score / total_weight, 2)
        return 0.0
    
    def filter(self, data: Dict) -> Dict:
        """
        执行筛选
        
        Returns:
            {
                "passed": bool,
                "score": float,
                "reasons": List[str]
            }
        """
        reasons = []
        passed = True
        
        # 检查各项条件
        if data.get("pe", 999) <= 0 or data.get("pe", 999) > 50:
            reasons.append(f"PE={data.get('pe')}不在合理区间(0,50)")
            passed = False
        
        if data.get("roe", 0) < 10:
            reasons.append(f"ROE={data.get('roe')}低于10%")
            passed = False
        
        if data.get("debt_ratio", 999) > 60:
            reasons.append(f"负债率={data.get('debt_ratio')}超过60%")
            passed = False
        
        if data.get("revenue_growth", 0) < 15:
            reasons.append(f"营收增长={data.get('revenue_growth')}低于15%")
        
        score = self.calculate_score(data)
        
        if score >= 70:
            reasons.append("基本面优秀")
        elif score >= 50:
            reasons.append("基本面良好")
        
        return {
            "passed": passed,
            "score": score,
            "reasons": reasons
        }
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_fundamental_filter.py -v
```

---

### Task 11: 创建量价筛选器

**Files:**
- Create: `D:\workstation\xcnstock\services\stock_service\filters\volume_price.py`
- Test: `D:\workstation\xcnstock\tests\test_volume_price_filter.py`

**Step 1: 写失败测试**

```python
# tests/test_volume_price_filter.py
import pytest
import pandas as pd
from services.stock_service.filters.volume_price import VolumePriceFilter


def test_volume_price_filter_init():
    """测试量价筛选器初始化"""
    f = VolumePriceFilter()
    assert f is not None


def test_calculate_rsi():
    """测试RSI计算"""
    f = VolumePriceFilter()
    
    prices = pd.Series([10, 10.5, 11, 10.8, 11.2, 11.5, 11.3, 11.8, 12, 11.9, 
                        12.2, 12.5, 12.3, 12.8, 13])
    rsi = f.calculate_rsi(prices)
    
    assert 0 <= rsi <= 100


def test_calculate_macd():
    """测试MACD计算"""
    f = VolumePriceFilter()
    
    prices = pd.Series([10, 10.5, 11, 10.8, 11.2, 11.5, 11.3, 11.8, 12, 11.9,
                        12.2, 12.5, 12.3, 12.8, 13, 13.2, 13.5, 13.3, 13.8, 14])
    macd, signal, hist = f.calculate_macd(prices)
    
    assert macd is not None
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_volume_price_filter.py -v
```

**Step 3: 实现量价筛选器**

```python
# services/stock_service/filters/volume_price.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging

from core.logger import setup_logger

logger = setup_logger("volume_price_filter")


class VolumePriceFilter:
    """量价筛选器
    
    基于第一性原理：价格趋势 + 成交量确认 = 有效突破
    """
    
    def calculate_ma(self, prices: pd.Series, period: int) -> pd.Series:
        """计算移动平均线"""
        return prices.rolling(window=period).mean()
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """计算RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.iloc[-1] if not rsi.empty else 50
    
    def calculate_macd(self, prices: pd.Series) -> tuple:
        """计算MACD"""
        ema12 = prices.ewm(span=12, adjust=False).mean()
        ema26 = prices.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal
        
        return macd.iloc[-1], signal.iloc[-1], histogram.iloc[-1]
    
    def calculate_bollinger(self, prices: pd.Series, period: int = 20) -> tuple:
        """计算布林带"""
        mid = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        upper = mid + 2 * std
        lower = mid - 2 * std
        
        return upper.iloc[-1], mid.iloc[-1], lower.iloc[-1]
    
    def calculate_score(self, kline_df: pd.DataFrame) -> float:
        """
        计算量价评分
        
        Args:
            kline_df: K线数据，包含 close, volume 列
        Returns:
            评分 0-100
        """
        if kline_df is None or len(kline_df) < 30:
            return 0.0
        
        try:
            close = kline_df['close']
            volume = kline_df['volume']
            
            scores = {}
            
            # 1. 成交量突破 (30%)
            ma5_vol = self.calculate_ma(volume, 5)
            vol_ratio = volume.iloc[-1] / ma5_vol.iloc[-1] if ma5_vol.iloc[-1] > 0 else 0
            if vol_ratio > 2:
                scores['volume'] = 100
            elif vol_ratio > 1.5:
                scores['volume'] = 80
            elif vol_ratio > 1:
                scores['volume'] = 60
            else:
                scores['volume'] = 30
            
            # 2. 价格突破 (25%)
            ma20 = self.calculate_ma(close, 20)
            if close.iloc[-1] > ma20.iloc[-1] * 1.05:
                scores['price'] = 100
            elif close.iloc[-1] > ma20.iloc[-1]:
                scores['price'] = 70
            else:
                scores['price'] = 30
            
            # 3. RSI (20%)
            rsi = self.calculate_rsi(close)
            if 40 <= rsi <= 70:
                scores['rsi'] = 100
            elif 30 <= rsi <= 80:
                scores['rsi'] = 70
            else:
                scores['rsi'] = 40
            
            # 4. MACD (15%)
            macd, signal, hist = self.calculate_macd(close)
            if hist > 0 and macd > signal:
                scores['macd'] = 100
            elif macd > signal:
                scores['macd'] = 70
            else:
                scores['macd'] = 30
            
            # 5. 布林带 (10%)
            upper, mid, lower = self.calculate_bollinger(close)
            if close.iloc[-1] > mid:
                scores['bollinger'] = 100
            elif close.iloc[-1] > lower:
                scores['bollinger'] = 60
            else:
                scores['bollinger'] = 30
            
            # 加权平均
            weights = {
                'volume': 0.30,
                'price': 0.25,
                'rsi': 0.20,
                'macd': 0.15,
                'bollinger': 0.10
            }
            
            total_score = sum(scores[k] * weights[k] for k in scores)
            return round(total_score, 2)
            
        except Exception as e:
            logger.error(f"计算量价评分失败: {e}")
            return 0.0
    
    def filter(self, kline_df: pd.DataFrame) -> Dict:
        """执行量价筛选"""
        score = self.calculate_score(kline_df)
        reasons = []
        
        if kline_df is None or len(kline_df) < 30:
            return {"passed": False, "score": 0, "reasons": ["数据不足"]}
        
        close = kline_df['close']
        volume = kline_df['volume']
        
        # 检查突破
        ma20 = self.calculate_ma(close, 20)
        if close.iloc[-1] > ma20.iloc[-1]:
            reasons.append("突破20日均线")
        
        # 检查量能
        ma5_vol = self.calculate_ma(volume, 5)
        vol_ratio = volume.iloc[-1] / ma5_vol.iloc[-1] if ma5_vol.iloc[-1] > 0 else 0
        if vol_ratio > 1.5:
            reasons.append(f"放量{vol_ratio:.1f}倍")
        
        # 检查MACD
        macd, signal, _ = self.calculate_macd(close)
        if macd > signal:
            reasons.append("MACD金叉")
        
        passed = score >= 60
        
        return {
            "passed": passed,
            "score": score,
            "reasons": reasons
        }
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_volume_price_filter.py -v
```

---

### Task 12: 创建选股引擎

**Files:**
- Create: `D:\workstation\xcnstock\services\stock_service\engine.py`
- Create: `D:\workstation\xcnstock\services\stock_service\scorer.py`

**Step 1: 实现评分器**

```python
# services/stock_service/scorer.py
from typing import Dict
from core.models import StockScore


class StockScorer:
    """股票综合评分器
    
    基于第一性原理：
    股票价值 = 内在价值(35%) + 技术面(25%) + 资金面(25%) + 情绪面(15%)
    """
    
    # 维度权重
    WEIGHTS = {
        "fundamental": 0.35,
        "volume_price": 0.25,
        "fund_flow": 0.25,
        "sentiment": 0.15
    }
    
    def calculate_total_score(
        self,
        fundamental_score: float,
        volume_price_score: float,
        fund_flow_score: float,
        sentiment_score: float
    ) -> float:
        """计算综合评分"""
        total = (
            fundamental_score * self.WEIGHTS["fundamental"] +
            volume_price_score * self.WEIGHTS["volume_price"] +
            fund_flow_score * self.WEIGHTS["fund_flow"] +
            sentiment_score * self.WEIGHTS["sentiment"]
        )
        return round(total, 2)
    
    def create_score(
        self,
        code: str,
        name: str,
        fundamental_score: float = 0,
        volume_price_score: float = 0,
        fund_flow_score: float = 0,
        sentiment_score: float = 0,
        reasons: list = None
    ) -> StockScore:
        """创建股票评分对象"""
        total = self.calculate_total_score(
            fundamental_score,
            volume_price_score,
            fund_flow_score,
            sentiment_score
        )
        
        return StockScore(
            code=code,
            name=name,
            total_score=total,
            fundamental_score=fundamental_score,
            volume_price_score=volume_price_score,
            fund_flow_score=fund_flow_score,
            sentiment_score=sentiment_score,
            reasons=reasons or []
        )
```

**Step 2: 实现选股引擎**

```python
# services/stock_service/engine.py
import asyncio
from typing import List, Dict
import logging

from core.models import StockScore, StockSelectionSignal, SignalLevel
from core.logger import setup_logger
from services.stock_service.filters.fundamental import FundamentalFilter
from services.stock_service.filters.volume_price import VolumePriceFilter
from services.stock_service.scorer import StockScorer

logger = setup_logger("stock_engine", log_file="signals/stock_engine.log")


class StockSelectionEngine:
    """选股引擎"""
    
    def __init__(self):
        self.fundamental_filter = FundamentalFilter()
        self.volume_price_filter = VolumePriceFilter()
        self.scorer = StockScorer()
    
    async def analyze_stock(
        self,
        code: str,
        name: str,
        fundamental_data: Dict,
        kline_data,
        fund_flow_data: Dict,
        sentiment_data: Dict
    ) -> StockSelectionSignal:
        """
        分析单只股票
        
        Returns:
            选股信号
        """
        # 基本面筛选
        fund_result = self.fundamental_filter.filter(fundamental_data)
        fundamental_score = fund_result["score"]
        reasons = fund_result["reasons"]
        
        # 量价筛选
        vp_result = self.volume_price_filter.filter(kline_data)
        volume_price_score = vp_result["score"]
        reasons.extend(vp_result["reasons"])
        
        # 资金流向评分（简化）
        fund_flow_score = self._calculate_fund_flow_score(fund_flow_data)
        
        # 情绪评分（简化）
        sentiment_score = self._calculate_sentiment_score(sentiment_data)
        
        # 综合评分
        score = self.scorer.create_score(
            code=code,
            name=name,
            fundamental_score=fundamental_score,
            volume_price_score=volume_price_score,
            fund_flow_score=fund_flow_score,
            sentiment_score=sentiment_score,
            reasons=reasons
        )
        
        # 确定信号等级
        signal_level = self._determine_signal_level(score.total_score)
        
        return StockSelectionSignal(
            code=code,
            name=name,
            score=score,
            current_price=fundamental_data.get("price", 0),
            change_pct=fundamental_data.get("change_pct", 0),
            signal_level=signal_level,
            reasons=reasons
        )
    
    def _calculate_fund_flow_score(self, data: Dict) -> float:
        """计算资金流向评分"""
        if not data:
            return 50.0
        
        score = 50.0
        
        # 主力净流入
        if data.get("main_net_inflow", 0) > 0:
            score += 20
        
        # 北向资金
        if data.get("north_bound_days", 0) >= 3:
            score += 15
        
        # 大单净比
        if data.get("big_order_ratio", 0) > 0:
            score += 15
        
        return min(score, 100)
    
    def _calculate_sentiment_score(self, data: Dict) -> float:
        """计算情绪评分"""
        if not data:
            return 50.0
        
        score = 50.0
        
        # 板块热度
        if data.get("sector_rank", 999) <= 5:
            score += 20
        
        # 换手率
        turnover = data.get("turnover_rate", 0)
        if 3 <= turnover <= 15:
            score += 15
        
        # 市场情绪
        if data.get("market_up", False):
            score += 15
        
        return min(score, 100)
    
    def _determine_signal_level(self, total_score: float) -> SignalLevel:
        """确定信号等级"""
        if total_score >= 80:
            return SignalLevel.S
        elif total_score >= 70:
            return SignalLevel.A
        elif total_score >= 60:
            return SignalLevel.B
        else:
            return SignalLevel.C
    
    async def screen_stocks(
        self,
        stock_list: List[Dict],
        min_score: float = 60.0
    ) -> List[StockSelectionSignal]:
        """
        批量筛选股票
        
        Args:
            stock_list: 股票列表，每个包含 code, name, fundamental_data 等
            min_score: 最低评分阈值
        Returns:
            符合条件的选股信号列表
        """
        results = []
        
        for stock in stock_list:
            try:
                signal = await self.analyze_stock(
                    code=stock["code"],
                    name=stock["name"],
                    fundamental_data=stock.get("fundamental", {}),
                    kline_data=stock.get("kline"),
                    fund_flow_data=stock.get("fund_flow", {}),
                    sentiment_data=stock.get("sentiment", {})
                )
                
                if signal.score.total_score >= min_score:
                    results.append(signal)
                    
            except Exception as e:
                logger.error(f"分析股票失败: {stock.get('code')}, {e}")
        
        # 按评分排序
        results.sort(key=lambda x: x.score.total_score, reverse=True)
        
        logger.info(f"筛选完成: 输入{len(stock_list)}只, 输出{len(results)}只")
        return results
```

---

### Task 13: 创建选股服务主程序

**Files:**
- Create: `D:\workstation\xcnstock\services\stock_service\main.py`

**Step 1: 实现主程序**

```python
# services/stock_service/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from core.models import StockSelectionSignal
from services.stock_service.engine import StockSelectionEngine

settings = get_settings()
logger = setup_logger("stock_service", log_file="system/stock_service.log")

# 全局引擎实例
engine = StockSelectionEngine()

app = FastAPI(
    title="XCNStock Stock Service",
    description="A股选股服务 - 多维度筛选与评分",
    version="0.1.0"
)


class ScreenRequest(BaseModel):
    """选股请求"""
    codes: List[str]
    min_score: float = 60.0


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "stock_service"}


@app.post("/api/v1/stock/screen")
async def screen_stocks(request: ScreenRequest):
    """执行选股筛选"""
    # 简化版本：模拟数据
    # 实际应从 Data Service 获取数据
    mock_stock_list = [
        {
            "code": code,
            "name": f"股票{code}",
            "fundamental": {"pe": 20, "pb": 2, "roe": 15, "revenue_growth": 20, "profit_growth": 25, "debt_ratio": 40},
            "kline": None,
            "fund_flow": {"main_net_inflow": 1000000},
            "sentiment": {"turnover_rate": 5}
        }
        for code in request.codes[:10]  # 限制数量
    ]
    
    results = await engine.screen_stocks(mock_stock_list, request.min_score)
    
    return {
        "count": len(results),
        "data": [r.model_dump() for r in results]
    }


@app.get("/api/v1/stock/score/{code}")
async def get_stock_score(code: str):
    """获取个股评分"""
    # 模拟数据
    signal = await engine.analyze_stock(
        code=code,
        name=f"股票{code}",
        fundamental_data={"pe": 20, "pb": 2, "roe": 15, "revenue_growth": 20, "profit_growth": 25, "debt_ratio": 40},
        kline_data=None,
        fund_flow_data={"main_net_inflow": 1000000},
        sentiment_data={"turnover_rate": 5}
    )
    
    return signal.model_dump()


@app.get("/api/v1/stock/rank")
async def get_stock_rank(top: int = 20):
    """获取选股排行榜"""
    # 模拟数据
    return {
        "count": 0,
        "data": []
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.STOCK_SERVICE_PORT,
        reload=True
    )
```

---

## Phase 4: 打板服务 (Limit Service)

### Task 14: 创建涨停预判分析器

**Files:**
- Create: `D:\workstation\xcnstock\services\limit_service\analyzers\pre_limit.py`
- Test: `D:\workstation\xcnstock\tests\test_pre_limit.py`

**Step 1: 写失败测试**

```python
# tests/test_pre_limit.py
import pytest
from services.limit_service.analyzers.pre_limit import PreLimitPredictor


def test_pre_limit_predictor_init():
    """测试涨停预判器初始化"""
    predictor = PreLimitPredictor()
    assert predictor is not None


def test_predict_limit_probability():
    """测试涨停概率预测"""
    predictor = PreLimitPredictor()
    
    stock_data = {
        "change_pct": 8.5,
        "volume_ratio": 2.5,
        "turnover_rate": 8.0,
        "sector_change": 3.0,
        "sector_limit_count": 5
    }
    
    result = predictor.predict(stock_data)
    assert "probability" in result
    assert 0 <= result["probability"] <= 100
    assert "factors" in result
```

**Step 2: 运行测试验证失败**

```bash
pytest tests/test_pre_limit.py -v
```

**Step 3: 实现涨停预判器**

```python
# services/limit_service/analyzers/pre_limit.py
from typing import Dict
import logging

from core.logger import setup_logger

logger = setup_logger("pre_limit_predictor")


class PreLimitPredictor:
    """涨停预判器
    
    基于第一性原理：涨停 = 供需失衡 + 情绪共振
    """
    
    # 因子权重
    FACTOR_WEIGHTS = {
        "price_momentum": 0.30,     # 价格动能
        "volume_energy": 0.25,      # 成交量能
        "seal_strength": 0.25,      # 封单强度（已涨停）
        "sector_effect": 0.20       # 板块效应
    }
    
    def predict(self, stock_data: Dict) -> Dict:
        """
        预测涨停概率
        
        Args:
            stock_data: {
                "change_pct": 涨幅,
                "volume_ratio": 量比,
                "turnover_rate": 换手率,
                "seal_amount": 封单金额（已涨停）,
                "seal_ratio": 封单/流通市值比,
                "sector_change": 板块涨幅,
                "sector_limit_count": 板块内涨停数
            }
        Returns:
            {
                "probability": 概率(0-100),
                "factors": 各因子得分,
                "prediction": 预判结果
            }
        """
        factors = {}
        
        # 1. 价格动能 (30%)
        change_pct = stock_data.get("change_pct", 0)
        distance_to_limit = max(0, 10 - change_pct) / 10  # 距涨停距离
        
        if change_pct >= 9.9:
            factors["price_momentum"] = 100  # 已涨停
        elif change_pct >= 7:
            factors["price_momentum"] = 80 + (change_pct - 7) * 10
        elif change_pct >= 5:
            factors["price_momentum"] = 50 + (change_pct - 5) * 15
        else:
            factors["price_momentum"] = change_pct * 10
        
        # 2. 成交量能 (25%)
        volume_ratio = stock_data.get("volume_ratio", 1)
        turnover_rate = stock_data.get("turnover_rate", 0)
        
        volume_score = min(volume_ratio * 30, 80)  # 量比得分
        turnover_score = 0
        if 3 <= turnover_rate <= 10:
            turnover_score = 100  # 理想换手率
        elif turnover_rate > 10:
            turnover_score = max(0, 100 - (turnover_rate - 10) * 5)  # 换手太高反而不好
        
        factors["volume_energy"] = (volume_score + turnover_score) / 2
        
        # 3. 封单强度 (25%) - 仅对已涨停股票
        seal_amount = stock_data.get("seal_amount", 0)
        seal_ratio = stock_data.get("seal_ratio", 0)
        
        if seal_amount > 0:
            # 封单/流通市值比
            if seal_ratio >= 5:
                factors["seal_strength"] = 100
            elif seal_ratio >= 2:
                factors["seal_strength"] = 80
            elif seal_ratio >= 1:
                factors["seal_strength"] = 60
            else:
                factors["seal_strength"] = 40
        else:
            factors["seal_strength"] = 50  # 未涨停，中性
        
        # 4. 板块效应 (20%)
        sector_change = stock_data.get("sector_change", 0)
        sector_limit_count = stock_data.get("sector_limit_count", 0)
        
        sector_score = min(50 + sector_change * 10, 100)
        limit_bonus = min(sector_limit_count * 5, 30)
        factors["sector_effect"] = min(sector_score + limit_bonus, 100)
        
        # 计算综合概率
        probability = sum(
            factors[k] * self.FACTOR_WEIGHTS[k] 
            for k in self.FACTOR_WEIGHTS
        )
        
        # 预判结果
        if probability >= 80:
            prediction = "极高"
        elif probability >= 60:
            prediction = "较高"
        elif probability >= 40:
            prediction = "中等"
        else:
            prediction = "较低"
        
        return {
            "probability": round(probability, 2),
            "factors": factors,
            "prediction": prediction
        }
```

**Step 4: 运行测试验证通过**

```bash
pytest tests/test_pre_limit.py -v
```

---

### Task 15: 创建打板引擎和主程序

**Files:**
- Create: `D:\workstation\xcnstock\services\limit_service\engine.py`
- Create: `D:\workstation\xcnstock\services\limit_service\main.py`

**Step 1: 实现打板引擎**

```python
# services/limit_service/engine.py
from typing import List, Dict
import logging

from core.models import LimitUpSignal, SignalLevel
from core.logger import setup_logger, get_signal_logger
from services.limit_service.analyzers.pre_limit import PreLimitPredictor

logger = setup_logger("limit_engine", log_file="signals/limit_engine.log")
signal_logger = get_signal_logger()


class LimitUpEngine:
    """打板引擎"""
    
    def __init__(self):
        self.pre_limit_predictor = PreLimitPredictor()
    
    async def analyze_limit_stock(self, stock_data: Dict) -> LimitUpSignal:
        """分析涨停股票"""
        code = stock_data["code"]
        name = stock_data["name"]
        
        # 预判
        prediction = self.pre_limit_predictor.predict(stock_data)
        
        # 评估封板强度
        seal_strength = self._evaluate_seal_strength(stock_data)
        
        # 分析涨停原因
        reasons = self._analyze_reasons(stock_data)
        
        # 次日预判
        next_day_predict = self._predict_next_day(stock_data, seal_strength)
        
        # 确定信号等级
        signal_level = self._determine_signal_level(stock_data, seal_strength)
        
        # 操作建议
        suggestion = self._get_suggestion(signal_level, seal_strength)
        
        signal = LimitUpSignal(
            code=code,
            name=name,
            change_pct=stock_data.get("change_pct", 10),
            limit_time=stock_data.get("limit_time", ""),
            seal_amount=stock_data.get("seal_amount", 0),
            seal_ratio=stock_data.get("seal_ratio", 0),
            continuous_limit=stock_data.get("continuous_limit", 1),
            open_count=stock_data.get("open_count", 0),
            reasons=reasons,
            signal_level=signal_level,
            next_day_predict=next_day_predict,
            suggestion=suggestion
        )
        
        # 记录信号日志
        signal_logger.info(signal.model_dump_json())
        
        return signal
    
    def _evaluate_seal_strength(self, data: Dict) -> str:
        """评估封板强度"""
        seal_ratio = data.get("seal_ratio", 0)
        limit_time = data.get("limit_time", "15:00:00")
        open_count = data.get("open_count", 0)
        
        # 封单金额/流通市值
        if seal_ratio >= 5:
            seal_score = "强"
        elif seal_ratio >= 2:
            seal_score = "中强"
        elif seal_ratio >= 1:
            seal_score = "中"
        else:
            seal_score = "弱"
        
        # 封板时间
        if limit_time < "10:00:00":
            time_score = "早"
        elif limit_time < "14:00:00":
            time_score = "中"
        else:
            time_score = "晚"
        
        # 开板次数
        if open_count == 0:
            open_score = "无"
        elif open_count <= 2:
            open_score = "少"
        else:
            open_score = "多"
        
        # 综合评估
        if seal_score in ["强", "中强"] and time_score == "早" and open_score == "无":
            return "强势封板"
        elif seal_score in ["强", "中强", "中"] and open_score in ["无", "少"]:
            return "中等封板"
        else:
            return "弱势封板"
    
    def _analyze_reasons(self, data: Dict) -> List[str]:
        """分析涨停原因"""
        reasons = []
        
        sector = data.get("sector", "")
        if sector:
            reasons.append(f"板块: {sector}")
        
        continuous = data.get("continuous_limit", 1)
        if continuous > 1:
            reasons.append(f"{continuous}连板")
        
        limit_time = data.get("limit_time", "")
        if limit_time < "10:00:00":
            reasons.append("早盘快速封板")
        
        return reasons
    
    def _predict_next_day(self, data: Dict, seal_strength: str) -> str:
        """次日预判"""
        continuous = data.get("continuous_limit", 1)
        
        if seal_strength == "强势封板":
            if continuous == 1:
                return "高开涨停概率70%"
            else:
                return "高开加速概率65%"
        elif seal_strength == "中等封板":
            if continuous == 1:
                return "高开冲高概率60%"
            else:
                return "高开分歧概率55%"
        else:
            return "平开震荡概率50%"
    
    def _determine_signal_level(self, data: Dict, seal_strength: str) -> SignalLevel:
        """确定信号等级"""
        continuous = data.get("continuous_limit", 1)
        limit_time = data.get("limit_time", "15:00:00")
        
        # S级：龙头 + 首板 + 强封 + 早盘
        if continuous == 1 and seal_strength == "强势封板" and limit_time < "10:00:00":
            return SignalLevel.S
        
        # A级：龙头/跟风 + 首板 + 中强封
        if continuous <= 2 and seal_strength in ["强势封板", "中等封板"]:
            return SignalLevel.A
        
        # B级：跟风 + 连板 + 中等封板
        if seal_strength == "中等封板":
            return SignalLevel.B
        
        return SignalLevel.C
    
    def _get_suggestion(self, level: SignalLevel, seal_strength: str) -> str:
        """获取操作建议"""
        suggestions = {
            SignalLevel.S: "重点关注，竞价可参与",
            SignalLevel.A: "关注，竞价观望，确认后可参与",
            SignalLevel.B: "观望，等分歧转一致",
            SignalLevel.C: "谨慎，风险较高"
        }
        return suggestions.get(level, "观望")
    
    async def get_pre_limit_stocks(self, quotes: List[Dict]) -> List[Dict]:
        """获取涨停预判股票池（涨幅7%以上）"""
        results = []
        
        for quote in quotes:
            change_pct = quote.get("change_pct", 0)
            if 7 <= change_pct < 10:
                prediction = self.pre_limit_predictor.predict(quote)
                if prediction["probability"] >= 50:
                    results.append({
                        **quote,
                        "prediction": prediction
                    })
        
        # 按概率排序
        results.sort(key=lambda x: x["prediction"]["probability"], reverse=True)
        
        return results
```

**Step 2: 实现打板服务主程序**

```python
# services/limit_service/main.py
from fastapi import FastAPI
from typing import List
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from core.models import LimitUpSignal
from services.limit_service.engine import LimitUpEngine

settings = get_settings()
logger = setup_logger("limit_service", log_file="system/limit_service.log")

# 全局引擎实例
engine = LimitUpEngine()

app = FastAPI(
    title="XCNStock Limit Service",
    description="A股打板服务 - 涨停监控与分析",
    version="0.1.0"
)


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "limit_service"}


@app.get("/api/v1/limit/pre-limit")
async def get_pre_limit_stocks():
    """获取涨停预判股票池"""
    # 模拟数据
    mock_quotes = [
        {"code": "000001", "name": "平安银行", "change_pct": 8.5, "volume_ratio": 2.5, "turnover_rate": 6, "sector_change": 2, "sector_limit_count": 3}
    ]
    
    results = await engine.get_pre_limit_stocks(mock_quotes)
    return {"count": len(results), "data": results}


@app.get("/api/v1/limit/today")
async def get_today_limit_up():
    """今日涨停板监控"""
    # 模拟数据
    mock_limit_stocks = [
        {
            "code": "000001",
            "name": "平安银行",
            "change_pct": 10.0,
            "limit_time": "09:35:00",
            "seal_amount": 50000,
            "seal_ratio": 3.5,
            "continuous_limit": 1,
            "open_count": 0,
            "sector": "银行"
        }
    ]
    
    results = []
    for stock in mock_limit_stocks:
        signal = await engine.analyze_limit_stock(stock)
        results.append(signal.model_dump())
    
    return {"count": len(results), "data": results}


@app.get("/api/v1/limit/{code}/analysis")
async def get_limit_analysis(code: str):
    """个股涨停分析"""
    mock_data = {
        "code": code,
        "name": f"股票{code}",
        "change_pct": 10.0,
        "limit_time": "09:45:00",
        "seal_amount": 30000,
        "seal_ratio": 2.5,
        "continuous_limit": 1,
        "open_count": 0,
        "sector": "科技"
    }
    
    signal = await engine.analyze_limit_stock(mock_data)
    return signal.model_dump()


@app.get("/api/v1/limit/{code}/predict")
async def get_next_day_predict(code: str):
    """次日预判"""
    mock_data = {
        "code": code,
        "name": f"股票{code}",
        "continuous_limit": 1,
        "limit_time": "09:45:00",
        "seal_amount": 30000,
        "seal_ratio": 2.5,
        "open_count": 0
    }
    
    signal = await engine.analyze_limit_stock(mock_data)
    return {
        "code": code,
        "next_day_predict": signal.next_day_predict,
        "suggestion": signal.suggestion
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.LIMIT_SERVICE_PORT,
        reload=True
    )
```

---

## Phase 5: 通知服务 (Notify Service)

### Task 16: 创建通知渠道实现

**Files:**
- Create: `D:\workstation\xcnstock\services\notify_service\channels\base.py`
- Create: `D:\workstation\xcnstock\services\notify_service\channels\wechat.py`
- Create: `D:\workstation\xcnstock\services\notify_service\channels\dingtalk.py`
- Create: `D:\workstation\xcnstock\services\notify_service\channels\email_channel.py`

**Step 1: 实现基类**

```python
# services/notify_service/channels/base.py
from abc import ABC, abstractmethod
from typing import Dict, Optional
from core.models import NotificationMessage


class BaseChannel(ABC):
    """通知渠道基类"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.enabled = self.config.get("enabled", False)
    
    @abstractmethod
    async def send(self, message: NotificationMessage) -> bool:
        """发送通知"""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """渠道名称"""
        pass
```

**Step 2: 实现微信渠道**

```python
# services/notify_service/channels/wechat.py
import httpx
from typing import Optional, Dict
import logging

from core.models import NotificationMessage
from core.config import get_settings
from core.logger import setup_logger
from .base import BaseChannel

logger = setup_logger("wechat_channel")


class WechatChannel(BaseChannel):
    """微信通知渠道（Server酱）"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        settings = get_settings()
        self.send_key = self.config.get("send_key") or settings.WECHAT_SEND_KEY
    
    @property
    def name(self) -> str:
        return "wechat"
    
    async def send(self, message: NotificationMessage) -> bool:
        """发送微信通知"""
        if not self.enabled or not self.send_key:
            logger.warning("微信通知未配置或未启用")
            return False
        
        url = f"https://sctapi.ftqq.com/{self.send_key}.send"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    data={
                        "title": message.title,
                        "desp": message.content
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("code") == 0:
                        logger.info(f"微信通知发送成功: {message.title}")
                        return True
                    else:
                        logger.error(f"微信通知发送失败: {result}")
                else:
                    logger.error(f"微信通知请求失败: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"微信通知发送异常: {e}")
        
        return False
```

**Step 3: 实现钉钉渠道**

```python
# services/notify_service/channels/dingtalk.py
import httpx
import hmac
import hashlib
import base64
import time
from urllib.parse import quote_plus
from typing import Optional, Dict
import logging

from core.models import NotificationMessage
from core.config import get_settings
from core.logger import setup_logger
from .base import BaseChannel

logger = setup_logger("dingtalk_channel")


class DingtalkChannel(BaseChannel):
    """钉钉通知渠道"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        settings = get_settings()
        self.webhook = self.config.get("webhook") or settings.DINGTALK_WEBHOOK
        self.secret = self.config.get("secret") or settings.DINGTALK_SECRET
    
    @property
    def name(self) -> str:
        return "dingtalk"
    
    def _sign(self) -> str:
        """生成签名"""
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = quote_plus(base64.b64encode(hmac_code))
        return f"&timestamp={timestamp}&sign={sign}"
    
    async def send(self, message: NotificationMessage) -> bool:
        """发送钉钉通知"""
        if not self.enabled or not self.webhook:
            logger.warning("钉钉通知未配置或未启用")
            return False
        
        url = self.webhook
        if self.secret:
            url += self._sign()
        
        # 钉钉markdown格式
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": message.title,
                "text": message.content
            }
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    json=data,
                    timeout=10
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("errcode") == 0:
                        logger.info(f"钉钉通知发送成功: {message.title}")
                        return True
                    else:
                        logger.error(f"钉钉通知发送失败: {result}")
                else:
                    logger.error(f"钉钉通知请求失败: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"钉钉通知发送异常: {e}")
        
        return False
```

**Step 4: 实现邮件渠道**

```python
# services/notify_service/channels/email_channel.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, List
import asyncio
import logging

from core.models import NotificationMessage
from core.config import get_settings
from core.logger import setup_logger
from .base import BaseChannel

logger = setup_logger("email_channel")


class EmailChannel(BaseChannel):
    """邮件通知渠道"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        settings = get_settings()
        self.host = self.config.get("host") or settings.SMTP_HOST
        self.port = self.config.get("port") or settings.SMTP_PORT
        self.user = self.config.get("user") or settings.SMTP_USER
        self.password = self.config.get("password") or settings.SMTP_PASSWORD
        
        recipients = self.config.get("recipients") or settings.EMAIL_RECIPIENTS
        self.recipients: List[str] = recipients.split(",") if recipients else []
    
    @property
    def name(self) -> str:
        return "email"
    
    async def send(self, message: NotificationMessage) -> bool:
        """发送邮件通知"""
        if not self.enabled or not self.host:
            logger.warning("邮件通知未配置或未启用")
            return False
        
        if not self.recipients:
            logger.warning("邮件收件人未配置")
            return False
        
        try:
            # 在线程池中执行同步邮件发送
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._send_email_sync,
                message
            )
            logger.info(f"邮件通知发送成功: {message.title}")
            return True
            
        except Exception as e:
            logger.error(f"邮件通知发送异常: {e}")
            return False
    
    def _send_email_sync(self, message: NotificationMessage):
        """同步发送邮件"""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[XCNStock] {message.title}"
        msg["From"] = self.user
        msg["To"] = ", ".join(self.recipients)
        
        # 纯文本内容
        text_part = MIMEText(message.content, "plain", "utf-8")
        msg.attach(text_part)
        
        # HTML内容
        html_content = f"""
        <html>
        <body>
        <h2>{message.title}</h2>
        <pre>{message.content}</pre>
        <hr>
        <p><small>XCNStock 自动通知 - {message.timestamp}</small></p>
        </body>
        </html>
        """
        html_part = MIMEText(html_content, "html", "utf-8")
        msg.attach(html_part)
        
        # 发送
        with smtplib.SMTP_SSL(self.host, self.port) as server:
            server.login(self.user, self.password)
            server.sendmail(self.user, self.recipients, msg.as_string())
```

---

### Task 17: 创建信号中心和通知服务主程序

**Files:**
- Create: `D:\workstation\xcnstock\services\notify_service\signal_hub.py`
- Create: `D:\workstation\xcnstock\services\notify_service\main.py`

**Step 1: 实现信号中心**

```python
# services/notify_service/signal_hub.py
import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import logging

from core.models import NotificationMessage, SignalLevel, LimitUpSignal, StockSelectionSignal
from core.config import get_settings
from core.logger import setup_logger
from services.notify_service.channels.base import BaseChannel
from services.notify_service.channels.wechat import WechatChannel
from services.notify_service.channels.dingtalk import DingtalkChannel
from services.notify_service.channels.email_channel import EmailChannel

logger = setup_logger("signal_hub", log_file="signals/notify.log")


class SignalHub:
    """信号中心 - 信号聚合与通知分发"""
    
    # 通知策略配置
    NOTIFY_STRATEGY = {
        SignalLevel.S: {
            "channels": ["wechat", "dingtalk", "email"],
            "immediate": True
        },
        SignalLevel.A: {
            "channels": ["wechat", "dingtalk", "email"],
            "immediate": True
        },
        SignalLevel.B: {
            "channels": ["wechat", "dingtalk"],
            "immediate": False,
            "batch_interval": 900  # 15分钟
        },
        SignalLevel.C: {
            "channels": ["wechat"],
            "immediate": False,
            "batch_interval": 3600  # 1小时
        }
    }
    
    # 频率限制
    RATE_LIMITS = {
        "same_stock_interval": 300,  # 同一股票5分钟
        "same_level_daily_max": 50
    }
    
    def __init__(self):
        self.channels: Dict[str, BaseChannel] = {}
        self._last_notify_time: Dict[str, datetime] = defaultdict(lambda: datetime.min)
        self._daily_count: Dict[str, int] = defaultdict(int)
        
        self._init_channels()
    
    def _init_channels(self):
        """初始化通知渠道"""
        settings = get_settings()
        
        # 微信
        if settings.WECHAT_SEND_KEY:
            self.channels["wechat"] = WechatChannel({
                "enabled": True,
                "send_key": settings.WECHAT_SEND_KEY
            })
        
        # 钉钉
        if settings.DINGTALK_WEBHOOK:
            self.channels["dingtalk"] = DingtalkChannel({
                "enabled": True,
                "webhook": settings.DINGTALK_WEBHOOK,
                "secret": settings.DINGTALK_SECRET
            })
        
        # 邮件
        if settings.SMTP_HOST:
            self.channels["email"] = EmailChannel({
                "enabled": True,
                "host": settings.SMTP_HOST,
                "port": settings.SMTP_PORT,
                "user": settings.SMTP_USER,
                "password": settings.SMTP_PASSWORD,
                "recipients": settings.EMAIL_RECIPIENTS
            })
        
        logger.info(f"已初始化通知渠道: {list(self.channels.keys())}")
    
    async def emit_limit_up_signal(self, signal: LimitUpSignal):
        """发送涨停信号"""
        message = self._create_limit_up_message(signal)
        await self._dispatch(message, signal.signal_level, signal.code)
    
    async def emit_stock_selection_signal(self, signal: StockSelectionSignal):
        """发送选股信号"""
        message = self._create_stock_selection_message(signal)
        await self._dispatch(message, signal.signal_level, signal.code)
    
    def _create_limit_up_message(self, signal: LimitUpSignal) -> NotificationMessage:
        """创建涨停通知消息"""
        level_emoji = {
            SignalLevel.S: "🚨",
            SignalLevel.A: "⚠️",
            SignalLevel.B: "📢",
            SignalLevel.C: "📋"
        }
        
        title = f"{level_emoji.get(signal.signal_level, '')}【打板信号-{signal.signal_level.value}级】{signal.name}"
        
        content = f"""
**股票**: {signal.name} ({signal.code})
**涨幅**: {signal.change_pct}%
**封单**: {signal.seal_amount:.0f}万
**封板时间**: {signal.limit_time}
**连板**: {signal.continuous_limit}板

**涨停原因**:
{chr(10).join(f'- {r}' for r in signal.reasons)}

**次日预判**: {signal.next_day_predict}
**操作建议**: {signal.suggestion}

📅 {signal.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        return NotificationMessage(
            title=title,
            content=content,
            level=signal.signal_level
        )
    
    def _create_stock_selection_message(self, signal: StockSelectionSignal) -> NotificationMessage:
        """创建选股通知消息"""
        title = f"🎯【选股信号-{signal.signal_level.value}级】{signal.name}"
        
        content = f"""
**股票**: {signal.name} ({signal.code})
**综合评分**: {signal.score.total_score}分

**四维评分**:
- 基本面: {signal.score.fundamental_score}分
- 量价: {signal.score.volume_price_score}分
- 资金: {signal.score.fund_flow_score}分
- 情绪: {signal.score.sentiment_score}分

**筛选理由**:
{chr(10).join(f'- {r}' for r in signal.reasons)}

📅 {signal.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        return NotificationMessage(
            title=title,
            content=content,
            level=signal.signal_level
        )
    
    async def _dispatch(
        self,
        message: NotificationMessage,
        level: SignalLevel,
        stock_code: str
    ):
        """分发通知"""
        # 检查频率限制
        if not self._check_rate_limit(stock_code, level):
            logger.info(f"通知频率限制: {stock_code}")
            return
        
        strategy = self.NOTIFY_STRATEGY.get(level, self.NOTIFY_STRATEGY[SignalLevel.C])
        channels = strategy["channels"]
        
        # 并发发送
        tasks = []
        for channel_name in channels:
            channel = self.channels.get(channel_name)
            if channel and channel.enabled:
                tasks.append(channel.send(message))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success = sum(1 for r in results if r is True)
            logger.info(f"通知发送完成: {success}/{len(tasks)} 成功")
    
    def _check_rate_limit(self, stock_code: str, level: SignalLevel) -> bool:
        """检查频率限制"""
        now = datetime.now()
        
        # 检查同一股票间隔
        last_time = self._last_notify_time.get(stock_code)
        interval = (now - last_time).total_seconds()
        
        if interval < self.RATE_LIMITS["same_stock_interval"]:
            return False
        
        # 检查每日上限
        level_key = f"{level.value}_{now.strftime('%Y%m%d')}"
        if self._daily_count[level_key] >= self.RATE_LIMITS["same_level_daily_max"]:
            return False
        
        # 更新记录
        self._last_notify_time[stock_code] = now
        self._daily_count[level_key] += 1
        
        return True
```

**Step 2: 实现通知服务主程序**

```python
# services/notify_service/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from core.models import LimitUpSignal, StockSelectionSignal, SignalLevel
from services.notify_service.signal_hub import SignalHub

settings = get_settings()
logger = setup_logger("notify_service", log_file="system/notify_service.log")

# 全局信号中心
signal_hub = SignalHub()

app = FastAPI(
    title="XCNStock Notify Service",
    description="A股通知服务 - 多渠道推送",
    version="0.1.0"
)


class NotifyRequest(BaseModel):
    """通知请求"""
    title: str
    content: str
    level: str = "B"
    channels: List[str] = ["wechat"]


@app.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "ok",
        "service": "notify_service",
        "channels": list(signal_hub.channels.keys())
    }


@app.post("/api/v1/notify/send")
async def send_notification(request: NotifyRequest):
    """发送通知"""
    from core.models import NotificationMessage
    
    message = NotificationMessage(
        title=request.title,
        content=request.content,
        level=SignalLevel(request.level),
        channels=request.channels
    )
    
    # 直接发送到指定渠道
    results = {}
    for channel_name in request.channels:
        channel = signal_hub.channels.get(channel_name)
        if channel:
            results[channel_name] = await channel.send(message)
        else:
            results[channel_name] = False
    
    return {"results": results}


@app.get("/api/v1/notify/channels")
async def get_channels():
    """获取通知渠道列表"""
    return {
        "channels": [
            {
                "name": name,
                "enabled": channel.enabled
            }
            for name, channel in signal_hub.channels.items()
        ]
    }


@app.post("/api/v1/notify/limit-up")
async def notify_limit_up(signal: LimitUpSignal):
    """发送涨停信号通知"""
    await signal_hub.emit_limit_up_signal(signal)
    return {"status": "ok"}


@app.post("/api/v1/notify/stock-selection")
async def notify_stock_selection(signal: StockSelectionSignal):
    """发送选股信号通知"""
    await signal_hub.emit_stock_selection_signal(signal)
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.NOTIFY_SERVICE_PORT,
        reload=True
    )
```

---

## Phase 6: API Gateway

### Task 18: 创建API网关

**Files:**
- Create: `D:\workstation\xcnstock\gateway\main.py`
- Create: `D:\workstation\xcnstock\gateway\routers\proxy.py`

**Step 1: 实现代理路由**

```python
# gateway/routers/proxy.py
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
import httpx
from typing import Optional

from core.config import get_settings

settings = get_settings()
router = APIRouter()

# 服务地址映射
SERVICE_URLS = {
    "data": f"http://127.0.0.1:{settings.DATA_SERVICE_PORT}",
    "stock": f"http://127.0.0.1:{settings.STOCK_SERVICE_PORT}",
    "limit": f"http://127.0.0.1:{settings.LIMIT_SERVICE_PORT}",
    "notify": f"http://127.0.0.1:{settings.NOTIFY_SERVICE_PORT}"
}


async def proxy_request(
    service: str,
    path: str,
    request: Request
) -> StreamingResponse:
    """通用代理请求"""
    if service not in SERVICE_URLS:
        raise HTTPException(status_code=404, detail=f"Service {service} not found")
    
    url = f"{SERVICE_URLS[service]}{path}"
    
    async with httpx.AsyncClient() as client:
        proxy_req = client.build_request(
            method=request.method,
            url=url,
            headers=dict(request.headers),
            content=await request.body()
        )
        
        response = await client.send(proxy_req)
        
        return StreamingResponse(
            response.aiter_bytes(),
            status_code=response.status_code,
            headers=dict(response.headers)
        )


@router.api_route("/data/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_data_service(path: str, request: Request):
    """代理数据服务"""
    return await proxy_request("data", f"/api/v1/{path}", request)


@router.api_route("/stock/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_stock_service(path: str, request: Request):
    """代理选股服务"""
    return await proxy_request("stock", f"/api/v1/{path}", request)


@router.api_route("/limit/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_limit_service(path: str, request: Request):
    """代理打板服务"""
    return await proxy_request("limit", f"/api/v1/{path}", request)


@router.api_route("/notify/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_notify_service(path: str, request: Request):
    """代理通知服务"""
    return await proxy_request("notify", f"/api/v1/{path}", request)
```

**Step 2: 实现网关主程序**

```python
# gateway/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from gateway.routers.proxy import router as proxy_router

settings = get_settings()
logger = setup_logger("gateway", log_file="system/gateway.log")

app = FastAPI(
    title="XCNStock API Gateway",
    description="A股交易系统API网关",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(proxy_router, prefix="/api/v1")


@app.get("/")
async def root():
    """根路径"""
    return {
        "name": "XCNStock API Gateway",
        "version": "0.1.0",
        "services": {
            "data": f"http://127.0.0.1:{settings.DATA_SERVICE_PORT}",
            "stock": f"http://127.0.0.1:{settings.STOCK_SERVICE_PORT}",
            "limit": f"http://127.0.0.1:{settings.LIMIT_SERVICE_PORT}",
            "notify": f"http://127.0.0.1:{settings.NOTIFY_SERVICE_PORT}"
        }
    }


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "gateway"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.GATEWAY_PORT,
        reload=True
    )
```

---

## Phase 7: 启动脚本与配置

### Task 19: 创建启动脚本

**Files:**
- Create: `D:\workstation\xcnstock\scripts\run_all_services.py`
- Create: `D:\workstation\xcnstock\config\settings.yaml`

**Step 1: 创建启动脚本**

```python
# scripts/run_all_services.py
import subprocess
import sys
import time
import signal
from pathlib import Path

# 项目根目录
ROOT_DIR = Path(__file__).parent.parent

# 服务配置
SERVICES = [
    {"name": "data_service", "path": "services/data_service/main.py", "port": 8001},
    {"name": "stock_service", "path": "services/stock_service/main.py", "port": 8002},
    {"name": "limit_service", "path": "services/limit_service/main.py", "port": 8003},
    {"name": "notify_service", "path": "services/notify_service/main.py", "port": 8004},
    {"name": "gateway", "path": "gateway/main.py", "port": 8000},
]

processes = []


def start_services():
    """启动所有服务"""
    print("=" * 50)
    print("XCNStock 服务启动中...")
    print("=" * 50)
    
    for service in SERVICES:
        print(f"\n启动 {service['name']} (端口 {service['port']})...")
        
        process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", 
             f"{service['path'].replace('/', '.').replace('.py', '')}:app",
             "--host", "0.0.0.0",
             "--port", str(service['port'])],
            cwd=ROOT_DIR
        )
        processes.append(process)
        time.sleep(1)
    
    print("\n" + "=" * 50)
    print("所有服务已启动！")
    print("Gateway: http://127.0.0.1:8000")
    print("API文档: http://127.0.0.1:8000/docs")
    print("=" * 50)


def stop_services(signum=None, frame=None):
    """停止所有服务"""
    print("\n\n正在停止所有服务...")
    for process in processes:
        process.terminate()
    
    for process in processes:
        process.wait()
    
    print("所有服务已停止")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_services)
    signal.signal(signal.SIGTERM, stop_services)
    
    start_services()
    
    # 保持运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_services()
```

**Step 2: 创建配置文件**

```yaml
# config/settings.yaml
app:
  name: XCNStock
  version: 0.1.0
  debug: false

services:
  gateway:
    port: 8000
  data_service:
    port: 8001
  stock_service:
    port: 8002
  limit_service:
    port: 8003
  notify_service:
    port: 8004

data:
  dir: data
  formats:
    - parquet
    - csv

logging:
  dir: logs
  level: INFO
  retention:
    system: 30
    signals: 180
    trades: 365

schedule:
  enabled: true
  timezone: Asia/Shanghai

notification:
  rate_limits:
    same_stock_interval: 300
    same_level_daily_max: 50
    batch_interval: 900
  quiet_hours: [23, 0, 1, 2, 3, 4, 5, 6, 7]
```

---

### Task 20: 创建环境变量模板

**Files:**
- Create: `D:\workstation\xcnstock\.env.example`

```bash
# .env.example
# 应用配置
DEBUG=false

# 服务端口
GATEWAY_PORT=8000
DATA_SERVICE_PORT=8001
STOCK_SERVICE_PORT=8002
LIMIT_SERVICE_PORT=8003
NOTIFY_SERVICE_PORT=8004

# 数据路径
DATA_DIR=data
LOG_DIR=logs

# 微信通知（Server酱）
WECHAT_SEND_KEY=your_send_key_here

# 钉钉通知
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx
DINGTALK_SECRET=your_secret_here

# 邮件通知
SMTP_HOST=smtp.qq.com
SMTP_PORT=465
SMTP_USER=your_email@qq.com
SMTP_PASSWORD=your_authorization_code
EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com

# 调度配置
SCHEDULE_ENABLED=true
```

---

## 实施计划总结

### 执行顺序

1. **Phase 1**: 项目基础设施 (Task 1-5)
2. **Phase 2**: 数据服务 (Task 6-9)
3. **Phase 3**: 选股服务 (Task 10-13)
4. **Phase 4**: 打板服务 (Task 14-15)
5. **Phase 5**: 通知服务 (Task 16-17)
6. **Phase 6**: API Gateway (Task 18)
7. **Phase 7**: 启动脚本与配置 (Task 19-20)

### 测试命令

```bash
# 运行所有测试
pytest tests/ -v

# 运行单个测试文件
pytest tests/test_config.py -v

# 运行覆盖率
pytest tests/ --cov=. --cov-report=html
```

### 启动命令

```bash
# 安装依赖
pip install -r requirements.txt

# 复制环境变量配置
copy .env.example .env

# 启动所有服务
python scripts/run_all_services.py

# 或单独启动
python -m uvicorn gateway.main:app --port 8000
python -m uvicorn services.data_service.main:app --port 8001
python -m uvicorn services.stock_service.main:app --port 8002
python -m uvicorn services.limit_service.main:app --port 8003
python -m uvicorn services.notify_service.main:app --port 8004
```

---

**计划创建日期**: 2026-03-16
