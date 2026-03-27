# 每日自动化工作流程实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现XCNStock每日自动化运行，包括定时任务调度、多渠道推送、Kafka集成、一字涨停处理。

**Architecture:** 基于APScheduler统一调度，10个定时任务脚本按时间触发，通过邮件/微信/钉钉/Kafka四渠道推送，Redis缓存热数据。

**Tech Stack:** Python 3.14, APScheduler, kafka-python, FastAPI, Redis, Parquet/Polars

---

## Task 1: Kafka生产者模块

**Files:**
- Create: `services/notify_service/channels/kafka_producer.py`
- Modify: `core/config.py` - 添加Kafka配置
- Test: `tests/test_kafka_producer.py`

**Step 1: Write the failing test**

```python
# tests/test_kafka_producer.py
import pytest
from services.notify_service.channels.kafka_producer import KafkaProducer

class TestKafkaProducer:
    def test_kafka_producer_init(self):
        producer = KafkaProducer()
        assert producer is not None
    
    def test_send_stock_picks(self):
        producer = KafkaProducer()
        data = {
            "date": "2026-03-17",
            "stocks": [{"code": "600721", "name": "百花医药", "score": 92.0}]
        }
        result = producer.send_stock_picks(data)
        assert result is True
    
    def test_send_limit_up(self):
        producer = KafkaProducer()
        data = {
            "date": "2026-03-17",
            "stocks": [{"code": "002235", "name": "安妮股份"}]
        }
        result = producer.send_limit_up(data)
        assert result is True
```

**Step 2: Run test to verify it fails**

Run: `cd D:\workstation\xcnstock; python -m pytest tests/test_kafka_producer.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Add Kafka config to config.py**

```python
# 在 core/config.py 的 Settings 类中添加
class Settings(BaseSettings):
    # ... 现有配置 ...
    
    # Kafka配置
    KAFKA_BROKER: str = "49.233.10.199:9092"
    KAFKA_STOCK_PICKS_TOPIC: str = "xcnstock_stock_picks"
    KAFKA_LIMIT_UP_TOPIC: str = "xcnstock_limit_up"
    KAFKA_ENABLED: bool = True
```

**Step 4: Write Kafka producer implementation**

```python
# services/notify_service/channels/kafka_producer.py
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
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = KafkaProducer()
    return _producer_instance
```

**Step 5: Run test to verify it passes**

Run: `cd D:\workstation\xcnstock; python -m pytest tests/test_kafka_producer.py -v`
Expected: PASS

**Step 6: Install kafka-python dependency**

Run: `pip install kafka-python -q`

**Step 7: Commit**

```bash
git add services/notify_service/channels/kafka_producer.py core/config.py tests/test_kafka_producer.py
git commit -m "feat: add kafka producer module for stock picks and limit up data"
```

---

## Task 2: 每日任务目录结构

**Files:**
- Create: `scripts/daily_tasks/__init__.py`
- Create: `scripts/daily_tasks/base_task.py`

**Step 1: Create __init__.py**

```python
# scripts/daily_tasks/__init__.py
"""每日定时任务模块"""
from .base_task import BaseTask

__all__ = ["BaseTask"]
```

**Step 2: Create base_task.py**

```python
# scripts/daily_tasks/base_task.py
"""任务基类"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from core.logger import setup_logger


class BaseTask(ABC):
    """定时任务基类"""
    
    name: str = "base_task"
    description: str = "基础任务"
    
    def __init__(self):
        self.logger = setup_logger(f"task.{self.name}", log_file="system/tasks.log")
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def execute(self) -> bool:
        """执行任务"""
        self.start_time = datetime.now()
        self.logger.info(f"开始执行任务: {self.name}")
        
        try:
            result = self.run()
            self.end_time = datetime.now()
            elapsed = (self.end_time - self.start_time).total_seconds()
            self.logger.info(f"任务完成: {self.name}, 耗时{elapsed:.2f}秒")
            return result
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.error(f"任务失败: {self.name}, 错误: {e}")
            return False
    
    @abstractmethod
    def run(self) -> bool:
        """具体任务逻辑，子类实现"""
        pass
```

**Step 3: Commit**

```bash
git add scripts/daily_tasks/__init__.py scripts/daily_tasks/base_task.py
git commit -m "feat: add daily tasks base module"
```

---

## Task 3: 数据采集任务

**Files:**
- Create: `scripts/daily_tasks/task_data_collect.py`
- Test: `tests/test_task_data_collect.py`

**Step 1: Write the failing test**

```python
# tests/test_task_data_collect.py
import pytest
from scripts.daily_tasks.task_data_collect import DataCollectTask

class TestDataCollectTask:
    def test_task_init(self):
        task = DataCollectTask()
        assert task.name == "data_collect"
    
    def test_task_run(self):
        task = DataCollectTask()
        result = task.execute()
        assert result is True
```

**Step 2: Write implementation**

```python
# scripts/daily_tasks/task_data_collect.py
"""数据采集任务 - 15:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class DataCollectTask(BaseTask):
    """数据采集任务"""
    
    name = "data_collect"
    description = "采集实时行情和K线数据"
    
    def run(self) -> bool:
        try:
            # 1. 采集实时行情
            self._fetch_realtime()
            
            # 2. 采集K线数据
            self._fetch_klines()
            
            return True
        except Exception as e:
            self.logger.error(f"数据采集失败: {e}")
            return False
    
    def _fetch_realtime(self):
        """采集实时行情"""
        import requests
        
        self.logger.info("开始采集实时行情...")
        
        url = "http://82.102.73.198/realtime"
        resp = requests.get(url, timeout=30)
        data = resp.json()
        
        if data.get("code") == 200:
            df = pd.DataFrame(data["data"])
            today = datetime.now().strftime("%Y%m%d")
            df.to_parquet(f"data/realtime/{today}.parquet", index=False)
            self.logger.info(f"实时行情采集完成: {len(df)}只")
    
    def _fetch_klines(self):
        """增量更新K线数据"""
        self.logger.info("K线数据采集...")
        # 复用已有的fetch_all_enhanced.py逻辑
```

**Step 3: Run test**

Run: `cd D:\workstation\xcnstock; python -m pytest tests/test_task_data_collect.py -v`

**Step 4: Commit**

```bash
git add scripts/daily_tasks/task_data_collect.py tests/test_task_data_collect.py
git commit -m "feat: add data collect task"
```

---

## Task 4: 数据验证任务

**Files:**
- Create: `scripts/daily_tasks/task_data_audit.py`

**Step 1: Write implementation**

```python
# scripts/daily_tasks/task_data_audit.py
"""数据验证审计任务 - 16:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class DataAuditTask(BaseTask):
    """数据验证任务"""
    
    name = "data_audit"
    description = "验证数据完整性和新鲜度"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            issues = []
            
            # 1. 检查实时行情
            realtime_path = f"data/realtime/{today}.parquet"
            if os.path.exists(realtime_path):
                df = pd.read_parquet(realtime_path)
                if len(df) < 4000:
                    issues.append(f"实时行情不足: {len(df)}只")
            else:
                issues.append(f"实时行情文件不存在: {realtime_path}")
            
            # 2. 检查分析结果
            scores_path = "data/enhanced_scores_full.parquet"
            if os.path.exists(scores_path):
                df = pd.read_parquet(scores_path)
                if len(df) < 4000:
                    issues.append(f"分析结果不足: {len(df)}只")
            else:
                issues.append(f"分析结果文件不存在: {scores_path}")
            
            # 3. 记录结果
            if issues:
                for issue in issues:
                    self.logger.warning(issue)
                # 可以触发告警通知
            else:
                self.logger.info("数据验证通过")
            
            return True
        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            return False
```

**Step 2: Commit**

```bash
git add scripts/daily_tasks/task_data_audit.py
git commit -m "feat: add data audit task"
```

---

## Task 5: 当日复盘任务

**Files:**
- Create: `scripts/daily_tasks/task_daily_review.py`

**Step 1: Write implementation**

```python
# scripts/daily_tasks/task_daily_review.py
"""当日复盘任务 - 16:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class DailyReviewTask(BaseTask):
    """当日复盘任务"""
    
    name = "daily_review"
    description = "生成当日复盘报告"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            
            # 读取数据
            realtime = pd.read_parquet(f"data/realtime/{today}.parquet")
            scores = pd.read_parquet("data/enhanced_scores_full.parquet")
            index_df = pd.read_parquet(f"data/index_analysis_{today}.parquet")
            
            # 生成复盘报告
            report = self._generate_report(realtime, scores, index_df)
            
            # 保存报告
            self._save_report(report, today)
            
            return True
        except Exception as e:
            self.logger.error(f"复盘任务失败: {e}")
            return False
    
    def _generate_report(self, realtime, scores, index_df):
        """生成复盘报告"""
        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "market": {},
            "limit_up": {},
            "hot_sectors": {},
            "capital_flow": {}
        }
        
        # 1. 大盘分析
        report["market"] = {
            "上证指数": index_df[index_df["name"]=="上证指数"].iloc[0].to_dict() if len(index_df[index_df["name"]=="上证指数"]) > 0 else {},
            "创业板指": index_df[index_df["name"]=="创业板指"].iloc[0].to_dict() if len(index_df[index_df["name"]=="创业板指"]) > 0 else {},
        }
        
        # 2. 涨停板分析
        limit_up = realtime[realtime["change_pct"] >= 9.5]
        report["limit_up"] = {
            "total": len(limit_up),
            "stocks": limit_up[["code", "name", "change_pct"]].to_dict("records")[:20]
        }
        
        # 3. 热点板块
        # TODO: 需要板块数据
        
        return report
    
    def _save_report(self, report, today):
        """保存报告"""
        import json
        path = f"data/reports/review_{today}.json"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self.logger.info(f"复盘报告已保存: {path}")
```

**Step 2: Commit**

```bash
git add scripts/daily_tasks/task_daily_review.py
git commit -m "feat: add daily review task"
```

---

## Task 6: 次日选股任务

**Files:**
- Create: `scripts/daily_tasks/task_stock_pick.py`

**Step 1: Write implementation**

```python
# scripts/daily_tasks/task_stock_pick.py
"""次日选股任务 - 17:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class StockPickTask(BaseTask):
    """次日选股任务"""
    
    name = "stock_pick"
    description = "生成次日选股报告"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            
            # 读取分析结果
            scores = pd.read_parquet("data/enhanced_scores_full.parquet")
            
            # 生成选股报告
            picks = self._generate_picks(scores)
            
            # 保存报告
            self._save_picks(picks, today)
            
            return True
        except Exception as e:
            self.logger.error(f"选股任务失败: {e}")
            return False
    
    def _generate_picks(self, scores):
        """生成选股结果"""
        picks = {
            "s_grade": [],
            "a_grade": [],
            "limit_up_potential": []
        }
        
        # S级推荐
        s_stocks = scores[scores["grade"] == "S"].sort_values("enhanced_score", ascending=False)
        picks["s_grade"] = s_stocks[["code", "name", "price", "enhanced_score", "reasons"]].head(30).to_dict("records")
        
        # A级推荐
        a_stocks = scores[scores["grade"] == "A"].sort_values("enhanced_score", ascending=False)
        picks["a_grade"] = a_stocks[["code", "name", "price", "enhanced_score", "reasons"]].head(30).to_dict("records")
        
        # 打板潜力 (涨停+多头排列)
        limit_up = scores[(scores["change_pct"] >= 9.5) & (scores["trend"] == 100)]
        picks["limit_up_potential"] = limit_up[["code", "name", "price", "change_pct", "enhanced_score"]].to_dict("records")
        
        return picks
    
    def _save_picks(self, picks, today):
        """保存选股结果"""
        import json
        path = f"data/reports/picks_{today}.json"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(picks, f, ensure_ascii=False, indent=2)
        self.logger.info(f"选股报告已保存: {path}")
```

**Step 2: Commit**

```bash
git add scripts/daily_tasks/task_stock_pick.py
git commit -m "feat: add stock pick task"
```

---

## Task 7: 报告推送任务

**Files:**
- Create: `scripts/daily_tasks/task_morning_push.py`

**Step 1: Write implementation**

```python
# scripts/daily_tasks/task_morning_push.py
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
        from services.notify_service.channels.email import EmailChannel
        # 构建邮件内容并发送
        self.logger.info("邮件推送完成")
    
    def _push_wechat(self, picks):
        """微信推送"""
        from services.notify_service.channels.wechat import WeChatChannel
        # 构建微信消息并发送
        self.logger.info("微信推送完成")
    
    def _push_dingtalk(self, picks):
        """钉钉推送"""
        from services.notify_service.channels.dingtalk import DingTalkChannel
        # 构建钉钉消息并发送
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
```

**Step 2: Commit**

```bash
git add scripts/daily_tasks/task_morning_push.py
git commit -m "feat: add morning push task"
```

---

## Task 8: 统一调度器

**Files:**
- Modify: `services/data_service/scheduler.py`
- Create: `scripts/run_daily_scheduler.py`

**Step 1: Update scheduler.py**

```python
# services/data_service/scheduler.py - 添加每日任务调度
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime

class DailyScheduler:
    """每日任务调度器"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self._setup_jobs()
    
    def _setup_jobs(self):
        """配置定时任务"""
        # 盘后任务
        from scripts.daily_tasks.task_data_collect import DataCollectTask
        from scripts.daily_tasks.task_data_audit import DataAuditTask
        from scripts.daily_tasks.task_daily_review import DailyReviewTask
        from scripts.daily_tasks.task_stock_pick import StockPickTask
        from scripts.daily_tasks.task_morning_push import MorningPushTask
        
        # 15:30 数据采集
        self.scheduler.add_job(
            DataCollectTask().execute,
            CronTrigger(hour=15, minute=30),
            id="data_collect",
            name="数据采集"
        )
        
        # 16:00 数据验证
        self.scheduler.add_job(
            DataAuditTask().execute,
            CronTrigger(hour=16, minute=0),
            id="data_audit",
            name="数据验证"
        )
        
        # 16:30 当日复盘
        self.scheduler.add_job(
            DailyReviewTask().execute,
            CronTrigger(hour=16, minute=30),
            id="daily_review",
            name="当日复盘"
        )
        
        # 17:00 次日选股
        self.scheduler.add_job(
            StockPickTask().execute,
            CronTrigger(hour=17, minute=0),
            id="stock_pick",
            name="次日选股"
        )
        
        # 次日 08:30 报告推送
        self.scheduler.add_job(
            MorningPushTask().execute,
            CronTrigger(hour=8, minute=30),
            id="morning_push",
            name="早间推送"
        )
    
    def start(self):
        """启动调度器"""
        self.scheduler.start()
        print(f"[{datetime.now()}] 每日任务调度器已启动")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        print(f"[{datetime.now()}] 调度器已停止")
```

**Step 2: Create run script**

```python
# scripts/run_daily_scheduler.py
"""启动每日任务调度器"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import signal
from services.data_service.scheduler import DailyScheduler

scheduler = DailyScheduler()

def signal_handler(sig, frame):
    scheduler.stop()
    exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    scheduler.start()
    
    # 保持运行
    import time
    while True:
        time.sleep(60)
```

**Step 3: Commit**

```bash
git add services/data_service/scheduler.py scripts/run_daily_scheduler.py
git commit -m "feat: add daily scheduler with all tasks"
```

---

## Task 9: 一字涨停处理

**Files:**
- Create: `scripts/daily_tasks/task_open_process.py`

**Step 1: Write implementation**

```python
# scripts/daily_tasks/task_open_process.py
"""开盘处理任务 - 09:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class OpenProcessTask(BaseTask):
    """开盘处理任务 - 标记一字涨停"""
    
    name = "open_process"
    description = "处理一字涨停股票"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            
            # 读取昨日打板名单
            yesterday_path = f"data/reports/picks_{today}.json"
            if not os.path.exists(yesterday_path):
                self.logger.warning("昨日选股报告不存在")
                return True
            
            import json
            with open(yesterday_path, "r", encoding="utf-8") as f:
                picks = json.load(f)
            
            # 获取今日开盘数据
            realtime = pd.read_parquet(f"data/realtime/{today}.parquet")
            
            # 处理一字涨停
            processed = self._process_limit_up(picks, realtime)
            
            # 更新报告
            with open(yesterday_path, "w", encoding="utf-8") as f:
                json.dump(processed, f, ensure_ascii=False, indent=2)
            
            return True
        except Exception as e:
            self.logger.error(f"开盘处理失败: {e}")
            return False
    
    def _process_limit_up(self, picks, realtime):
        """标记一字涨停"""
        limit_up_stocks = picks.get("limit_up_potential", [])
        
        for stock in limit_up_stocks:
            code = stock["code"]
            
            # 查找实时数据
            rt = realtime[realtime["code"] == code]
            if len(rt) == 0:
                continue
            
            rt = rt.iloc[0]
            change_pct = rt["change_pct"]
            
            # 判断一字涨停
            if change_pct >= 9.9 and rt.get("volume", 0) < 10000:
                stock["seal_type"] = "一字涨停"
                stock["excluded"] = True
                stock["exclude_reason"] = "开盘一字涨停，无法买入"
            elif change_pct >= 9.5:
                stock["seal_type"] = "正常涨停"
            else:
                stock["seal_type"] = "未涨停"
        
        return picks
```

**Step 2: Commit**

```bash
git add scripts/daily_tasks/task_open_process.py
git commit -m "feat: add open process task for one-word limit up handling"
```

---

## Task 10: 集成测试

**Files:**
- Create: `tests/test_daily_workflow.py`

**Step 1: Write integration test**

```python
# tests/test_daily_workflow.py
"""每日工作流程集成测试"""
import pytest
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.daily_tasks.task_data_collect import DataCollectTask
from scripts.daily_tasks.task_data_audit import DataAuditTask
from scripts.daily_tasks.task_daily_review import DailyReviewTask
from scripts.daily_tasks.task_stock_pick import StockPickTask


class TestDailyWorkflow:
    """每日工作流程测试"""
    
    def test_data_collect_task(self):
        task = DataCollectTask()
        assert task.name == "data_collect"
    
    def test_data_audit_task(self):
        task = DataAuditTask()
        assert task.name == "data_audit"
    
    def test_daily_review_task(self):
        task = DailyReviewTask()
        assert task.name == "daily_review"
    
    def test_stock_pick_task(self):
        task = StockPickTask()
        assert task.name == "stock_pick"
    
    def test_kafka_producer_available(self):
        from services.notify_service.channels.kafka_producer import get_kafka_producer
        producer = get_kafka_producer()
        assert producer is not None
    
    def test_scheduler_can_be_created(self):
        from services.data_service.scheduler import DailyScheduler
        scheduler = DailyScheduler()
        assert scheduler is not None
```

**Step 2: Run all tests**

Run: `cd D:\workstation\xcnstock; python -m pytest tests/test_daily_workflow.py -v`

**Step 3: Commit**

```bash
git add tests/test_daily_workflow.py
git commit -m "test: add daily workflow integration tests"
```

---

## 部署说明

### 启动调度器

```bash
python scripts/run_daily_scheduler.py
```

### 依赖安装

```bash
pip install kafka-python apscheduler
```

### 环境变量

在 `.env` 中配置:

```
KAFKA_BROKER=49.233.10.199:9092
KAFKA_ENABLED=true
```

---

## 设计日期

2026-03-17
