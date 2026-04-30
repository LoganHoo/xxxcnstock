#!/usr/bin/env python3
"""
统一进度上报 helper 测试
"""
import importlib
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


def stub_data_collect_dependencies():
    """为 data_collect_with_validation 提供最小依赖桩。"""
    polars_stub = types.ModuleType("polars")
    polars_stub.read_parquet = lambda *_args, **_kwargs: SimpleNamespace(
        to_pandas=lambda: SimpleNamespace(empty=False)
    )
    pandas_stub = types.ModuleType("pandas")

    async_fetcher_module = types.ModuleType("services.data_service.fetchers.async_kline_fetcher")

    class AsyncConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class AsyncKlineFetcher:
        def __init__(self, config):
            self.config = config

        async def fetch_single_stock(self, code, kline_dir, days=0):
            return SimpleNamespace(success=True, rows=1, status="ok", error="")

    async_fetcher_module.AsyncConfig = AsyncConfig
    async_fetcher_module.AsyncKlineFetcher = AsyncKlineFetcher
    async_fetcher_module.FetchResult = SimpleNamespace

    stock_list_module = types.ModuleType("services.data_service.fetchers.stock_list_cache")

    class StockListCacheManager:
        def __init__(self, auto_update=False):
            self.auto_update = auto_update

        def get_codes_auto(self):
            return ["000001", "000002"]

        def get_codes_with_freshness_check(self, use_redis=True):
            return {
                "codes": ["000001", "000002"],
                "freshness": {"age_days": 0},
                "source": "redis",
            }

    stock_list_module.StockListCacheManager = StockListCacheManager

    gx_module = types.ModuleType("services.data_service.quality.gx_validator")

    class GreatExpectationsValidator:
        def expect_column_to_exist(self, *_args, **_kwargs):
            return None

        def expect_column_values_to_not_be_null(self, *_args, **_kwargs):
            return None

        def expect_column_values_to_be_between(self, *_args, **_kwargs):
            return None

        def expect_ohlc_logic(self, *_args, **_kwargs):
            return None

        def validate(self, *_args, **_kwargs):
            return SimpleNamespace(success=True, success_rate=1.0)

    gx_module.GreatExpectationsValidator = GreatExpectationsValidator
    gx_module.ValidationSuiteResult = SimpleNamespace

    sys.modules["polars"] = polars_stub
    sys.modules["pandas"] = pandas_stub
    sys.modules["services.data_service.fetchers.async_kline_fetcher"] = async_fetcher_module
    sys.modules["services.data_service.fetchers.stock_list_cache"] = stock_list_module
    sys.modules["services.data_service.quality.gx_validator"] = gx_module


def load_data_collect_module(monkeypatch):
    """加载主采集脚本模块。"""
    stub_data_collect_dependencies()
    module = importlib.import_module("scripts.pipeline.data_collect_with_validation")
    return importlib.reload(module)


def test_progress_reporter_writes_expected_payload(tmp_path, monkeypatch):
    monkeypatch.setenv("XCN_PROGRESS_DIR", str(tmp_path))

    from scripts.pipeline.progress_helper import ProgressReporter

    reporter = ProgressReporter("demo_task")
    reporter.start("启动采集", progress=0)

    payload = json.loads((tmp_path / "demo_task.json").read_text(encoding="utf-8"))
    assert payload["task_name"] == "demo_task"
    assert payload["status"] == "running"
    assert payload["progress"] == 0
    assert payload["message"] == "启动采集"
    assert "updated_at" in payload


def test_progress_reporter_complete_writes_success_status(tmp_path, monkeypatch):
    monkeypatch.setenv("XCN_PROGRESS_DIR", str(tmp_path))

    from scripts.pipeline.progress_helper import ProgressReporter

    reporter = ProgressReporter("demo_task")
    reporter.start("启动", progress=0)
    reporter.complete("采集完成", extra={"rows": 10})

    payload = json.loads((tmp_path / "demo_task.json").read_text(encoding="utf-8"))
    assert payload["status"] == "success"
    assert payload["progress"] == 100
    assert payload["extra"]["rows"] == 10


@pytest.mark.asyncio
async def test_data_collect_updates_progress(monkeypatch):
    module = load_data_collect_module(monkeypatch)
    events = []

    class DummyReporter:
        def __init__(self, task_name):
            self.task_name = task_name

        def start(self, message, progress=0):
            events.append(("start", progress, message))

        def update(self, progress, message, extra=None):
            events.append(("update", progress, message))

        def complete(self, message="完成", extra=None):
            events.append(("complete", 100, message))

        def fail(self, message, extra=None):
            events.append(("fail", 0, message))

    monkeypatch.setattr(module, "ProgressReporter", DummyReporter)
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: SimpleNamespace(
            date="2026-04-30",
            retry_failed=False,
            codes="000001,000002",
            auto_update_list=False,
            max_retries=1,
            min_success_rate=0.95,
            concurrent=2,
            batch_size=2,
            request_delay=0.0,
            kline_dir="data/kline",
        ),
    )
    monkeypatch.setattr(module, "setup_logging", lambda log_dir=None: SimpleNamespace(
        info=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
    ))
    async def fake_collect_stocks_with_validation(*args, **kwargs):
        return {
            "total": 2,
            "success": 2,
            "validation_passed": 2,
            "failed": 0,
            "total_rows": 20,
            "details": [],
        }

    monkeypatch.setattr(module, "collect_stocks_with_validation", fake_collect_stocks_with_validation)
    monkeypatch.setattr(module, "generate_report", lambda *args, **kwargs: None)

    result = await module.main()

    assert result == 0
    assert events[0][0] == "start"
    assert any(event[0] == "update" for event in events)
    assert events[-1][0] == "complete"


def test_data_fetch_config_points_to_validation_script():
    from scripts import scheduler

    tasks = scheduler.load_cron_tasks()
    data_fetch = next(task for task in tasks if task["name"] == "data_fetch")

    assert data_fetch["script"] == "scripts/pipeline/data_collect_with_validation.py"


def test_smart_retry_uses_validation_collector_and_reports_progress(monkeypatch):
    module = importlib.import_module("scripts.pipeline.smart_data_fetch_retry")
    module = importlib.reload(module)
    events = []
    run_calls = []

    class DummyReporter:
        def __init__(self, task_name):
            self.task_name = task_name

        def start(self, message, progress=0):
            events.append(("start", progress, message))

        def update(self, progress, message, extra=None):
            events.append(("update", progress, message))

        def complete(self, message="完成", extra=None):
            events.append(("complete", 100, message))

        def fail(self, message, extra=None):
            events.append(("fail", 0, message))

    monkeypatch.setattr(module, "ProgressReporter", DummyReporter)

    def fake_run(args, capture_output, text, timeout, cwd):
        run_calls.append(args)
        return SimpleNamespace(returncode=0, stderr="", stdout="ok")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    assert module.run_data_fetch_retry() is True
    assert run_calls[0][1].endswith("data_collect_with_validation.py")
    assert events[0][0] == "start"
    assert events[-1][0] == "complete"
