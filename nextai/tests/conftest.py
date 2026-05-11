"""选股微服务测试框架配置"""
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def data_access():
    from nextai.services.data_access import DataAccess

    return DataAccess(
        kline_dir=str(PROJECT_ROOT / "data" / "kline"),
        stock_list_path=str(PROJECT_ROOT / "data" / "stock_list.parquet"),
        limitup_dir=str(PROJECT_ROOT / "data" / "limitup"),
    )


@pytest.fixture
def app():
    from nextai.api.app import create_app

    app = create_app()
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()
