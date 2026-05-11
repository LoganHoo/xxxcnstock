"""API 端点基础测试"""
import pytest


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["service"] == "xcnstock-picker-api"


class TestStatusEndpoint:
    def test_status_returns_services(self, client):
        resp = client.get("/api/v1/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "scanner" in data
        assert "predictor" in data


class TestFlowsList:
    def test_list_flows(self, client):
        resp = client.get("/api/v1/flows/list")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)
        assert len(data) >= 3


class TestFilterList:
    def test_list_filters(self, client):
        resp = client.get("/api/v1/filter/list")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)
