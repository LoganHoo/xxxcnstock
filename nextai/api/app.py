"""Flask API 应用入口"""
import sys
from pathlib import Path

from flask import Flask, jsonify
from flask_cors import CORS

from nextai.services.data_access import DataAccess
from nextai.services.scanner.service import ScannerService
from nextai.services.predictor.service import PredictorService
from nextai.services.flows.service import FlowsService
from nextai.services.filter.service import FilterService
from nextai.api.routes.scanner import scanner_bp
from nextai.api.routes.predictor import predictor_bp
from nextai.api.routes.flows import flows_bp
from nextai.api.routes.filter import filter_bp


def create_app(config_path: str = None) -> Flask:
    app = Flask(__name__)

    if config_path:
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        app.config.update(config)

    CORS(app)

    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))

    kline_dir = app.config.get("data", {}).get("kline_dir", "data/kline")
    stock_list_path = app.config.get("data", {}).get(
        "stock_list_path", "data/stock_list.parquet"
    )
    limitup_dir = app.config.get("data", {}).get("limitup_dir", "data/limitup")

    data_access = DataAccess(
        kline_dir=str(project_root / kline_dir),
        stock_list_path=str(project_root / stock_list_path),
        limitup_dir=str(project_root / limitup_dir),
    )

    scanner_svc = ScannerService(
        data_access=data_access,
        max_results=app.config.get("scanner", {}).get("max_results", 100),
    )
    predictor_svc = PredictorService(
        data_access=data_access,
        model_path=app.config.get("predictor", {}).get("model_path"),
        min_confidence=app.config.get("predictor", {}).get("confidence_threshold", 70.0),
    )
    flows_svc = FlowsService(
        data_access=data_access,
        kline_dir=str(project_root / kline_dir),
    )
    filter_svc = FilterService(
        data_access=data_access,
        config_dir=app.config.get("filters", {}).get("config_dir", "config/filters"),
    )

    app.register_blueprint(scanner_bp, url_prefix="/api/v1/scanner")
    app.register_blueprint(predictor_bp, url_prefix="/api/v1/predictor")
    app.register_blueprint(flows_bp, url_prefix="/api/v1/flows")
    app.register_blueprint(filter_bp, url_prefix="/api/v1/filter")

    app.extensions["scanner_svc"] = scanner_svc
    app.extensions["predictor_svc"] = predictor_svc
    app.extensions["flows_svc"] = flows_svc
    app.extensions["filter_svc"] = filter_svc

    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "service": "xcnstock-picker-api"})

    @app.route("/api/v1/status")
    def status():
        return jsonify(
            {
                "scanner": scanner_svc.get_status(),
                "predictor": predictor_svc.get_status(),
                "flows": flows_svc.list_flows(),
                "filters": filter_svc.list_filters(),
            }
        )

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)
