"""Scanner API 路由"""
from flask import Blueprint, jsonify, request, current_app

scanner_bp = Blueprint("scanner", __name__)


@scanner_bp.route("/scan", methods=["POST"])
def scan():
    svc = current_app.extensions["scanner_svc"]
    signal_threshold = request.json.get("signal_threshold", 0.5) if request.json else 0.5
    result = svc.scan(signal_threshold=signal_threshold)
    return jsonify(result.to_dict())


@scanner_bp.route("/status", methods=["GET"])
def status():
    svc = current_app.extensions["scanner_svc"]
    return jsonify(svc.get_status())
