"""Predictor API 路由"""
from flask import Blueprint, jsonify, request, current_app

predictor_bp = Blueprint("predictor", __name__)


@predictor_bp.route("/run", methods=["POST"])
def run():
    svc = current_app.extensions["predictor_svc"]
    body = request.json or {}
    trade_date = body.get("trade_date")
    top_n = body.get("top_n", 10)
    min_confidence = body.get("min_confidence")
    result = svc.predict(
        trade_date=trade_date, top_n=top_n, min_confidence=min_confidence
    )
    return jsonify(result.to_dict())


@predictor_bp.route("/status", methods=["GET"])
def status():
    svc = current_app.extensions["predictor_svc"]
    return jsonify(svc.get_status())
