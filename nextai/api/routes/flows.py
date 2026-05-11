"""Flows API 路由"""
from flask import Blueprint, jsonify, request, current_app

flows_bp = Blueprint("flows", __name__)


@flows_bp.route("/run", methods=["POST"])
def run():
    svc = current_app.extensions["flows_svc"]
    body = request.json or {}
    flow_name = body.get("flow_name", "balanced_factor")
    scores_path = body.get("scores_path")
    result = svc.run_flow(flow_name=flow_name, scores_path=scores_path)
    return jsonify(result.to_dict())


@flows_bp.route("/run-all", methods=["POST"])
def run_all():
    svc = current_app.extensions["flows_svc"]
    results = svc.run_all_flows()
    return jsonify({k: v.to_dict() for k, v in results.items()})


@flows_bp.route("/compare", methods=["POST"])
def compare():
    svc = current_app.extensions["flows_svc"]
    result = svc.compare_flows()
    return jsonify(result)


@flows_bp.route("/list", methods=["GET"])
def list_flows():
    svc = current_app.extensions["flows_svc"]
    return jsonify(svc.list_flows())
