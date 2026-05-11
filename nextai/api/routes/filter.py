"""Filter API 路由"""
from flask import Blueprint, jsonify, request, current_app

filter_bp = Blueprint("filter", __name__)


@filter_bp.route("/apply", methods=["POST"])
def apply():
    svc = current_app.extensions["filter_svc"]
    body = request.json or {}
    filter_names = body.get("filter_names")
    result = svc.apply_filters(filter_names=filter_names)
    return jsonify(result.to_dict())


@filter_bp.route("/list", methods=["GET"])
def list_filters():
    svc = current_app.extensions["filter_svc"]
    return jsonify(svc.list_filters())
