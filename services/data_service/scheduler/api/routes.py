"""
Flask API routes for scheduler monitoring.

Endpoints:
    GET /health  - Service alive status and DB accessibility
    GET /tasks   - Recent task execution records
    GET /stats   - Aggregate stats for all tasks
    GET /stats/<task_name> - Stats for a specific task
"""

from datetime import datetime

from flask import Flask, jsonify

from services.data_service.scheduler.history import HistoryDB


def register_routes(app: Flask, history_db: HistoryDB) -> None:
    """Register all API routes on the Flask app."""

    @app.route("/health", methods=["GET"])
    def health():
        """Health check: service alive and DB accessible."""
        db_accessible = False
        try:
            # Simple DB connectivity check
            history_db.get_recent_tasks(limit=1)
            db_accessible = True
        except Exception:
            pass

        return jsonify({
            "status": "alive",
            "timestamp": datetime.now().isoformat(),
            "db_accessible": db_accessible,
        })

    @app.route("/tasks", methods=["GET"])
    def tasks():
        """List recent task execution records."""
        records = history_db.get_recent_tasks(limit=50)
        return jsonify({
            "tasks": records,
            "total": len(records),
        })

    @app.route("/stats", methods=["GET"])
    def all_stats():
        """Aggregate stats for all tasks."""
        stats = history_db.get_all_task_stats(days=7)
        return jsonify(stats)

    @app.route("/stats/<task_name>", methods=["GET"])
    def task_stats(task_name: str):
        """Stats for a specific task."""
        stats = history_db.get_task_stats(task_name, days=7)
        last_exec = history_db.get_last_execution(task_name)
        return jsonify({
            "task_name": task_name,
            "stats": stats,
            "last_execution": last_exec,
        })
