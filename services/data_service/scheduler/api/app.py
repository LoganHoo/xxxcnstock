"""
Flask app factory for the scheduler HTTP API.

Serves /health, /tasks, and /stats endpoints reading from the shared
SQLite HistoryDB. Runs as an independent process from the scheduler engine.
"""

from pathlib import Path

from flask import Flask

from services.data_service.scheduler.history import HistoryDB


def create_app(
    history_db_path: Path = Path("data/scheduler_history.db"),
) -> Flask:
    """Create and configure the Flask API application."""
    app = Flask(__name__)

    history_db = HistoryDB(history_db_path)

    from services.data_service.scheduler.api.routes import register_routes
    register_routes(app, history_db)

    return app
