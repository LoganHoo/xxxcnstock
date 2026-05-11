"""
Entry point for the scheduler HTTP API process.

Run independently from the scheduler engine:
    python -m services.data_service.scheduler.run_api [--port 5001]
"""

import argparse
from pathlib import Path

from services.data_service.scheduler.api.app import create_app


def main():
    parser = argparse.ArgumentParser(
        description="XCNStock Scheduler API Server"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5001,
        help="API server port (default: 5001)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="API server host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/scheduler_history.db",
        help="Path to scheduler history database",
    )
    args = parser.parse_args()

    app = create_app(history_db_path=Path(args.db_path))
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
