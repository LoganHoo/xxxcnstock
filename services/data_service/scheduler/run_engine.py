"""
Entry point for the scheduler engine process.

Usage: python -m services.data_service.scheduler.run_engine
"""

from pathlib import Path

from services.data_service.scheduler.engine import SchedulerEngine


def main():
    engine = SchedulerEngine(config_path=Path("config/scheduler.yaml"))
    engine.start()


if __name__ == "__main__":
    main()
