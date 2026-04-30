#!/usr/bin/env python3
"""
统一进度上报 helper。
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def get_progress_dir() -> Path:
    """获取进度文件目录，测试时允许通过环境变量覆盖。"""
    custom_dir = os.getenv("XCN_PROGRESS_DIR")
    if custom_dir:
        return Path(custom_dir)
    return Path("data/tasks/progress")


class ProgressReporter:
    """统一管理任务进度文件写入。"""

    def __init__(self, task_name: str, progress_dir: Optional[Path] = None):
        self.task_name = task_name
        self.progress_dir = progress_dir or get_progress_dir()
        self.progress_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = self.progress_dir / f"{task_name}.json"
        self.started_at: Optional[str] = None

    def start(self, message: str, progress: float = 0, extra: Optional[Dict[str, Any]] = None) -> None:
        """记录任务开始。"""
        self.started_at = datetime.now().isoformat()
        self._write("running", progress, message, extra)

    def update(self, progress: float, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """更新运行中进度。"""
        if self.started_at is None:
            self.started_at = datetime.now().isoformat()
        self._write("running", progress, message, extra)

    def complete(self, message: str = "完成", extra: Optional[Dict[str, Any]] = None) -> None:
        """记录成功完成。"""
        if self.started_at is None:
            self.started_at = datetime.now().isoformat()
        self._write("success", 100, message, extra)

    def fail(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """记录失败状态。"""
        if self.started_at is None:
            self.started_at = datetime.now().isoformat()
        self._write("failed", 0, message, extra)

    def cleanup(self) -> None:
        """删除进度文件。"""
        if self.file_path.exists():
            self.file_path.unlink()

    def _write(self, status: str, progress: float, message: str, extra: Optional[Dict[str, Any]]) -> None:
        payload = {
            "task_name": self.task_name,
            "status": status,
            "progress": progress,
            "message": message,
            "updated_at": datetime.now().isoformat(),
            "started_at": self.started_at,
            "extra": extra or {},
        }
        self.file_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
