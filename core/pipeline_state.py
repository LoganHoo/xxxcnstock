"""
================================================================================
Pipeline State Manager - 流水线状态跟踪器
================================================================================

【设计原则】
- 断点机制：每个阶段完成后保存状态，支持灾难恢复
- 状态持久化：进度保存到 JSON 文件
- 内存管理：阶段性释放内存

【状态流转】
start -> loaded -> validated -> transformed -> buffered -> executed -> distributed -> done
         |           |            |            |            |            |
         v           v            v            v            v            v
       [checkpoint] [checkpoint] [checkpoint] [checkpoint] [checkpoint] [checkpoint]

【断点文件】
data/checkpoints/
  ├── fund_behavior_2026-04-09_load.json
  ├── fund_behavior_2026-04-09_validate.json
  ├── fund_behavior_2026-04-09_transform.json
  ├── fund_behavior_2026-04-09_factor.parquet
  ├── fund_behavior_2026-04-09_execute.json
  └── fund_behavior_2026-04-09_distribute.json

================================================================================
"""
import json
import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class PipelineState(Enum):
    """流水线状态枚举"""
    START = "start"
    LOADED = "loaded"
    VALIDATED = "validated"
    TRANSFORMED = "transformed"
    BUFFERED = "buffered"
    EXECUTED = "executed"
    DISTRIBUTED = "distributed"
    DONE = "done"
    FAILED = "failed"


class PipelineStateManager:
    """流水线状态管理器"""

    WORKFLOW_STEPS = [
        "load",
        "validate",
        "transform",
        "buffer",
        "execute",
        "distribute"
    ]

    def __init__(self, report_date: str, checkpoint_dir: str = "data/checkpoints"):
        self.report_date = report_date
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.checkpoint_dir / f"fund_behavior_{report_date}_state.json"
        self._state = PipelineState.START
        self._step_results: Dict[str, Any] = {}
        self._load_state()

    def _load_state(self):
        """从文件加载状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self._state = PipelineState(data.get('state', 'start'))
                self._step_results = data.get('step_results', {})
                logger.info(f"从断点恢复状态: {self._state.value}")
            except Exception as e:
                logger.warning(f"状态文件读取失败: {e}")

    def _save_state(self):
        """保存状态到文件"""
        try:
            data = {
                'state': self._state.value,
                'report_date': self.report_date,
                'updated_at': datetime.now().isoformat(),
                'step_results': self._step_results
            }
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"状态保存失败: {e}")

    @property
    def state(self) -> PipelineState:
        return self._state

    def get_step_result(self, step: str) -> Optional[Any]:
        """获取指定步骤的结果"""
        return self._step_results.get(step)

    def can_resume_from(self, step: str) -> bool:
        """检查是否可以从指定步骤恢复"""
        step_index = self.WORKFLOW_STEPS.index(step)
        state_index = self.WORKFLOW_STEPS.index(self._state.value) if self._state.value in self.WORKFLOW_STEPS else -1
        return state_index >= step_index

    def get_checkpoint_path(self, step: str, suffix: str = "") -> Path:
        """获取断点文件路径"""
        filename = f"fund_behavior_{self.report_date}_{step}{suffix}.parquet"
        return self.checkpoint_dir / filename

    def transition(self, new_state: PipelineState, step: str, result: Any = None, message: str = ""):
        """状态转换"""
        old_state = self._state.value
        self._state = new_state
        if step:
            self._step_results[step] = {
                'state': new_state.value,
                'timestamp': datetime.now().isoformat(),
                'message': message,
                'data': result
            }
        self._save_state()
        logger.info(f"状态转换: {old_state} -> {new_state.value} ({message})")

    def mark_failed(self, step: str, error: str):
        """标记失败状态"""
        self._state = PipelineState.FAILED
        self._step_results[step] = {
            'state': 'failed',
            'timestamp': datetime.now().isoformat(),
            'error': str(error)
        }
        self._save_state()
        logger.error(f"流水线失败于 [{step}]: {error}")

    def reset(self):
        """重置状态"""
        self._state = PipelineState.START
        self._step_results = {}
        if self.state_file.exists():
            self.state_file.unlink()
        logger.info("状态已重置")

    def cleanup_checkpoints(self):
        """清理当前日期的断点文件"""
        try:
            for f in self.checkpoint_dir.glob(f"fund_behavior_{self.report_date}_*"):
                f.unlink()
            logger.info(f"已清理断点文件: {self.checkpoint_dir}/fund_behavior_{self.report_date}_*")
        except Exception as e:
            logger.warning(f"清理断点文件失败: {e}")


def get_pipeline_manager(report_date: str = None) -> PipelineStateManager:
    """获取流水线管理器实例"""
    if report_date is None:
        report_date = datetime.now().strftime('%Y-%m-%d')
    return PipelineStateManager(report_date)
