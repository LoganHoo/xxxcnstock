"""预计算技术指标 - 20:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from core.logger import get_logger
from scripts.precompute_enhanced_scores import PrecomputeEngine


class Precomputer:
    """预计算器"""

    def __init__(self):
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行预计算"""
        self.logger.info("开始预计算技术指标...")

        try:
            project_root = Path(__file__).parent.parent.parent
            kline_dir = project_root / "data" / "kline"
            output_path = project_root / "data" / "enhanced_full_temp.parquet"
            stock_list_path = project_root / "data" / "stock_list.parquet"

            if not kline_dir.exists():
                self.logger.error(f"K线数据目录不存在: {kline_dir}")
                return False

            engine = PrecomputeEngine(
                kline_dir=str(kline_dir),
                output_path=str(output_path),
                stock_list_path=str(stock_list_path) if stock_list_path.exists() else None
            )
            engine.run()

            self.logger.info("预计算成功")
            return True

        except Exception as e:
            self.logger.error(f"预计算失败: {e}")
            return False


if __name__ == "__main__":
    computer = Precomputer()
    result = computer.run()
    sys.exit(0 if result else 1)