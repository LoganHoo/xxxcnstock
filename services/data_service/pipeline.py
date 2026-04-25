#!/usr/bin/env python3
"""
数据管道 - 集成采集、存储和质量验证

提供统一的数据处理流程：
1. 采集数据
2. 质量验证
3. 存储数据
4. 错误处理
"""
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import polars as pl

from core.logger import setup_logger
from services.data_service.storage.parquet_manager import ParquetManager
from services.data_service.quality.gx_validator import (
    KlineDataQualitySuite,
    ValidationSuiteResult
)

logger = setup_logger("data_pipeline", log_file="system/pipeline.log")


@dataclass
class PipelineResult:
    """管道处理结果"""
    success: bool
    stage: str  # 'fetch', 'validate', 'save'
    message: str
    validation_result: Optional[ValidationSuiteResult] = None
    file_path: Optional[Path] = None
    metadata: Dict[str, Any] = None


class DataPipeline:
    """数据管道 - 集成采集、验证、存储"""

    def __init__(
        self,
        data_dir: str = "data",
        min_success_rate: float = 0.95,
        validate_before_save: bool = True
    ):
        self.storage = ParquetManager(data_dir)
        self.data_dir = Path(data_dir)
        self.min_success_rate = min_success_rate
        self.validate_before_save = validate_before_save
        self.validator = KlineDataQualitySuite.create_validator()

    def process_kline(
        self,
        stock_code: str,
        df: pd.DataFrame,
        validate: bool = True
    ) -> PipelineResult:
        """
        处理K线数据：验证 + 存储

        Args:
            stock_code: 股票代码
            df: K线数据DataFrame
            validate: 是否进行质量验证

        Returns:
            PipelineResult: 处理结果
        """
        # 1. 数据基础检查
        if df is None or df.empty:
            return PipelineResult(
                success=False,
                stage='fetch',
                message=f"股票 {stock_code} 数据为空"
            )

        logger.info(f"处理股票 {stock_code}: {len(df)} 条数据")

        # 2. 质量验证
        validation_result = None
        if validate and self.validate_before_save:
            validation_result = self._validate_kline(df, stock_code)

            if not validation_result.success:
                success_rate = validation_result.success_rate
                if success_rate < self.min_success_rate:
                    logger.error(
                        f"股票 {stock_code} 验证失败: 成功率 {success_rate:.1%} < {self.min_success_rate:.1%}"
                    )
                    return PipelineResult(
                        success=False,
                        stage='validate',
                        message=f"验证失败，成功率 {success_rate:.1%}",
                        validation_result=validation_result
                    )
                else:
                    logger.warning(
                        f"股票 {stock_code} 验证警告: 成功率 {success_rate:.1%}"
                    )

        # 3. 存储数据
        relative_path = f"kline/{stock_code}.parquet"
        save_success = self.storage.save(df, relative_path)

        if not save_success:
            return PipelineResult(
                success=False,
                stage='save',
                message="存储失败",
                validation_result=validation_result
            )

        file_path = self.data_dir / relative_path

        # 4. 返回成功结果
        return PipelineResult(
            success=True,
            stage='complete',
            message=f"处理完成: {len(df)} 条数据",
            validation_result=validation_result,
            file_path=file_path,
            metadata={
                'row_count': len(df),
                'success_rate': validation_result.success_rate if validation_result else 1.0,
                'timestamp': datetime.now().isoformat()
            }
        )

    def _validate_kline(
        self,
        df: pd.DataFrame,
        stock_code: str
    ) -> ValidationSuiteResult:
        """验证K线数据"""
        try:
            result = self.validator.validate(df, suite_name=f"kline_{stock_code}")
            return result
        except Exception as e:
            logger.error(f"验证过程出错: {e}")
            # 返回失败的验证结果
            from services.data_service.quality.gx_validator import ValidationSuiteResult
            return ValidationSuiteResult(
                suite_name=f"kline_{stock_code}",
                success=False,
                results=[],
                statistics={'error': str(e)}
            )

    def batch_process(
        self,
        data_dict: Dict[str, pd.DataFrame],
        validate: bool = True,
        continue_on_error: bool = True
    ) -> Dict[str, PipelineResult]:
        """
        批量处理多个股票数据

        Args:
            data_dict: {stock_code: DataFrame}
            validate: 是否验证
            continue_on_error: 出错时是否继续

        Returns:
            Dict[str, PipelineResult]: 每个股票的处理结果
        """
        results = {}
        total = len(data_dict)
        success_count = 0

        logger.info(f"开始批量处理 {total} 个股票")

        for i, (stock_code, df) in enumerate(data_dict.items(), 1):
            try:
                result = self.process_kline(stock_code, df, validate=validate)
                results[stock_code] = result

                if result.success:
                    success_count += 1

                # 每10个输出进度
                if i % 10 == 0:
                    logger.info(f"进度: {i}/{total}, 成功: {success_count}")

            except Exception as e:
                logger.error(f"处理 {stock_code} 时出错: {e}")
                results[stock_code] = PipelineResult(
                    success=False,
                    stage='error',
                    message=str(e)
                )

                if not continue_on_error:
                    break

        logger.info(f"批量处理完成: {success_count}/{total} 成功")
        return results

    def validate_existing_file(self, stock_code: str) -> PipelineResult:
        """验证已存在的文件"""
        file_path = self.data_dir / f"kline/{stock_code}.parquet"

        if not file_path.exists():
            return PipelineResult(
                success=False,
                stage='validate',
                message=f"文件不存在: {file_path}"
            )

        try:
            df = pd.read_parquet(file_path)
            result = self._validate_kline(df, stock_code)

            return PipelineResult(
                success=result.success,
                stage='validate',
                message=f"验证完成: 成功率 {result.success_rate:.1%}",
                validation_result=result,
                file_path=file_path,
                metadata={'row_count': len(df)}
            )

        except Exception as e:
            return PipelineResult(
                success=False,
                stage='validate',
                message=f"读取文件失败: {e}"
            )


# 全局管道实例
_pipeline: Optional[DataPipeline] = None


def get_data_pipeline(
    data_dir: str = "data",
    min_success_rate: float = 0.95
) -> DataPipeline:
    """获取数据管道实例（单例）"""
    global _pipeline
    if _pipeline is None:
        _pipeline = DataPipeline(
            data_dir=data_dir,
            min_success_rate=min_success_rate
        )
    return _pipeline
