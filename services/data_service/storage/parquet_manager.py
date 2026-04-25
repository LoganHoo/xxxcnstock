import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from typing import Optional
from datetime import datetime
import logging

from core.config import get_settings
from core.logger import setup_logger

logger = setup_logger("parquet_manager", log_file="system/storage.log")


class ParquetManager:
    """Parquet存储管理器"""
    
    def __init__(self, data_dir: str = None):
        settings = get_settings()
        self.data_dir = Path(data_dir or settings.DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def save(
        self,
        df: pd.DataFrame,
        relative_path: str,
        validate: bool = False,
        min_success_rate: float = 0.95
    ) -> bool:
        """
        保存DataFrame到Parquet文件

        Args:
            df: DataFrame数据
            relative_path: 相对于data_dir的路径
            validate: 是否进行质量验证
            min_success_rate: 最低成功率阈值
        """
        try:
            # 1. 质量验证（可选）
            if validate and 'kline' in relative_path:
                from services.data_service.quality.gx_validator import KlineDataQualitySuite
                validator = KlineDataQualitySuite.create_validator()
                result = validator.validate(df, suite_name=f"save_{relative_path}")

                if not result.success and result.success_rate < min_success_rate:
                    logger.error(
                        f"数据质量验证失败: {relative_path}, "
                        f"成功率 {result.success_rate:.1%} < {min_success_rate:.1%}"
                    )
                    return False

                if not result.success:
                    logger.warning(
                        f"数据质量警告: {relative_path}, "
                        f"成功率 {result.success_rate:.1%}"
                    )

            # 2. 保存数据
            file_path = self.data_dir / relative_path
            file_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"保存数据成功: {file_path}, {len(df)} 条")
            return True

        except Exception as e:
            logger.error(f"保存数据失败: {relative_path}, {e}")
            return False
    
    def append(self, df: pd.DataFrame, relative_path: str) -> bool:
        """
        追加数据到Parquet文件
        
        Args:
            df: 要追加的DataFrame
            relative_path: 文件路径
        """
        try:
            file_path = self.data_dir / relative_path
            
            if file_path.exists():
                existing_df = pd.read_parquet(file_path)
                df = pd.concat([existing_df, df], ignore_index=True)
            
            return self.save(df, relative_path)
            
        except Exception as e:
            logger.error(f"追加数据失败: {relative_path}, {e}")
            return False
    
    def read(self, relative_path: str) -> Optional[pd.DataFrame]:
        """
        读取Parquet文件
        
        Args:
            relative_path: 文件路径
        Returns:
            DataFrame 或 None
        """
        try:
            file_path = self.data_dir / relative_path
            
            if not file_path.exists():
                logger.warning(f"文件不存在: {file_path}")
                return None
            
            df = pd.read_parquet(file_path)
            logger.info(f"读取数据成功: {file_path}, {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"读取数据失败: {relative_path}, {e}")
            return None
    
    def save_daily_data(
        self, 
        df: pd.DataFrame, 
        data_type: str,
        date: str = None
    ) -> bool:
        """
        按日期保存数据
        
        Args:
            df: 数据
            data_type: 数据类型 (kline, limitup, fundflow等)
            date: 日期 YYYYMMDD
        """
        if not date:
            date = datetime.now().strftime("%Y%m%d")
        
        path = f"{data_type}/{date}.parquet"
        return self.save(df, path)
    
    def get_latest_data(self, data_type: str) -> Optional[pd.DataFrame]:
        """获取最新一天的数据"""
        type_dir = self.data_dir / data_type
        
        if not type_dir.exists():
            return None
        
        files = sorted(type_dir.glob("*.parquet"), reverse=True)
        
        if not files:
            return None
        
        return pd.read_parquet(files[0])
