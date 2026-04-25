#!/usr/bin/env python3
"""
DataHub 集成模块

将股票数据集成到 DataHub 元数据平台:
- 注册数据集 (K线、财务、市场行为、公告)
- 发布数据血缘
- 记录数据质量
- 追踪数据处理流程

使用示例:
    integration = DataHubIntegration()
    
    # 注册 K 线数据集
    integration.register_kline_dataset("000001")
    
    # 发布数据处理血缘
    integration.publish_kline_to_selection_lineage("000001")
    
    # 记录数据质量
    integration.record_data_quality("kline", 95.5, ["数据延迟1天"])
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

import pandas as pd

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.datahub_client import DataHubClient, DataHubConfig

logger = setup_logger("datahub_integration")


class DataHubIntegration:
    """DataHub 集成器"""
    
    def __init__(self, client: Optional[DataHubClient] = None):
        """
        初始化 DataHub 集成器
        
        Args:
            client: DataHub 客户端，默认创建新实例
        """
        self.client = client or DataHubClient()
        self.logger = logger
        self.data_path = get_data_path()
        
        # 平台名称
        self.platform = "xcnstock"
        
        self.logger.info("DataHub 集成器初始化完成")
    
    def register_kline_dataset(self, code: str) -> Optional[str]:
        """
        注册 K 线数据集到 DataHub
        
        Args:
            code: 股票代码
        
        Returns:
            数据集 URN
        """
        try:
            # 读取 K 线数据获取 Schema
            kline_file = self.data_path / "kline" / f"{code}.parquet"
            if not kline_file.exists():
                self.logger.warning(f"K线文件不存在: {kline_file}")
                return None
            
            df = pd.read_parquet(kline_file)
            
            # 构建 Schema 字段
            schema_fields = []
            for col in df.columns:
                dtype = str(df[col].dtype)
                field_type = "STRING"
                if 'int' in dtype:
                    field_type = "NUMBER"
                elif 'float' in dtype:
                    field_type = "NUMBER"
                elif 'date' in dtype or 'time' in dtype:
                    field_type = "DATE"
                
                schema_fields.append({
                    "name": col,
                    "type": field_type,
                    "description": self._get_field_description(col)
                })
            
            # 注册数据集
            dataset_name = f"stock_kline.{code}"
            urn = self.client.register_dataset(
                platform=self.platform,
                name=dataset_name,
                schema_fields=schema_fields,
                description=f"股票 {code} 的K线数据",
                custom_properties={
                    "code": code,
                    "record_count": str(len(df)),
                    "date_range": f"{df['trade_date'].min()} to {df['trade_date'].max()}" if 'trade_date' in df.columns else "N/A",
                    "last_updated": datetime.now().isoformat()
                }
            )
            
            if urn:
                self.logger.info(f"K线数据集注册成功: {urn}")
            
            return urn
            
        except Exception as e:
            self.logger.error(f"注册K线数据集失败 {code}: {e}")
            return None
    
    def register_stock_list_dataset(self) -> Optional[str]:
        """
        注册股票列表数据集到 DataHub
        
        Returns:
            数据集 URN
        """
        try:
            stock_list_file = self.data_path / "stock_list.parquet"
            if not stock_list_file.exists():
                self.logger.warning("股票列表文件不存在")
                return None
            
            df = pd.read_parquet(stock_list_file)
            
            schema_fields = []
            for col in df.columns:
                schema_fields.append({
                    "name": col,
                    "type": "STRING",
                    "description": f"股票列表字段: {col}"
                })
            
            urn = self.client.register_dataset(
                platform=self.platform,
                name="stock_list",
                schema_fields=schema_fields,
                description="A股股票基础列表",
                custom_properties={
                    "record_count": str(len(df)),
                    "last_updated": datetime.now().isoformat()
                }
            )
            
            if urn:
                self.logger.info(f"股票列表数据集注册成功: {urn}")
            
            return urn
            
        except Exception as e:
            self.logger.error(f"注册股票列表数据集失败: {e}")
            return None
    
    def register_selection_result_dataset(self, result_file: str) -> Optional[str]:
        """
        注册选股结果数据集到 DataHub
        
        Args:
            result_file: 选股结果文件名
        
        Returns:
            数据集 URN
        """
        try:
            result_path = self.data_path / "workflow_results" / result_file
            if not result_path.exists():
                self.logger.warning(f"选股结果文件不存在: {result_path}")
                return None
            
            with open(result_path, 'r') as f:
                result = json.load(f)
            
            # 构建 Schema
            schema_fields = [
                {"name": "code", "type": "STRING", "description": "股票代码"},
                {"name": "name", "type": "STRING", "description": "股票名称"},
                {"name": "total_score", "type": "NUMBER", "description": "综合评分"},
                {"name": "financial_score", "type": "NUMBER", "description": "财务评分"},
                {"name": "market_score", "type": "NUMBER", "description": "市场评分"},
                {"name": "technical_score", "type": "NUMBER", "description": "技术评分"},
                {"name": "rank", "type": "NUMBER", "description": "排名"}
            ]
            
            dataset_name = f"selection_result.{result_file.replace('.json', '')}"
            urn = self.client.register_dataset(
                platform=self.platform,
                name=dataset_name,
                schema_fields=schema_fields,
                description=f"选股策略结果: {result.get('strategy_type', 'unknown')}",
                custom_properties={
                    "strategy_type": result.get('strategy_type', 'unknown'),
                    "total_stocks": str(result.get('total_stocks', 0)),
                    "selected_stocks": str(result.get('selected_stocks', 0)),
                    "generated_at": result.get('end_time', ''),
                    "result_file": result_file
                }
            )
            
            if urn:
                self.logger.info(f"选股结果数据集注册成功: {urn}")
            
            return urn
            
        except Exception as e:
            self.logger.error(f"注册选股结果数据集失败: {e}")
            return None
    
    def publish_kline_to_selection_lineage(self, code: str) -> bool:
        """
        发布 K 线数据到选股结果的血缘关系
        
        Args:
            code: 股票代码
        
        Returns:
            发布是否成功
        """
        try:
            source_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},stock_kline.{code},PROD)"
            target_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},selection_result.latest,PROD)"
            
            return self.client.publish_lineage(source_urn, target_urn, "TRANSFORMED")
            
        except Exception as e:
            self.logger.error(f"发布血缘关系失败 {code}: {e}")
            return False
    
    def publish_stock_list_to_kline_lineage(self, code: str) -> bool:
        """
        发布股票列表到 K 线数据的血缘关系
        
        Args:
            code: 股票代码
        
        Returns:
            发布是否成功
        """
        try:
            source_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},stock_list,PROD)"
            target_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},stock_kline.{code},PROD)"
            
            return self.client.publish_lineage(source_urn, target_urn, "DERIVED")
            
        except Exception as e:
            self.logger.error(f"发布血缘关系失败 {code}: {e}")
            return False
    
    def record_data_quality(self, data_type: str, score: float, 
                           issues: List[str] = None) -> bool:
        """
        记录数据质量到 DataHub
        
        Args:
            data_type: 数据类型 (kline, financial, market_behavior, announcement)
            score: 质量评分 (0-100)
            issues: 问题列表
        
        Returns:
            记录是否成功
        """
        try:
            urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{data_type},PROD)"
            
            quality_aspect = {
                "timestamp": int(datetime.now().timestamp() * 1000),
                "score": score,
                "issues": issues or [],
                "evaluator": "xcnstock_data_quality_monitor"
            }
            
            return self.client.emit_metadata(
                entity_type="dataset",
                entity_urn=urn,
                aspect_name="dataQuality",
                aspect_value=quality_aspect
            )
            
        except Exception as e:
            self.logger.error(f"记录数据质量失败 {data_type}: {e}")
            return False
    
    def register_data_job(self, job_name: str, job_type: str,
                         inputs: List[str], outputs: List[str],
                         properties: Dict[str, Any] = None) -> Optional[str]:
        """
        注册数据处理任务到 DataHub
        
        Args:
            job_name: 任务名称
            job_type: 任务类型 (data_collection, stock_selection, backtest)
            inputs: 输入数据集 URN 列表
            outputs: 输出数据集 URN 列表
            properties: 任务属性
        
        Returns:
            任务 URN
        """
        try:
            urn = f"urn:li:dataJob:(urn:li:dataPlatform:{self.platform},{job_name},PROD)"
            
            job_aspect = {
                "jobId": job_name,
                "jobType": job_type,
                "inputs": inputs,
                "outputs": outputs,
                "created": {"time": int(datetime.now().timestamp() * 1000)},
                "customProperties": properties or {}
            }
            
            success = self.client.emit_metadata(
                entity_type="dataJob",
                entity_urn=urn,
                aspect_name="dataJobInfo",
                aspect_value=job_aspect
            )
            
            if success:
                self.logger.info(f"数据处理任务注册成功: {urn}")
                return urn
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"注册数据处理任务失败 {job_name}: {e}")
            return None
    
    def batch_register_kline_datasets(self, max_count: int = 100) -> Dict[str, int]:
        """
        批量注册 K 线数据集
        
        Args:
            max_count: 最大注册数量
        
        Returns:
            统计结果
        """
        kline_dir = self.data_path / "kline"
        if not kline_dir.exists():
            return {"success": 0, "failed": 0, "total": 0}
        
        parquet_files = list(kline_dir.glob("*.parquet"))[:max_count]
        
        success_count = 0
        failed_count = 0
        
        for f in parquet_files:
            code = f.stem
            urn = self.register_kline_dataset(code)
            if urn:
                success_count += 1
            else:
                failed_count += 1
        
        self.logger.info(f"批量注册完成: 成功 {success_count}, 失败 {failed_count}")
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": len(parquet_files)
        }
    
    def _get_field_description(self, field_name: str) -> str:
        """获取字段描述"""
        descriptions = {
            "code": "股票代码",
            "trade_date": "交易日期",
            "open": "开盘价",
            "close": "收盘价",
            "high": "最高价",
            "low": "最低价",
            "volume": "成交量",
            "amount": "成交额",
            "pct_chg": "涨跌幅(%)"
        }
        return descriptions.get(field_name, f"字段: {field_name}")


def main():
    """测试 DataHub 集成"""
    integration = DataHubIntegration()
    
    # 检查 DataHub 连接
    print("="*60)
    print("DataHub 集成测试")
    print("="*60)
    
    if not integration.client.health_check():
        print("❌ DataHub 服务不可用")
        return
    
    print("✅ DataHub 服务正常")
    
    # 注册股票列表数据集
    print("\n" + "-"*60)
    print("注册股票列表数据集")
    print("-"*60)
    urn = integration.register_stock_list_dataset()
    if urn:
        print(f"✅ 股票列表数据集注册成功")
        print(f"   URN: {urn}")
    else:
        print("⚠️ 股票列表数据集注册失败或已存在")
    
    # 批量注册 K 线数据集 (前10个)
    print("\n" + "-"*60)
    print("批量注册 K 线数据集 (前10个)")
    print("-"*60)
    stats = integration.batch_register_kline_datasets(max_count=10)
    print(f"✅ 批量注册完成: 成功 {stats['success']}, 失败 {stats['failed']}")
    
    # 注册选股结果数据集
    print("\n" + "-"*60)
    print("注册选股结果数据集")
    print("-"*60)
    result_files = list(Path(integration.data_path / "workflow_results").glob("real_selection_*.json"))
    if result_files:
        urn = integration.register_selection_result_dataset(result_files[0].name)
        if urn:
            print(f"✅ 选股结果数据集注册成功")
            print(f"   URN: {urn}")
        else:
            print("⚠️ 选股结果数据集注册失败")
    else:
        print("⚠️ 未找到选股结果文件")
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)


if __name__ == '__main__':
    main()
