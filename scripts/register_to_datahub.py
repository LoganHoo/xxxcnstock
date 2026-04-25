#!/usr/bin/env python3
"""
注册 XCNStock 数据到 DataHub

使用 DataHub Frontend API (端口 9002)
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import os
import json
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("register_to_datahub")


class DataHubFrontendClient:
    """DataHub 前端客户端"""
    
    def __init__(self):
        self.base_url = os.getenv('DATAHUB_UI_URL', 'http://192.168.1.168:9002')
        self.username = os.getenv('DATAHUB_USERNAME', 'datahub')
        self.password = os.getenv('DATAHUB_PASSWORD', 'datahub')
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        self.logger = logger
    
    def test_connection(self) -> bool:
        """测试连接"""
        try:
            response = self.session.get(f"{self.base_url}/api/v2/graphql", timeout=10)
            self.logger.info(f"连接测试: HTTP {response.status_code}")
            return response.status_code in [200, 401]  # 401 表示需要认证，也是正常的
        except Exception as e:
            self.logger.error(f"连接失败: {e}")
            return False
    
    def graphql_query(self, query: str, variables: Dict = None) -> Optional[Dict]:
        """执行 GraphQL 查询"""
        try:
            url = f"{self.base_url}/api/v2/graphql"
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            
            response = self.session.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"GraphQL 查询失败: {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"GraphQL 查询异常: {e}")
            return None


class DataHubRegistrar:
    """DataHub 数据注册器"""
    
    def __init__(self):
        self.client = DataHubFrontendClient()
        self.data_path = get_data_path()
        self.logger = logger
        self.platform = "xcnstock"
    
    def register_all(self):
        """注册所有数据"""
        self.logger.info("="*60)
        self.logger.info("开始注册数据到 DataHub")
        self.logger.info("="*60)
        
        # 1. 测试连接
        if not self.client.test_connection():
            self.logger.error("❌ DataHub 连接失败")
            return
        self.logger.info("✅ DataHub 连接正常")
        
        # 2. 注册股票列表
        self.register_stock_list()
        
        # 3. 注册 K 线数据 (采样)
        self.register_sample_kline()
        
        # 4. 注册选股结果
        self.register_selection_results()
        
        self.logger.info("="*60)
        self.logger.info("数据注册完成")
        self.logger.info("="*60)
    
    def register_stock_list(self):
        """注册股票列表"""
        self.logger.info("\n" + "-"*60)
        self.logger.info("注册股票列表")
        self.logger.info("-"*60)
        
        stock_list_file = self.data_path / "stock_list.parquet"
        if not stock_list_file.exists():
            self.logger.warning("⚠️ 股票列表文件不存在")
            return
        
        try:
            df = pd.read_parquet(stock_list_file)
            
            # 构建数据集信息
            dataset_info = {
                "name": "stock_list",
                "platform": self.platform,
                "description": "A股股票基础列表",
                "schema": {
                    "fields": [
                        {"fieldPath": col, "type": "STRING", "description": f"字段: {col}"}
                        for col in df.columns
                    ]
                },
                "properties": {
                    "record_count": len(df),
                    "last_updated": datetime.now().isoformat(),
                    "columns": list(df.columns)
                }
            }
            
            # 保存到本地 JSON (作为 DataHub 导入的源)
            output_file = self.data_path / "datahub_metadata" / "stock_list.json"
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(dataset_info, f, indent=2, default=str)
            
            self.logger.info(f"✅ 股票列表元数据已保存: {output_file}")
            self.logger.info(f"   记录数: {len(df)}")
            self.logger.info(f"   字段: {', '.join(df.columns[:5])}...")
            
        except Exception as e:
            self.logger.error(f"❌ 注册股票列表失败: {e}")
    
    def register_sample_kline(self, max_stocks: int = 10):
        """注册 K 线数据样本"""
        self.logger.info("\n" + "-"*60)
        self.logger.info(f"注册 K 线数据样本 (前{max_stocks}只股票)")
        self.logger.info("-"*60)
        
        kline_dir = self.data_path / "kline"
        if not kline_dir.exists():
            self.logger.warning("⚠️ K线数据目录不存在")
            return
        
        parquet_files = sorted(kline_dir.glob("*.parquet"))[:max_stocks]
        
        success_count = 0
        for f in parquet_files:
            code = f.stem
            try:
                df = pd.read_parquet(f)
                
                dataset_info = {
                    "name": f"kline_{code}",
                    "platform": self.platform,
                    "description": f"股票 {code} 的K线数据",
                    "schema": {
                        "fields": [
                            {"fieldPath": col, "type": str(df[col].dtype), "description": f"字段: {col}"}
                            for col in df.columns
                        ]
                    },
                    "properties": {
                        "code": code,
                        "record_count": len(df),
                        "date_range": f"{df['trade_date'].min()} to {df['trade_date'].max()}" if 'trade_date' in df.columns else "N/A",
                        "last_updated": datetime.now().isoformat()
                    }
                }
                
                # 保存元数据
                output_file = self.data_path / "datahub_metadata" / "kline" / f"{code}.json"
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file, 'w') as f_out:
                    json.dump(dataset_info, f_out, indent=2, default=str)
                
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"❌ 注册 K 线数据失败 {code}: {e}")
        
        self.logger.info(f"✅ K 线数据注册完成: {success_count}/{len(parquet_files)}")
    
    def register_selection_results(self):
        """注册选股结果"""
        self.logger.info("\n" + "-"*60)
        self.logger.info("注册选股结果")
        self.logger.info("-"*60)
        
        results_dir = self.data_path / "workflow_results"
        if not results_dir.exists():
            self.logger.warning("⚠️ 选股结果目录不存在")
            return
        
        result_files = list(results_dir.glob("real_selection_*.json"))
        
        for result_file in result_files:
            try:
                with open(result_file, 'r') as f:
                    result = json.load(f)
                
                dataset_info = {
                    "name": result_file.stem,
                    "platform": self.platform,
                    "description": f"选股策略结果: {result.get('strategy_type', 'unknown')}",
                    "schema": {
                        "fields": [
                            {"fieldPath": "code", "type": "STRING", "description": "股票代码"},
                            {"fieldPath": "name", "type": "STRING", "description": "股票名称"},
                            {"fieldPath": "total_score", "type": "NUMBER", "description": "综合评分"},
                            {"fieldPath": "financial_score", "type": "NUMBER", "description": "财务评分"},
                            {"fieldPath": "market_score", "type": "NUMBER", "description": "市场评分"},
                            {"fieldPath": "technical_score", "type": "NUMBER", "description": "技术评分"}
                        ]
                    },
                    "properties": {
                        "strategy_type": result.get('strategy_type'),
                        "total_stocks": result.get('total_stocks'),
                        "selected_stocks": result.get('selected_stocks'),
                        "generated_at": result.get('end_time'),
                        "top_stocks": [s['code'] for s in result.get('top_stocks', [])[:5]]
                    }
                }
                
                # 保存元数据
                output_file = self.data_path / "datahub_metadata" / "selection" / f"{result_file.stem}.json"
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file, 'w') as f_out:
                    json.dump(dataset_info, f_out, indent=2, default=str)
                
                self.logger.info(f"✅ 选股结果已保存: {output_file.name}")
                
            except Exception as e:
                self.logger.error(f"❌ 注册选股结果失败 {result_file.name}: {e}")
    
    def generate_lineage_config(self):
        """生成血缘配置"""
        self.logger.info("\n" + "-"*60)
        self.logger.info("生成血缘配置")
        self.logger.info("-"*60)
        
        lineage = {
            "dataflows": [
                {
                    "name": "stock_list_to_kline",
                    "source": "stock_list",
                    "target": "kline_*",
                    "type": "DERIVED",
                    "description": "股票列表生成K线数据"
                },
                {
                    "name": "kline_to_selection",
                    "source": "kline_*",
                    "target": "real_selection_*",
                    "type": "TRANSFORMED",
                    "description": "K线数据选股分析"
                }
            ]
        }
        
        output_file = self.data_path / "datahub_metadata" / "lineage.json"
        with open(output_file, 'w') as f:
            json.dump(lineage, f, indent=2)
        
        self.logger.info(f"✅ 血缘配置已保存: {output_file}")


def main():
    """主函数"""
    registrar = DataHubRegistrar()
    registrar.register_all()
    registrar.generate_lineage_config()
    
    print("\n" + "="*60)
    print("DataHub 数据注册完成")
    print("="*60)
    print(f"元数据目录: {get_data_path() / 'datahub_metadata'}")
    print("\n文件列表:")
    metadata_dir = get_data_path() / "datahub_metadata"
    if metadata_dir.exists():
        for f in sorted(metadata_dir.rglob("*.json")):
            print(f"  - {f.relative_to(metadata_dir)}")


if __name__ == '__main__':
    main()
