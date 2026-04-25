#!/usr/bin/env python3
"""
导出数据为 DataHub 可导入格式

生成 DataHub 兼容的元数据文件，可通过 DataHub CLI 或 UI 导入
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("export_to_datahub")


class DataHubExporter:
    """DataHub 格式导出器"""
    
    def __init__(self):
        self.data_path = get_data_path()
        self.output_dir = self.data_path / "datahub_export"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        self.platform = "xcnstock"
        
        # MCE (Metadata Change Event) 列表
        self.mce_list: List[Dict] = []
    
    def export_all(self):
        """导出所有数据"""
        self.logger.info("="*60)
        self.logger.info("开始导出数据为 DataHub 格式")
        self.logger.info("="*60)
        
        # 1. 导出股票列表
        self.export_stock_list()
        
        # 2. 导出 K 线数据 (采样)
        self.export_kline_sample()
        
        # 3. 导出选股结果
        self.export_selection_results()
        
        # 4. 生成血缘关系
        self.generate_lineage()
        
        # 5. 保存 MCE 文件
        self.save_mce_file()
        
        self.logger.info("="*60)
        self.logger.info("导出完成")
        self.logger.info("="*60)
    
    def export_stock_list(self):
        """导出股票列表"""
        self.logger.info("\n导出股票列表...")
        
        stock_list_file = self.data_path / "stock_list.parquet"
        if not stock_list_file.exists():
            self.logger.warning("股票列表文件不存在")
            return
        
        df = pd.read_parquet(stock_list_file)
        
        # 构建 MCE
        mce = {
            "proposedSnapshot": {
                "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                    "urn": f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},stock_list,PROD)",
                    "aspects": [
                        {
                            "com.linkedin.pegasus2avro.common.Status": {
                                "removed": False
                            }
                        },
                        {
                            "com.linkedin.pegasus2avro.dataset.DatasetProperties": {
                                "description": "A股股票基础列表",
                                "customProperties": {
                                    "record_count": str(len(df)),
                                    "last_updated": datetime.now().isoformat()
                                }
                            }
                        },
                        {
                            "com.linkedin.pegasus2avro.schema.SchemaMetadata": {
                                "schemaName": "stock_list",
                                "platform": f"urn:li:dataPlatform:{self.platform}",
                                "version": 0,
                                "created": {"time": int(datetime.now().timestamp() * 1000)},
                                "fields": [
                                    {
                                        "fieldPath": col,
                                        "type": {"type": {"com.linkedin.pegasus2avro.schema.StringType": {}}},
                                        "description": f"字段: {col}"
                                    }
                                    for col in df.columns
                                ]
                            }
                        }
                    ]
                }
            }
        }
        
        self.mce_list.append(mce)
        self.logger.info(f"✅ 股票列表导出完成: {len(df)} 条记录")
    
    def export_kline_sample(self, max_stocks: int = 100):
        """导出 K 线数据样本"""
        self.logger.info(f"\n导出 K 线数据样本 (前{max_stocks}只)...")
        
        kline_dir = self.data_path / "kline"
        if not kline_dir.exists():
            self.logger.warning("K线数据目录不存在")
            return
        
        parquet_files = sorted(kline_dir.glob("*.parquet"))[:max_stocks]
        
        count = 0
        for f in parquet_files:
            code = f.stem
            try:
                df = pd.read_parquet(f)
                
                if df.empty:
                    continue
                
                # 构建字段
                fields = []
                for col in df.columns:
                    dtype = str(df[col].dtype)
                    if 'int' in dtype or 'float' in dtype:
                        field_type = {"com.linkedin.pegasus2avro.schema.NumberType": {}}
                    elif 'date' in dtype or 'time' in dtype:
                        field_type = {"com.linkedin.pegasus2avro.schema.DateType": {}}
                    else:
                        field_type = {"com.linkedin.pegasus2avro.schema.StringType": {}}
                    
                    fields.append({
                        "fieldPath": col,
                        "type": {"type": field_type},
                        "description": self._get_field_description(col)
                    })
                
                # 获取日期范围
                date_range = "N/A"
                if 'trade_date' in df.columns:
                    date_range = f"{df['trade_date'].min()} to {df['trade_date'].max()}"
                
                mce = {
                    "proposedSnapshot": {
                        "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                            "urn": f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},kline_{code},PROD)",
                            "aspects": [
                                {
                                    "com.linkedin.pegasus2avro.common.Status": {
                                        "removed": False
                                    }
                                },
                                {
                                    "com.linkedin.pegasus2avro.dataset.DatasetProperties": {
                                        "description": f"股票 {code} 的K线历史数据",
                                        "customProperties": {
                                            "code": code,
                                            "record_count": str(len(df)),
                                            "date_range": date_range,
                                            "last_updated": datetime.now().isoformat()
                                        }
                                    }
                                },
                                {
                                    "com.linkedin.pegasus2avro.schema.SchemaMetadata": {
                                        "schemaName": f"kline_{code}",
                                        "platform": f"urn:li:dataPlatform:{self.platform}",
                                        "version": 0,
                                        "created": {"time": int(datetime.now().timestamp() * 1000)},
                                        "fields": fields
                                    }
                                }
                            ]
                        }
                    }
                }
                
                self.mce_list.append(mce)
                count += 1
                
            except Exception as e:
                self.logger.error(f"导出失败 {code}: {e}")
        
        self.logger.info(f"✅ K线数据导出完成: {count} 只股票")
    
    def export_selection_results(self):
        """导出选股结果"""
        self.logger.info("\n导出选股结果...")
        
        results_dir = self.data_path / "workflow_results"
        if not results_dir.exists():
            self.logger.warning("选股结果目录不存在")
            return
        
        result_files = list(results_dir.glob("real_selection_*.json"))
        
        for result_file in result_files:
            try:
                with open(result_file, 'r') as f:
                    result = json.load(f)
                
                mce = {
                    "proposedSnapshot": {
                        "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                            "urn": f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{result_file.stem},PROD)",
                            "aspects": [
                                {
                                    "com.linkedin.pegasus2avro.common.Status": {
                                        "removed": False
                                    }
                                },
                                {
                                    "com.linkedin.pegasus2avro.dataset.DatasetProperties": {
                                        "description": f"选股策略结果: {result.get('strategy_type', 'unknown')}",
                                        "customProperties": {
                                            "strategy_type": result.get('strategy_type', ''),
                                            "total_stocks": str(result.get('total_stocks', 0)),
                                            "selected_stocks": str(result.get('selected_stocks', 0)),
                                            "generated_at": result.get('end_time', ''),
                                            "top_5_stocks": json.dumps([s['code'] for s in result.get('top_stocks', [])[:5]])
                                        }
                                    }
                                },
                                {
                                    "com.linkedin.pegasus2avro.schema.SchemaMetadata": {
                                        "schemaName": result_file.stem,
                                        "platform": f"urn:li:dataPlatform:{self.platform}",
                                        "version": 0,
                                        "created": {"time": int(datetime.now().timestamp() * 1000)},
                                        "fields": [
                                            {"fieldPath": "code", "type": {"type": {"com.linkedin.pegasus2avro.schema.StringType": {}}}, "description": "股票代码"},
                                            {"fieldPath": "name", "type": {"type": {"com.linkedin.pegasus2avro.schema.StringType": {}}}, "description": "股票名称"},
                                            {"fieldPath": "total_score", "type": {"type": {"com.linkedin.pegasus2avro.schema.NumberType": {}}}, "description": "综合评分"},
                                            {"fieldPath": "financial_score", "type": {"type": {"com.linkedin.pegasus2avro.schema.NumberType": {}}}, "description": "财务评分"},
                                            {"fieldPath": "market_score", "type": {"type": {"com.linkedin.pegasus2avro.schema.NumberType": {}}}, "description": "市场评分"},
                                            {"fieldPath": "technical_score", "type": {"type": {"com.linkedin.pegasus2avro.schema.NumberType": {}}}, "description": "技术评分"}
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
                
                self.mce_list.append(mce)
                self.logger.info(f"✅ 选股结果导出: {result_file.stem}")
                
            except Exception as e:
                self.logger.error(f"导出失败 {result_file.name}: {e}")
    
    def generate_lineage(self):
        """生成血缘关系"""
        self.logger.info("\n生成血缘关系...")
        
        # 股票列表 -> K线数据
        for mce in self.mce_list:
            snapshot = mce.get("proposedSnapshot", {}).get("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot", {})
            urn = snapshot.get("urn", "")
            
            if "kline_" in urn:
                # 提取股票代码
                code = urn.split("kline_")[-1].split(",")[0]
                
                lineage_mce = {
                    "proposedDelta": {
                        "com.linkedin.pegasus2avro.metadata.delta.DatasetDelta": {
                            "urn": urn,
                            "upstreamLineage": {
                                "com.linkedin.pegasus2avro.dataset.UpstreamLineage": {
                                    "upstreams": [
                                        {
                                            "auditStamp": {"time": int(datetime.now().timestamp() * 1000)},
                                            "dataset": f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},stock_list,PROD)",
                                            "type": "DERIVED"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
                
                self.mce_list.append(lineage_mce)
        
        self.logger.info("✅ 血缘关系生成完成")
    
    def save_mce_file(self):
        """保存 MCE 文件"""
        output_file = self.output_dir / "metadata_change_events.json"
        
        with open(output_file, 'w') as f:
            for mce in self.mce_list:
                f.write(json.dumps(mce) + "\n")
        
        self.logger.info(f"\n✅ MCE 文件保存: {output_file}")
        self.logger.info(f"   总事件数: {len(self.mce_list)}")
    
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
    """主函数"""
    exporter = DataHubExporter()
    exporter.export_all()
    
    print("\n" + "="*60)
    print("DataHub 格式导出完成")
    print("="*60)
    print(f"导出目录: {exporter.output_dir}")
    print(f"总事件数: {len(exporter.mce_list)}")
    print("\n导入命令:")
    print(f"  datahub ingest -c {exporter.output_dir}/metadata_change_events.json")


if __name__ == '__main__':
    main()
