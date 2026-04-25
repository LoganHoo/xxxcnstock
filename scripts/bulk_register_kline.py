#!/usr/bin/env python3
"""
批量注册 K 线数据到 DataHub

注册所有已采集的 K 线数据到 DataHub 元数据平台
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.datahub_rest_client import DataHubRestClient

logger = setup_logger("bulk_register_kline")


class BulkKlineRegistrar:
    """批量 K 线数据注册器"""
    
    def __init__(self, max_workers: int = 5):
        self.client = DataHubRestClient()
        self.data_path = get_data_path()
        self.logger = logger
        self.max_workers = max_workers
        self.platform = "xcnstock"
        
        # 统计
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0
        }
    
    def register_all_kline(self, limit: int = None) -> Dict:
        """
        注册所有 K 线数据
        
        Args:
            limit: 限制注册数量，None 表示全部
        
        Returns:
            统计结果
        """
        kline_dir = self.data_path / "kline"
        if not kline_dir.exists():
            self.logger.error("K 线数据目录不存在")
            return self.stats
        
        # 获取所有 parquet 文件
        parquet_files = sorted(kline_dir.glob("*.parquet"))
        if limit:
            parquet_files = parquet_files[:limit]
        
        self.stats["total"] = len(parquet_files)
        
        self.logger.info("="*60)
        self.logger.info(f"开始批量注册 K 线数据: {len(parquet_files)} 只股票")
        self.logger.info("="*60)
        
        # 检查 DataHub 连接
        if not self.client.health_check():
            self.logger.error("DataHub 服务不可用")
            return self.stats
        
        self.logger.info("✅ DataHub 连接正常")
        
        # 使用线程池并行处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._register_single_kline, f): f 
                for f in parquet_files
            }
            
            for i, future in enumerate(as_completed(future_to_file)):
                f = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        self.stats["success"] += 1
                    else:
                        self.stats["failed"] += 1
                except Exception as e:
                    self.logger.error(f"处理失败 {f.stem}: {e}")
                    self.stats["failed"] += 1
                
                # 每 100 个显示进度
                if (i + 1) % 100 == 0:
                    self._print_progress(i + 1)
        
        self._print_final_stats()
        return self.stats
    
    def _register_single_kline(self, parquet_file: Path) -> bool:
        """注册单个 K 线数据集"""
        code = parquet_file.stem
        
        try:
            # 读取数据
            df = pd.read_parquet(parquet_file)
            
            if df.empty:
                self.logger.warning(f"跳过空数据: {code}")
                self.stats["skipped"] += 1
                return False
            
            # 构建 schema
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
                    "fieldPath": col,
                    "type": field_type,
                    "description": self._get_field_description(col)
                })
            
            # 获取数据范围
            date_range = "N/A"
            if 'trade_date' in df.columns:
                min_date = df['trade_date'].min()
                max_date = df['trade_date'].max()
                date_range = f"{min_date} to {max_date}"
            
            # 创建数据集
            dataset_name = f"kline_{code}"
            urn = self.client.create_dataset(
                platform=self.platform,
                name=dataset_name,
                description=f"股票 {code} 的K线历史数据",
                schema_fields=schema_fields
            )
            
            if urn:
                # 保存元数据到本地
                self._save_metadata(code, df, date_range, urn)
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"注册失败 {code}: {e}")
            return False
    
    def _save_metadata(self, code: str, df: pd.DataFrame, date_range: str, urn: str):
        """保存元数据到本地"""
        metadata = {
            "code": code,
            "urn": urn,
            "platform": self.platform,
            "record_count": len(df),
            "date_range": date_range,
            "columns": list(df.columns),
            "registered_at": datetime.now().isoformat()
        }
        
        output_dir = self.data_path / "datahub_metadata" / "kline"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{code}.json"
        with open(output_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    
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
    
    def _print_progress(self, current: int):
        """打印进度"""
        total = self.stats["total"]
        percent = (current / total) * 100
        self.logger.info(
            f"进度: {current}/{total} ({percent:.1f}%) | "
            f"成功: {self.stats['success']} | "
            f"失败: {self.stats['failed']} | "
            f"跳过: {self.stats['skipped']}"
        )
    
    def _print_final_stats(self):
        """打印最终统计"""
        self.logger.info("="*60)
        self.logger.info("批量注册完成")
        self.logger.info("="*60)
        self.logger.info(f"总计: {self.stats['total']}")
        self.logger.info(f"成功: {self.stats['success']}")
        self.logger.info(f"失败: {self.stats['failed']}")
        self.logger.info(f"跳过: {self.stats['skipped']}")
        self.logger.info(f"成功率: {(self.stats['success']/self.stats['total']*100):.1f}%")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='批量注册 K 线数据到 DataHub')
    parser.add_argument('--limit', type=int, default=None, help='限制注册数量')
    parser.add_argument('--workers', type=int, default=5, help='并发线程数')
    
    args = parser.parse_args()
    
    registrar = BulkKlineRegistrar(max_workers=args.workers)
    stats = registrar.register_all_kline(limit=args.limit)
    
    print("\n" + "="*60)
    print("执行完成")
    print("="*60)
    print(f"总计: {stats['total']}")
    print(f"成功: {stats['success']}")
    print(f"失败: {stats['failed']}")


if __name__ == '__main__':
    main()
