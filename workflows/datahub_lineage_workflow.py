#!/usr/bin/env python3
"""
DataHub 血缘追踪工作流

自动化数据血缘追踪:
1. 数据采集血缘: 原始数据 -> K线/财务/市场/公告数据
2. 数据处理血缘: K线数据 -> 选股结果
3. 数据质量血缘: 数据质量检查结果关联

使用示例:
    workflow = DataHubLineageWorkflow()
    
    # 记录数据采集血缘
    workflow.record_collection_lineage("000001", "kline")
    
    # 记录选股处理血缘
    workflow.record_selection_lineage(["000001", "000002"], "comprehensive")
    
    # 批量同步所有血缘
    workflow.sync_all_lineage()
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
from services.data_service.datahub_client import DataHubClient

logger = setup_logger("datahub_lineage_workflow")


class DataHubLineageWorkflow:
    """DataHub 血缘追踪工作流"""
    
    def __init__(self):
        self.client = DataHubClient()
        self.logger = logger
        self.data_path = get_data_path()
        self.lineage_log_file = self.data_path / "datahub_metadata" / "lineage_log.json"
        self.lineage_log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 加载现有血缘日志
        self.lineage_records = self._load_lineage_log()
    
    def _load_lineage_log(self) -> List[Dict]:
        """加载血缘日志"""
        if self.lineage_log_file.exists():
            try:
                with open(self.lineage_log_file, 'r') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def _save_lineage_log(self):
        """保存血缘日志"""
        with open(self.lineage_log_file, 'w') as f:
            json.dump(self.lineage_records, f, indent=2, default=str)
    
    def _build_urn(self, dataset_name: str, platform: str = "xcnstock") -> str:
        """构建数据集 URN"""
        return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},PROD)"
    
    def record_collection_lineage(self, code: str, data_type: str) -> bool:
        """
        记录数据采集血缘
        
        Args:
            code: 股票代码
            data_type: 数据类型 (kline, financial, market_behavior, announcement)
        
        Returns:
            记录是否成功
        """
        try:
            # 源: 交易所/数据源
            source_urn = self._build_urn(f"source_{data_type}")
            # 目标: 本地数据集
            target_urn = self._build_urn(f"{data_type}_{code}")
            
            lineage_record = {
                "timestamp": datetime.now().isoformat(),
                "type": "collection",
                "source": source_urn,
                "target": target_urn,
                "code": code,
                "data_type": data_type,
                "lineage_type": "COPIED"
            }
            
            self.lineage_records.append(lineage_record)
            self._save_lineage_log()
            
            # 尝试发布到 DataHub (如果可用)
            if self.client.health_check():
                self.client.publish_lineage(source_urn, target_urn, "COPIED")
            
            self.logger.info(f"✅ 数据采集血缘记录: {code} {data_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 记录数据采集血缘失败 {code}: {e}")
            return False
    
    def record_selection_lineage(self, codes: List[str], strategy_type: str) -> bool:
        """
        记录选股处理血缘
        
        Args:
            codes: 参与选股的股票代码列表
            strategy_type: 策略类型
        
        Returns:
            记录是否成功
        """
        try:
            result_id = f"selection_{strategy_type}_{datetime.now().strftime('%Y%m%d')}"
            target_urn = self._build_urn(result_id)
            
            lineage_records = []
            for code in codes[:50]:  # 限制记录数量
                source_urn = self._build_urn(f"kline_{code}")
                
                record = {
                    "timestamp": datetime.now().isoformat(),
                    "type": "selection",
                    "source": source_urn,
                    "target": target_urn,
                    "code": code,
                    "strategy_type": strategy_type,
                    "lineage_type": "TRANSFORMED"
                }
                lineage_records.append(record)
            
            self.lineage_records.extend(lineage_records)
            self._save_lineage_log()
            
            self.logger.info(f"✅ 选股处理血缘记录: {len(codes)} 只股票 -> {strategy_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 记录选股处理血缘失败: {e}")
            return False
    
    def record_backtest_lineage(self, selection_result: str, backtest_result: str) -> bool:
        """
        记录回测血缘
        
        Args:
            selection_result: 选股结果 ID
            backtest_result: 回测结果 ID
        
        Returns:
            记录是否成功
        """
        try:
            source_urn = self._build_urn(selection_result)
            target_urn = self._build_urn(backtest_result)
            
            lineage_record = {
                "timestamp": datetime.now().isoformat(),
                "type": "backtest",
                "source": source_urn,
                "target": target_urn,
                "lineage_type": "TRANSFORMED"
            }
            
            self.lineage_records.append(lineage_record)
            self._save_lineage_log()
            
            self.logger.info(f"✅ 回测血缘记录: {selection_result} -> {backtest_result}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 记录回测血缘失败: {e}")
            return False
    
    def record_quality_check_lineage(self, dataset: str, check_result: str) -> bool:
        """
        记录数据质量检查血缘
        
        Args:
            dataset: 数据集名称
            check_result: 检查结果 ID
        
        Returns:
            记录是否成功
        """
        try:
            source_urn = self._build_urn(dataset)
            target_urn = self._build_urn(f"quality_check_{check_result}")
            
            lineage_record = {
                "timestamp": datetime.now().isoformat(),
                "type": "quality_check",
                "source": source_urn,
                "target": target_urn,
                "lineage_type": "VIEW"
            }
            
            self.lineage_records.append(lineage_record)
            self._save_lineage_log()
            
            self.logger.info(f"✅ 质量检查血缘记录: {dataset}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 记录质量检查血缘失败: {e}")
            return False
    
    def sync_all_lineage(self) -> Dict[str, int]:
        """
        同步所有血缘到 DataHub
        
        Returns:
            同步统计
        """
        if not self.client.health_check():
            self.logger.warning("⚠️ DataHub 不可用，跳过同步")
            return {"synced": 0, "failed": 0, "total": len(self.lineage_records)}
        
        synced = 0
        failed = 0
        
        for record in self.lineage_records:
            try:
                success = self.client.publish_lineage(
                    record["source"],
                    record["target"],
                    record.get("lineage_type", "TRANSFORMED")
                )
                if success:
                    synced += 1
                else:
                    failed += 1
            except Exception as e:
                self.logger.error(f"同步血缘失败: {e}")
                failed += 1
        
        self.logger.info(f"血缘同步完成: 成功 {synced}, 失败 {failed}")
        return {"synced": synced, "failed": failed, "total": len(self.lineage_records)}
    
    def get_lineage_stats(self) -> Dict[str, Any]:
        """获取血缘统计"""
        stats = {
            "total_records": len(self.lineage_records),
            "by_type": {},
            "by_date": {}
        }
        
        for record in self.lineage_records:
            # 按类型统计
            record_type = record.get("type", "unknown")
            stats["by_type"][record_type] = stats["by_type"].get(record_type, 0) + 1
            
            # 按日期统计
            date = record.get("timestamp", "")[:10]
            if date:
                stats["by_date"][date] = stats["by_date"].get(date, 0) + 1
        
        return stats
    
    def generate_lineage_report(self) -> str:
        """生成血缘报告"""
        stats = self.get_lineage_stats()
        
        report = []
        report.append("="*60)
        report.append("DataHub 血缘追踪报告")
        report.append("="*60)
        report.append(f"总记录数: {stats['total_records']}")
        report.append("")
        report.append("按类型分布:")
        for t, count in stats["by_type"].items():
            report.append(f"  - {t}: {count}")
        report.append("")
        report.append("按日期分布:")
        for date, count in sorted(stats["by_date"].items()):
            report.append(f"  - {date}: {count}")
        report.append("="*60)
        
        return "\n".join(report)


def main():
    """测试血缘追踪工作流"""
    workflow = DataHubLineageWorkflow()
    
    print("="*60)
    print("DataHub 血缘追踪工作流测试")
    print("="*60)
    
    # 1. 记录数据采集血缘
    print("\n1. 记录数据采集血缘")
    print("-"*60)
    workflow.record_collection_lineage("000001", "kline")
    workflow.record_collection_lineage("000001", "financial")
    
    # 2. 记录选股处理血缘
    print("\n2. 记录选股处理血缘")
    print("-"*60)
    sample_codes = [f"{i:06d}" for i in range(1, 11)]
    workflow.record_selection_lineage(sample_codes, "comprehensive")
    
    # 3. 生成报告
    print("\n3. 血缘统计报告")
    print("-"*60)
    report = workflow.generate_lineage_report()
    print(report)
    
    # 4. 尝试同步到 DataHub
    print("\n4. 同步到 DataHub")
    print("-"*60)
    stats = workflow.sync_all_lineage()
    print(f"同步结果: {stats}")
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)


if __name__ == '__main__':
    main()
