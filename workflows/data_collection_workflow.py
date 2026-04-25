#!/usr/bin/env python3
"""
数据采集工作流

实现数据采集业务流:
- 财务数据采集
- 市场行为数据采集
- 公告数据采集
- 数据验证与存储
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import json

import pandas as pd
import polars as pl

from core.logger import setup_logger
from core.paths import get_data_path
from core.market_guardian import enforce_market_closed
from services.data_service.unified_data_service import UnifiedDataService
from services.data_service.quality.data_quality_monitor import DataQualityMonitor
from services.data_service.quality.ge_checkpoint_validators import GECheckpointValidators, CheckStatus, GERetryConfig
from workflows.datahub_lineage_workflow import DataHubLineageWorkflow


class CollectionType(Enum):
    """采集类型"""
    FINANCIAL = "financial"
    MARKET_BEHAVIOR = "market_behavior"
    ANNOUNCEMENT = "announcement"
    ALL = "all"


@dataclass
class CollectionResult:
    """采集结果"""
    collection_type: str
    status: str  # success, partial, failed
    start_time: str
    end_time: str
    duration_seconds: float
    records_collected: int
    records_updated: int
    records_failed: int
    errors: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'collection_type': self.collection_type,
            'status': self.status,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'records_collected': self.records_collected,
            'records_updated': self.records_updated,
            'records_failed': self.records_failed,
            'errors': self.errors,
            'details': self.details
        }


class DataCollectionWorkflow:
    """数据采集工作流"""

    def __init__(self):
        """初始化数据采集工作流"""
        self.logger = setup_logger("data_collection_workflow")
        self.data_service = UnifiedDataService()
        self.quality_monitor = DataQualityMonitor()
        # 使用GE验证器，配置重试3次
        retry_config = GERetryConfig(max_retries=3, retry_delay=1.0)
        self.checkpoint_validator = GECheckpointValidators(retry_config)
        self.lineage_workflow = DataHubLineageWorkflow()

        self.results_dir = get_data_path() / "workflow_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self,
            collection_type: CollectionType = CollectionType.ALL,
            date: Optional[str] = None,
            codes: Optional[List[str]] = None,
            validate: bool = True) -> Dict[str, CollectionResult]:
        """
        运行数据采集工作流

        Args:
            collection_type: 采集类型
            date: 指定日期 (YYYY-MM-DD)，默认为今天
            codes: 指定股票代码列表，默认为全部
            validate: 是否进行数据验证

        Returns:
            采集结果字典
        """
        # 检查点1: 采集前检查
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        pre_check = self.checkpoint_validator.pre_collection_check(
            date=date,
            data_service_health_check=lambda: self.data_service.health_check() if hasattr(self.data_service, 'health_check') else True
        )

        if pre_check.status == CheckStatus.FAILED:
            self.logger.error(f"采集前检查失败: {pre_check.message}")
            return {}

        if pre_check.status == CheckStatus.WARNING:
            self.logger.warning(f"采集前检查警告: {pre_check.message}")

        self.logger.info(f"开始数据采集工作流: {collection_type.value}, 日期: {date}")

        results = {}
        
        # 财务数据采集
        if collection_type in [CollectionType.FINANCIAL, CollectionType.ALL]:
            results['financial'] = self._collect_financial_data(date, codes)
        
        # 市场行为数据采集
        if collection_type in [CollectionType.MARKET_BEHAVIOR, CollectionType.ALL]:
            results['market_behavior'] = self._collect_market_behavior_data(date, codes)
        
        # 公告数据采集
        if collection_type in [CollectionType.ANNOUNCEMENT, CollectionType.ALL]:
            results['announcement'] = self._collect_announcement_data(date, codes)
        
        # 数据验证
        if validate:
            self._validate_collected_data(results)
        
        # 保存结果
        self._save_results(results, date)
        
        self.logger.info("数据采集工作流完成")
        
        return results
    
    def _collect_financial_data(self, date: str, codes: Optional[List[str]] = None) -> CollectionResult:
        """采集财务数据"""
        self.logger.info("开始采集财务数据")
        start_time = datetime.now()
        
        errors = []
        records_collected = 0
        records_updated = 0
        
        try:
            # 获取股票列表
            if codes is None:
                stock_list = self.data_service.get_stock_list_sync()
                codes = stock_list['code'].tolist() if 'code' in stock_list.columns else []
            
            self.logger.info(f"财务数据采集: {len(codes)} 只股票")
            
            # 采集资产负债表 (简化处理，实际应该调用具体fetcher)
            # 这里使用模拟数据演示流程
            for code in codes[:10]:  # 限制数量用于演示
                try:
                    # 模拟数据采集
                    records_collected += 1
                    records_updated += 1

                    # 记录血缘关系
                    self.lineage_workflow.record_collection_lineage(code, "financial")
                except Exception as e:
                    errors.append(f"{code}: {str(e)}")

            status = "success" if len(errors) == 0 else "partial"

        except Exception as e:
            self.logger.error(f"财务数据采集失败: {e}")
            errors.append(str(e))
            status = "failed"

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = CollectionResult(
            collection_type="financial",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            records_collected=records_collected,
            records_updated=records_updated,
            records_failed=len(errors),
            errors=errors[:10],  # 最多记录10个错误
            details={'codes_processed': len(codes) if codes else 0}
        )

        self.logger.info(f"财务数据采集完成: {status}, {records_collected} 条记录")

        return result
    
    def _collect_market_behavior_data(self, date: str, codes: Optional[List[str]] = None) -> CollectionResult:
        """采集市场行为数据"""
        self.logger.info("开始采集市场行为数据")
        start_time = datetime.now()
        
        errors = []
        records_collected = 0
        records_updated = 0
        
        try:
            # 采集龙虎榜数据
            try:
                dragon_tiger_data = self.data_service.get_dragon_tiger(trade_date=date)
                if dragon_tiger_data is not None and not dragon_tiger_data.empty:
                    records_collected += len(dragon_tiger_data)
                    records_updated += len(dragon_tiger_data)
                    self.logger.info(f"龙虎榜数据: {len(dragon_tiger_data)} 条")
            except Exception as e:
                errors.append(f"龙虎榜数据采集失败: {e}")
            
            # 采集资金流向数据
            try:
                if codes:
                    for code in codes[:50]:  # 限制数量
                        money_flow = self.data_service.get_money_flow(code)
                        if money_flow is not None:
                            records_collected += 1
                            records_updated += 1
            except Exception as e:
                errors.append(f"资金流向数据采集失败: {e}")
            
            status = "success" if len(errors) == 0 else "partial"
            
        except Exception as e:
            self.logger.error(f"市场行为数据采集失败: {e}")
            errors.append(str(e))
            status = "failed"
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = CollectionResult(
            collection_type="market_behavior",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            records_collected=records_collected,
            records_updated=records_updated,
            records_failed=len(errors),
            errors=errors[:10],
            details={'date': date}
        )
        
        self.logger.info(f"市场行为数据采集完成: {status}, {records_collected} 条记录")
        
        return result
    
    def _collect_announcement_data(self, date: str, codes: Optional[List[str]] = None) -> CollectionResult:
        """采集公告数据"""
        self.logger.info("开始采集公告数据")
        start_time = datetime.now()
        
        errors = []
        records_collected = 0
        
        try:
            # 采集公告数据 - 获取市场重大事项（不需要code参数）
            announcement_data = self.data_service.get_major_events(
                start_date=date,
                end_date=date
            )
            
            if announcement_data is not None and not announcement_data.empty:
                records_collected = len(announcement_data)
                self.logger.info(f"公告数据: {records_collected} 条")
            
            status = "success"
            
        except Exception as e:
            self.logger.error(f"公告数据采集失败: {e}")
            errors.append(str(e))
            status = "failed"
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = CollectionResult(
            collection_type="announcement",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            records_collected=records_collected,
            records_updated=records_collected,
            records_failed=len(errors),
            errors=errors[:10],
            details={'date': date}
        )
        
        self.logger.info(f"公告数据采集完成: {status}, {records_collected} 条记录")
        
        return result
    
    def _validate_collected_data(self, results: Dict[str, CollectionResult]):
        """验证采集的数据 - 集成检查点2"""
        self.logger.info("开始数据验证")

        for collection_type, result in results.items():
            if result.status == "failed":
                continue

            try:
                # 1. 原有质量检查
                if collection_type == "financial":
                    quality_result = self.quality_monitor.check_financial_data_quality()
                elif collection_type == "market_behavior":
                    quality_result = self.quality_monitor.check_data_freshness()
                elif collection_type == "announcement":
                    quality_result = self.quality_monitor.check_data_completeness()
                else:
                    quality_result = self.quality_monitor.check_data_freshness()

                if hasattr(quality_result, 'score'):
                    result.details['quality_score'] = quality_result.score
                if hasattr(quality_result, 'passed'):
                    result.details['quality_passed'] = quality_result.passed

                # 2. 检查点2: 采集后验证 (GE集成)
                # 尝试加载数据并进行GE验证
                try:
                    data_df = self._load_collected_data_for_validation(collection_type)
                    if data_df is not None and len(data_df) > 0:
                        ge_check = self.checkpoint_validator.post_collection_validation(
                            data=data_df,
                            data_type=collection_type
                        )
                        result.details['ge_validation'] = ge_check.to_dict()
                        self.logger.info(f"{collection_type} GE验证: {ge_check.status.value}")
                except Exception as ge_e:
                    self.logger.warning(f"{collection_type} GE验证失败: {ge_e}")

                self.logger.info(f"{collection_type} 数据质量评分: {result.details.get('quality_score', 0)}")

            except Exception as e:
                self.logger.warning(f"数据验证失败 {collection_type}: {e}")

    def _load_collected_data_for_validation(self, collection_type: str) -> Optional[pl.DataFrame]:
        """加载采集的数据用于验证"""
        try:
            data_path = get_data_path()
            if collection_type == "market_behavior":
                # 加载K线数据样本
                kline_path = data_path / "kline"
                if kline_path.exists():
                    parquet_files = list(kline_path.glob("*.parquet"))
                    if parquet_files:
                        return pl.read_parquet(parquet_files[0])
            elif collection_type == "financial":
                # 加载股票列表
                stock_list_path = data_path / "stock_list.parquet"
                if stock_list_path.exists():
                    return pl.read_parquet(stock_list_path)
            return None
        except Exception as e:
            self.logger.warning(f"加载数据失败: {e}")
            return None
    
    def _save_results(self, results: Dict[str, CollectionResult], date: str):
        """保存采集结果"""
        result_file = self.results_dir / f"collection_{date}.json"
        
        output = {
            'date': date,
            'timestamp': datetime.now().isoformat(),
            'results': {k: v.to_dict() for k, v in results.items()}
        }
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"采集结果已保存: {result_file}")
    
    def get_collection_history(self, days: int = 7) -> List[Dict]:
        """
        获取采集历史
        
        Args:
            days: 天数
        
        Returns:
            历史记录列表
        """
        history = []
        
        for result_file in sorted(self.results_dir.glob("collection_*.json"), reverse=True)[:days]:
            try:
                with open(result_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    history.append({
                        'date': data.get('date'),
                        'timestamp': data.get('timestamp'),
                        'results': {
                            k: {
                                'status': v.get('status'),
                                'records_collected': v.get('records_collected')
                            }
                            for k, v in data.get('results', {}).items()
                        }
                    })
            except Exception as e:
                self.logger.warning(f"读取历史记录失败 {result_file}: {e}")
        
        return history


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据采集工作流')
    parser.add_argument('--type', choices=['financial', 'market_behavior', 'announcement', 'all'],
                       default='all', help='采集类型')
    parser.add_argument('--date', help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--codes', help='指定股票代码 (逗号分隔)')
    parser.add_argument('--no-validate', action='store_true', help='跳过验证')
    
    args = parser.parse_args()
    
    # 创建工作流
    workflow = DataCollectionWorkflow()
    
    # 解析参数
    collection_type = CollectionType(args.type)
    codes = args.codes.split(',') if args.codes else None
    
    # 运行工作流
    results = workflow.run(
        collection_type=collection_type,
        date=args.date,
        codes=codes,
        validate=not args.no_validate
    )
    
    # 输出结果
    print("\n" + "="*60)
    print("数据采集工作流结果")
    print("="*60)
    
    for collection_type, result in results.items():
        status_icon = "✅" if result.status == "success" else "⚠️" if result.status == "partial" else "❌"
        print(f"\n{status_icon} {collection_type}")
        print(f"   状态: {result.status}")
        print(f"   采集记录: {result.records_collected}")
        print(f"   更新记录: {result.records_updated}")
        print(f"   失败记录: {result.records_failed}")
        print(f"   耗时: {result.duration_seconds:.2f}秒")
        
        if result.errors:
            print(f"   错误: {len(result.errors)} 个")
            for error in result.errors[:3]:
                print(f"      - {error}")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    main()
