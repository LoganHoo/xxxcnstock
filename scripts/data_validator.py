"""
数据验证工具
用于验证采集的数据完整性和准确性
"""
import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """数据验证器"""
    
    def __init__(self, data_dir: str = "data/kline"):
        """
        初始化数据验证器
        
        Args:
            data_dir: 数据存储目录
        """
        self.data_dir = Path(data_dir)
    
    def validate_stock_data(self, code: str) -> Dict:
        """
        验证单个股票数据
        
        Args:
            code: 股票代码
            
        Returns:
            Dict: 验证结果
        """
        result = {
            'code': code,
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        file_path = self.data_dir / f"{code}.parquet"
        
        if not file_path.exists():
            result['valid'] = False
            result['errors'].append("数据文件不存在")
            return result
        
        try:
            df = pd.read_parquet(file_path)
            
            if len(df) == 0:
                result['valid'] = False
                result['errors'].append("数据为空")
                return result
            
            required_columns = ['code', 'trade_date', 'open', 'close', 'high', 'low', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                result['valid'] = False
                result['errors'].append(f"缺少必要字段: {missing_columns}")
                return result
            
            if df['code'].iloc[0] != code:
                result['warnings'].append(f"股票代码不匹配: {df['code'].iloc[0]} != {code}")
            
            df_sorted = df.sort_values('trade_date')
            if not df_sorted['trade_date'].equals(df['trade_date']):
                result['warnings'].append("交易日期未排序")
            
            duplicates = df[df.duplicated(subset=['trade_date'], keep=False)]
            if len(duplicates) > 0:
                result['warnings'].append(f"存在重复日期: {len(duplicates)}条")
            
            price_errors = []
            for idx, row in df.iterrows():
                if row['high'] < row['low']:
                    price_errors.append(f"{row['trade_date']}: 最高价({row['high']}) < 最低价({row['low']})")
                if row['high'] < row['open']:
                    price_errors.append(f"{row['trade_date']}: 最高价({row['high']}) < 开盘价({row['open']})")
                if row['high'] < row['close']:
                    price_errors.append(f"{row['trade_date']}: 最高价({row['high']}) < 收盘价({row['close']})")
                if row['low'] > row['open']:
                    price_errors.append(f"{row['trade_date']}: 最低价({row['low']}) > 开盘价({row['open']})")
                if row['low'] > row['close']:
                    price_errors.append(f"{row['trade_date']}: 最低价({row['low']}) > 收盘价({row['close']})")
            
            if price_errors:
                result['warnings'].extend(price_errors[:5])
                if len(price_errors) > 5:
                    result['warnings'].append(f"... 还有 {len(price_errors) - 5} 个价格异常")
            
            result['stats'] = {
                'total_records': len(df),
                'date_range': {
                    'start': df['trade_date'].min(),
                    'end': df['trade_date'].max()
                },
                'latest_close': float(df.iloc[-1]['close']),
                'avg_volume': float(df['volume'].mean())
            }
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"数据读取失败: {str(e)}")
        
        return result
    
    def validate_all_stocks(self, stock_list: List[str]) -> Dict:
        """
        验证所有股票数据
        
        Args:
            stock_list: 股票代码列表
            
        Returns:
            Dict: 验证结果汇总
        """
        logger.info(f"开始验证 {len(stock_list)} 只股票数据...")
        
        results = {
            'total': len(stock_list),
            'valid': 0,
            'invalid': 0,
            'warnings': 0,
            'details': []
        }
        
        for i, code in enumerate(stock_list):
            if (i + 1) % 100 == 0:
                logger.info(f"验证进度: {i + 1}/{len(stock_list)}")
            
            validation = self.validate_stock_data(code)
            results['details'].append(validation)
            
            if validation['valid']:
                results['valid'] += 1
            else:
                results['invalid'] += 1
            
            if validation['warnings']:
                results['warnings'] += 1
        
        logger.info(f"验证完成: 有效 {results['valid']}, 无效 {results['invalid']}, 警告 {results['warnings']}")
        
        return results
    
    def check_data_completeness(self, expected_date: str) -> Dict:
        """
        检查数据完整性（是否包含指定日期的数据）
        
        Args:
            expected_date: 期望的日期（YYYY-MM-DD）
            
        Returns:
            Dict: 完整性检查结果
        """
        logger.info(f"检查数据完整性，期望日期: {expected_date}")
        
        result = {
            'expected_date': expected_date,
            'total_stocks': 0,
            'stocks_with_data': 0,
            'stocks_missing_data': 0,
            'missing_stocks': []
        }
        
        parquet_files = list(self.data_dir.glob("*.parquet"))
        result['total_stocks'] = len(parquet_files)
        
        for file in parquet_files:
            code = file.stem
            try:
                df = pd.read_parquet(file)
                if expected_date in df['trade_date'].values:
                    result['stocks_with_data'] += 1
                else:
                    result['stocks_missing_data'] += 1
                    result['missing_stocks'].append(code)
            except Exception as e:
                logger.error(f"读取 {code} 数据失败: {e}")
                result['stocks_missing_data'] += 1
                result['missing_stocks'].append(code)
        
        completeness_rate = result['stocks_with_data'] / result['total_stocks'] * 100 if result['total_stocks'] > 0 else 0
        
        logger.info(f"完整性检查完成: {result['stocks_with_data']}/{result['total_stocks']} ({completeness_rate:.2f}%)")
        
        return result
    
    def generate_validation_report(self, validation_results: Dict, completeness_results: Dict) -> str:
        """
        生成验证报告
        
        Args:
            validation_results: 数据验证结果
            completeness_results: 完整性检查结果
            
        Returns:
            str: 验证报告文本
        """
        report = []
        report.append("="*70)
        report.append("数据验证报告")
        report.append("="*70)
        report.append("")
        
        report.append("📊 数据完整性检查:")
        report.append(f"  ├─ 期望日期: {completeness_results['expected_date']}")
        report.append(f"  ├─ 总股票数: {completeness_results['total_stocks']}")
        report.append(f"  ├─ 包含数据: {completeness_results['stocks_with_data']}")
        report.append(f"  ├─ 缺失数据: {completeness_results['stocks_missing_data']}")
        
        if completeness_results['total_stocks'] > 0:
            rate = completeness_results['stocks_with_data'] / completeness_results['total_stocks'] * 100
            report.append(f"  └─ 完整率: {rate:.2f}%")
        
        report.append("")
        report.append("📈 数据质量检查:")
        report.append(f"  ├─ 验证总数: {validation_results['total']}")
        report.append(f"  ├─ 有效数据: {validation_results['valid']}")
        report.append(f"  ├─ 无效数据: {validation_results['invalid']}")
        report.append(f"  └─ 警告数量: {validation_results['warnings']}")
        
        if validation_results['invalid'] > 0:
            report.append("")
            report.append("❌ 无效数据详情:")
            invalid_details = [d for d in validation_results['details'] if not d['valid']]
            for i, detail in enumerate(invalid_details[:10], 1):
                report.append(f"  {i}. {detail['code']}: {', '.join(detail['errors'])}")
            if len(invalid_details) > 10:
                report.append(f"  ... 还有 {len(invalid_details) - 10} 只股票数据无效")
        
        report.append("")
        report.append("="*70)
        
        return "\n".join(report)


if __name__ == '__main__':
    from pathlib import Path
    
    data_dir = Path("data/kline")
    validator = DataValidator(str(data_dir))
    
    result = validator.validate_stock_data("000001")
    
    print("="*70)
    print("数据验证结果")
    print("="*70)
    print(f"股票代码: {result['code']}")
    print(f"验证状态: {'有效' if result['valid'] else '无效'}")
    
    if result['errors']:
        print(f"错误: {result['errors']}")
    
    if result['warnings']:
        print(f"警告: {result['warnings'][:5]}")
    
    if result['stats']:
        print(f"统计信息:")
        print(f"  总记录数: {result['stats']['total_records']}")
        print(f"  日期范围: {result['stats']['date_range']['start']} ~ {result['stats']['date_range']['end']}")
        print(f"  最新收盘价: {result['stats']['latest_close']}")
        print(f"  平均成交量: {result['stats']['avg_volume']:.0f}")
    
    print("="*70)
