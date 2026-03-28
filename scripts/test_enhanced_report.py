"""
测试增强版明日股票推荐报告生成器
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.enhanced_tomorrow_picks import TradingCalendar, DataQualityChecker


def test_trading_calendar():
    """测试交易日历"""
    print("=" * 80)
    print("测试交易日历")
    print("=" * 80)
    
    calendar = TradingCalendar()
    
    # 测试普通工作日
    wednesday = datetime(2026, 3, 25)  # 周三
    target, desc, info = calendar.get_report_target_date(wednesday)
    print(f"\n周三 ({wednesday.strftime('%Y-%m-%d')}):")
    print(f"  目标日期: {target.strftime('%Y-%m-%d')}")
    print(f"  描述: {desc}")
    print(f"  信息: {info}")
    
    # 测试周五
    friday = datetime(2026, 3, 27)  # 周五
    target, desc, info = calendar.get_report_target_date(friday)
    print(f"\n周五 ({friday.strftime('%Y-%m-%d')}):")
    print(f"  目标日期: {target.strftime('%Y-%m-%d')}")
    print(f"  描述: {desc}")
    print(f"  信息: {info}")
    
    # 测试节假日
    new_year = datetime(2025, 1, 1)
    is_trading = calendar.is_trading_day(new_year)
    print(f"\n元旦 ({new_year.strftime('%Y-%m-%d')}):")
    print(f"  是否交易日: {is_trading}")
    
    print("\n✓ 交易日历测试通过\n")


def test_data_quality_checker():
    """测试数据质量检查器"""
    print("=" * 80)
    print("测试数据质量检查器")
    print("=" * 80)
    
    # 创建模拟数据
    import polars as pl
    
    mock_data = pl.DataFrame({
        'code': ['000001', '000002', '000003'],
        'name': ['平安银行', '万科A', '国农科技'],
        'close': [10.5, 15.2, None],
        'change_pct': [2.5, -1.2, 0.8],
        'trade_date': ['2026-03-27', '2026-03-27', '2026-03-27']
    })
    
    # 创建模拟 DataLoader
    class MockDataLoader:
        def __init__(self):
            self.kline_dir = 'data/kline'
            self.stock_list_path = 'data/stock_list.parquet'
            self.key_levels_path = 'data/key_levels_latest.parquet'
            self.cvd_path = 'data/cvd_latest.parquet'
    
    mock_loader = MockDataLoader()
    checker = DataQualityChecker(mock_loader)
    
    # 测试完整性检查
    print("\n测试数据完整性检查:")
    completeness = checker.check_data_completeness(mock_data)
    print(f"  总记录数: {completeness['total_records']}")
    print(f"  完整率: {completeness['completeness_rate']}%")
    print(f"  状态: {completeness['status']}")
    print(f"  空值统计: {completeness['null_counts']}")
    
    # 测试新鲜度检查
    print("\n测试数据新鲜度检查:")
    freshness = checker.check_data_freshness(mock_data)
    print(f"  最新日期: {freshness['latest_date']}")
    print(f"  延迟天数: {freshness['delay_days']}")
    print(f"  新鲜度: {freshness['freshness']}")
    print(f"  状态: {freshness['status']}")
    
    # 测试原始数据质量
    print("\n测试原始数据质量检查:")
    raw_quality = checker.check_raw_data_quality()
    for data_type, info in raw_quality.items():
        print(f"  {data_type}:")
        print(f"    路径: {info['path']}")
        print(f"    状态: {info['status']}")
    
    print("\n✓ 数据质量检查器测试通过\n")


def test_report_generation():
    """测试报告生成"""
    print("=" * 80)
    print("测试报告生成")
    print("=" * 80)
    
    from scripts.enhanced_tomorrow_picks import EnhancedTextReporter
    
    # 创建模拟数据
    import polars as pl
    
    mock_filter_results = {
        's_grade': pl.DataFrame({
            'code': ['000001', '000002'],
            'name': ['平安银行', '万科A'],
            'price': [10.5, 15.2],
            'change_pct': [2.5, 1.8],
            'enhanced_score': [85.5, 82.3],
            'reasons': ['多头排列', '突破压力位']
        }),
        'a_grade': pl.DataFrame({
            'code': ['000003'],
            'name': ['国农科技'],
            'price': [8.5],
            'change_pct': [0.8],
            'enhanced_score': [75.2],
            'reasons': ['量价齐升']
        })
    }
    
    mock_stats = {
        'total_stocks': 5000,
        's_grade_count': 2,
        'a_grade_count': 1,
        'bullish_count': 150,
        'rising_count': 2800
    }
    
    # 创建模拟 ConfigManager
    class MockConfigManager:
        def get_filter_config(self, filter_name):
            configs = {
                's_grade': {'description': 'S级强烈推荐'},
                'a_grade': {'description': 'A级建议关注'}
            }
            return configs.get(filter_name, {'description': filter_name})
    
    mock_config = MockConfigManager()
    
    # 创建交易信息
    trading_info = {
        'target_date': '2026-03-30',
        'date_description': '下周一 (2026-03-30)',
        'trading_day_info': '今日周五，推荐股票目标交易日为下周一 (2026-03-30)'
    }
    
    # 创建质量报告
    quality_report = {
        'completeness': {
            'total_records': 5000,
            'completeness_rate': 98.5,
            'status': '良好',
            'missing_fields': [],
            'null_counts': {}
        },
        'freshness': {
            'latest_date': '2026-03-27',
            'delay_days': 0,
            'freshness': '最新',
            'status': '优秀'
        },
        'raw_data': {
            'kline_data': {'path': 'data/kline', 'file_count': 5079, 'status': '正常'},
            'index_data': {'path': 'data/index/000001.parquet', 'record_count': 8608, 'status': '正常'},
            'stock_list': {'path': 'data/stock_list.parquet', 'record_count': 5489, 'status': '正常'}
        }
    }
    
    # 生成文本报告
    print("\n生成文本报告...")
    reporter = EnhancedTextReporter()
    text_report = reporter.generate(
        mock_filter_results, mock_stats, mock_config,
        trading_info=trading_info,
        quality_report=quality_report
    )
    
    # 打印报告开头部分
    print("\n报告预览 (前50行):")
    lines = text_report.split('\n')[:50]
    for line in lines:
        print(line)
    
    print("\n✓ 报告生成测试通过\n")


def main():
    """主测试函数"""
    print("\n" + "=" * 80)
    print("增强版明日股票推荐报告生成器测试")
    print("=" * 80 + "\n")
    
    test_trading_calendar()
    test_data_quality_checker()
    test_report_generation()
    
    print("=" * 80)
    print("所有测试通过!")
    print("=" * 80)


if __name__ == '__main__':
    main()
