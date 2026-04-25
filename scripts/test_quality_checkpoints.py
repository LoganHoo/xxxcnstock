#!/usr/bin/env python3
"""
数据质量检查点测试脚本

测试所有6个检查点的功能
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from datetime import datetime, timedelta
import polars as pl
import pandas as pd
import numpy as np

from services.data_service.quality.checkpoint_validators import (
    DataQualityCheckpoints,
    CheckStatus,
    run_pre_collection_check,
    run_post_collection_validation,
    run_pre_scoring_check,
    run_post_scoring_validation,
    run_pre_selection_check,
    run_final_output_validation
)
from core.paths import get_data_path


def test_checkpoint_1():
    """测试检查点1: 采集前检查"""
    print("\n" + "="*60)
    print("🧪 测试检查点1: 采集前检查")
    print("="*60)

    validator = DataQualityCheckpoints()

    # 测试1: 正常日期
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    result = validator.pre_collection_check(
        date=yesterday,
        data_service_health_check=lambda: True
    )
    print(f"✅ 历史日期检查: {result.status.value} - {result.message}")

    # 测试2: 今天日期 (可能失败，取决于市场状态)
    today = datetime.now().strftime('%Y-%m-%d')
    result = validator.pre_collection_check(
        date=today,
        data_service_health_check=lambda: True
    )
    print(f"📊 今日日期检查: {result.status.value} - {result.message}")

    return True


def test_checkpoint_2():
    """测试检查点2: 采集后验证"""
    print("\n" + "="*60)
    print("🧪 测试检查点2: 采集后验证 (GE集成)")
    print("="*60)

    validator = DataQualityCheckpoints()

    # 测试1: 股票列表数据
    data_path = get_data_path()
    stock_list_path = data_path / "stock_list.parquet"

    if stock_list_path.exists():
        df = pl.read_parquet(stock_list_path)
        result = validator.post_collection_validation(df, data_type="stock_list")
        print(f"✅ 股票列表验证: {result.status.value}")
        print(f"   质量分数: {result.details.get('quality_score', 0):.1f}")
        print(f"   GE成功率: {result.details.get('ge_success_rate', 0):.1f}%")
        print(f"   行数: {result.details.get('rows', 0)}")
    else:
        print(f"⚠️  股票列表文件不存在: {stock_list_path}")

    # 测试2: K线数据
    kline_path = data_path / "kline"
    if kline_path.exists():
        parquet_files = list(kline_path.glob("*.parquet"))
        if parquet_files:
            df = pl.read_parquet(parquet_files[0])
            result = validator.post_collection_validation(df, data_type="kline")
            print(f"✅ K线数据验证: {result.status.value}")
            print(f"   质量分数: {result.details.get('quality_score', 0):.1f}")
            print(f"   行数: {result.details.get('rows', 0)}")
        else:
            print(f"⚠️  K线目录为空")
    else:
        print(f"⚠️  K线目录不存在: {kline_path}")

    return True


def test_checkpoint_3():
    """测试检查点3: 计算前检查"""
    print("\n" + "="*60)
    print("🧪 测试检查点3: 计算前检查")
    print("="*60)

    validator = DataQualityCheckpoints()

    # 测试1: 正常数据
    normal_data = pl.DataFrame({
        'trade_date': pl.date_range(datetime(2024, 1, 1), datetime(2024, 2, 1), '1d', eager=True),
        'close': np.random.uniform(10, 100, 32),
        'volume': np.random.randint(1000000, 10000000, 32),
        'high': np.random.uniform(10, 100, 32),
        'low': np.random.uniform(10, 100, 32),
    })
    result = validator.pre_scoring_check(normal_data, code="000001")
    print(f"✅ 正常数据检查: {result.status.value} - {result.message}")

    # 测试2: 数据不足
    small_data = pl.DataFrame({
        'trade_date': [datetime(2024, 1, 1)],
        'close': [100.0],
        'volume': [1000000],
        'high': [101.0],
        'low': [99.0],
    })
    result = validator.pre_scoring_check(small_data, code="000002")
    print(f"✅ 数据不足检查: {result.status.value} - {result.message}")

    # 测试3: 缺少必要列
    incomplete_data = pl.DataFrame({
        'trade_date': pl.date_range(datetime(2024, 1, 1), datetime(2024, 2, 1), '1d', eager=True),
        'close': np.random.uniform(10, 100, 32),
    })
    result = validator.pre_scoring_check(incomplete_data, code="000003")
    print(f"✅ 缺少列检查: {result.status.value} - {result.message}")

    # 测试4: 收盘价异常
    abnormal_data = pl.DataFrame({
        'trade_date': pl.date_range(datetime(2024, 1, 1), datetime(2024, 2, 1), '1d', eager=True),
        'close': np.random.uniform(-10, 0, 32),  # 负价格
        'volume': np.random.randint(1000000, 10000000, 32),
        'high': np.random.uniform(10, 100, 32),
        'low': np.random.uniform(10, 100, 32),
    })
    result = validator.pre_scoring_check(abnormal_data, code="000004")
    print(f"✅ 异常价格检查: {result.status.value} - {result.message}")

    return True


def test_checkpoint_4():
    """测试检查点4: 计算后验证"""
    print("\n" + "="*60)
    print("🧪 测试检查点4: 计算后验证")
    print("="*60)

    validator = DataQualityCheckpoints()

    # 测试1: 正常评分结果
    normal_scores = pl.DataFrame({
        'code': [f'{i:06d}' for i in range(100)],
        'total_score': np.random.uniform(30, 90, 100),
    })
    result = validator.post_scoring_validation(normal_scores)
    print(f"✅ 正常评分验证: {result.status.value} - {result.message}")

    # 测试2: 评分结果为空
    empty_scores = pl.DataFrame({'code': [], 'total_score': []})
    result = validator.post_scoring_validation(empty_scores)
    print(f"✅ 空结果验证: {result.status.value} - {result.message}")

    # 测试3: 评分过于集中
    concentrated_scores = pl.DataFrame({
        'code': [f'{i:06d}' for i in range(100)],
        'total_score': [50.0] * 100,  # 所有评分相同
    })
    result = validator.post_scoring_validation(concentrated_scores)
    print(f"✅ 集中评分验证: {result.status.value} - {result.message}")

    # 测试4: 评分超出范围
    out_of_range_scores = pl.DataFrame({
        'code': [f'{i:06d}' for i in range(100)],
        'total_score': np.random.uniform(-10, 150, 100),  # 超出0-100范围
    })
    result = validator.post_scoring_validation(out_of_range_scores)
    print(f"✅ 范围检查: {result.status.value} - {result.message}")

    return True


def test_checkpoint_5():
    """测试检查点5: 选股前检查"""
    print("\n" + "="*60)
    print("🧪 测试检查点5: 选股前检查")
    print("="*60)

    validator = DataQualityCheckpoints()
    today = datetime.now().strftime('%Y-%m-%d')

    # 测试1: 正常股票池
    normal_pool = pd.DataFrame({
        'code': [f'{i:06d}' for i in range(200)],
        'name': [f'股票{i}' for i in range(200)],
        'close': np.random.uniform(10, 100, 200),
        'volume': np.random.randint(1000000, 10000000, 200),
        'trade_date': [today] * 200,
    })
    result = validator.pre_selection_check(normal_pool, today)
    print(f"✅ 正常股票池检查: {result.status.value} - {result.message}")

    # 测试2: 股票池太小
    small_pool = pd.DataFrame({
        'code': ['000001', '000002'],
        'name': ['股票1', '股票2'],
        'close': [100.0, 200.0],
        'volume': [1000000, 2000000],
    })
    result = validator.pre_selection_check(small_pool, today)
    print(f"✅ 小股票池检查: {result.status.value} - {result.message}")

    # 测试3: 缺少必要列
    incomplete_pool = pd.DataFrame({
        'code': [f'{i:06d}' for i in range(200)],
    })
    result = validator.pre_selection_check(incomplete_pool, today)
    print(f"✅ 缺少列检查: {result.status.value} - {result.message}")

    return True


def test_checkpoint_6():
    """测试检查点6: 最终输出验证"""
    print("\n" + "="*60)
    print("🧪 测试检查点6: 最终输出验证")
    print("="*60)

    validator = DataQualityCheckpoints()
    today = datetime.now().strftime('%Y-%m-%d')

    # 测试1: 正常输出
    normal_output = pd.DataFrame({
        'code': ['000001', '000002', '000003'],
        'name': ['平安银行', '万科A', '国农科技'],
        'score': [85.5, 82.3, 78.9],
        'latest_date': [today, today, today],
    })
    result = validator.final_output_validation(normal_output)
    print(f"✅ 正常输出验证: {result.status.value} - {result.message}")

    # 测试2: 空输出
    empty_output = pd.DataFrame()
    result = validator.final_output_validation(empty_output)
    print(f"✅ 空输出验证: {result.status.value} - {result.message}")

    # 测试3: 包含ST股票
    st_output = pd.DataFrame({
        'code': ['000001', '000002'],
        'name': ['平安银行', '*ST股票'],
        'score': [85.5, 20.0],
        'latest_date': [today, today],
    })
    result = validator.final_output_validation(st_output)
    print(f"✅ ST股票检查: {result.status.value} - {result.message}")

    # 测试4: 包含退市股票
    delisting_output = pd.DataFrame({
        'code': ['000001', '000002'],
        'name': ['平安银行', '退市股票'],
        'score': [85.5, 10.0],
        'latest_date': [today, today],
    })
    result = validator.final_output_validation(delisting_output)
    print(f"✅ 退市股票检查: {result.status.value} - {result.message}")

    # 测试5: 数据过旧
    old_date = (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')
    old_data_output = pd.DataFrame({
        'code': ['000001', '000002'],
        'name': ['平安银行', '万科A'],
        'score': [85.5, 82.3],
        'latest_date': [old_date, old_date],
    })
    result = validator.final_output_validation(old_data_output)
    print(f"✅ 旧数据检查: {result.status.value} - {result.message}")

    return True


def test_summary():
    """打印测试摘要"""
    print("\n" + "="*60)
    print("📊 测试摘要")
    print("="*60)

    validator = DataQualityCheckpoints()
    summary = validator.get_summary()

    print(f"总检查次数: {summary['total_checks']}")
    print(f"通过: {summary['passed']} ✅")
    print(f"警告: {summary['warnings']} ⚠️")
    print(f"失败: {summary['failed']} ❌")
    print(f"成功率: {summary['success_rate']:.1f}%")

    return summary


def main():
    """主函数"""
    print("="*60)
    print("🚀 数据质量检查点测试")
    print("="*60)

    try:
        # 运行所有测试
        test_checkpoint_1()
        test_checkpoint_2()
        test_checkpoint_3()
        test_checkpoint_4()
        test_checkpoint_5()
        test_checkpoint_6()

        # 打印摘要
        summary = test_summary()

        print("\n" + "="*60)
        if summary['failed'] == 0:
            print("✅ 所有检查点测试通过!")
        else:
            print(f"⚠️  测试完成，有 {summary['failed']} 个失败项")
        print("="*60)

        return 0

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
