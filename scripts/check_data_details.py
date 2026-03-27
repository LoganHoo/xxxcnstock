"""显示数据检查详细信息"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
from scripts.tomorrow_picks import ConfigManager
from services.data_validator import DataValidator

# 加载配置
config_path = project_root / "config" / "xcn_comm.yaml"
config_manager = ConfigManager(str(config_path))

# 获取数据路径
data_path = config_manager.get_data_path()
print("=" * 70)
print("📊 数据检查详细信息")
print("=" * 70)

print(f"\n📁 数据路径: {data_path}")

# 加载数据
df = pl.read_parquet(data_path)
print(f"📈 数据记录数: {len(df)}")

# 获取验证配置
validation_config = config_manager.config.get('recommendation', {}).get('data_validation', {})
print(f"\n⚙️ 验证配置:")
print(f"  - 最小记录数: {validation_config.get('min_records', 1000)}")
print(f"  - 最大数据天数: {validation_config.get('max_age_days', 7)}")
print(f"  - 价格范围: {validation_config.get('price_range', [0.1, 1000])}")
print(f"  - 涨跌幅范围: {validation_config.get('change_pct_range', [-20, 20])}")
print(f"  - 一致性检查: {validation_config.get('check_consistency', True)}")

# 执行验证
validator = DataValidator(validation_config)
results = validator.validate_all(df)

print("\n" + "=" * 70)
print("📋 检查结果")
print("=" * 70)

# 完整性检查
completeness = results['completeness']
print(f"\n✅ 完整性检查:")
print(f"  - 通过: {'✓' if completeness['passed'] else '✗'}")
print(f"  - 记录数: {completeness['record_count']}")
print(f"  - 最小要求: {completeness['min_records']}")
print(f"  - 缺失字段: {completeness['missing_fields']}")

# 有效性检查
validity = results['validity']
print(f"\n✅ 有效性检查:")
print(f"  - 通过: {'✓' if validity['passed'] else '✗'}")
print(f"  - 价格异常数: {validity['invalid_price_count']}")
print(f"  - 涨跌幅异常数: {validity['invalid_change_count']}")

# 新鲜度检查
freshness = results['freshness']
print(f"\n✅ 新鲜度检查:")
print(f"  - 通过: {'✓' if freshness['passed'] else '✗'}")
print(f"  - 数据天数: {freshness.get('age_days', 'N/A')}")
print(f"  - 最大允许天数: {validation_config.get('max_age_days', 7)}")
if 'message' in freshness:
    print(f"  - 消息: {freshness['message']}")

# 一致性检查
if 'consistency' in results:
    consistency = results['consistency']
    print(f"\n✅ 一致性检查:")
    print(f"  - 通过: {'✓' if consistency['passed'] else '✗'}")
    if 'inconsistent_count' in consistency:
        print(f"  - 不一致记录数: {consistency['inconsistent_count']}")
    if 'message' in consistency:
        print(f"  - 消息: {consistency['message']}")

# 总体结果
print("\n" + "=" * 70)
print(f"🎯 总体结果: {'✅ 通过' if results['passed'] else '❌ 未通过'}")
print("=" * 70)

# 数据统计
print(f"\n📊 数据统计:")
print(f"  - S级股票: {len(df.filter(pl.col('grade') == 'S'))}")
print(f"  - A级股票: {len(df.filter(pl.col('grade') == 'A'))}")
print(f"  - B级股票: {len(df.filter(pl.col('grade') == 'B'))}")
print(f"  - C级股票: {len(df.filter(pl.col('grade') == 'C'))}")
print(f"  - 今日上涨: {len(df.filter(pl.col('change_pct') > 0))}")
print(f"  - 今日下跌: {len(df.filter(pl.col('change_pct') < 0))}")
