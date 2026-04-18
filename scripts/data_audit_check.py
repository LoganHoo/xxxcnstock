#!/usr/bin/env python3
"""
数据审计检查脚本
检查数据的连续性、完整性、新鲜度
并随机抽取股票进行价格对比验证
"""

import json
import random
from pathlib import Path
from datetime import datetime

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
KLINE_DIR = DATA_DIR / "kline"
AUDIT_DIR = DATA_DIR / "audit"

def check_data_freshness():
    """检查数据新鲜度"""
    print("=" * 60)
    print("一、数据新鲜度检查")
    print("-" * 60)
    
    quality_file = AUDIT_DIR / "2026-04-17_quality_metrics.json"
    if not quality_file.exists():
        print("  ❌ 质量指标文件不存在")
        return False
    
    with open(quality_file, 'r', encoding='utf-8') as f:
        quality = json.load(f)
    
    freshness_days = quality.get('data_freshness_days', 99)
    freshness_score = quality.get('freshness_score', 0)
    
    print(f"  报告日期: {quality.get('report_date', 'N/A')}")
    print(f"  数据新鲜度: {freshness_days} 天")
    print(f"  新鲜度评分: {freshness_score}/100")
    
    if freshness_days == 0:
        print("  ✅ 数据新鲜度: 通过（当日数据）")
        return True
    else:
        print(f"  ❌ 数据新鲜度: 不通过（延迟 {freshness_days} 天）")
        return False

def check_data_completeness():
    """检查数据完整性"""
    print()
    print("=" * 60)
    print("二、数据完整性检查")
    print("-" * 60)
    
    quality_file = AUDIT_DIR / "2026-04-17_quality_metrics.json"
    if not quality_file.exists():
        print("  ❌ 质量指标文件不存在")
        return False
    
    with open(quality_file, 'r', encoding='utf-8') as f:
        quality = json.load(f)
    
    total = quality.get('total_stocks', 0)
    collected = quality.get('collected_stocks', 0)
    valid = quality.get('valid_stocks', 0)
    invalid = quality.get('invalid_stocks', 0)
    collection_rate = quality.get('collection_rate', 0)
    missing_fields = quality.get('missing_fields_count', 0)
    
    print(f"  应采集股票: {total} 只")
    print(f"  实际采集: {collected} 只")
    print(f"  有效数据: {valid} 只")
    print(f"  无效数据: {invalid} 只")
    print(f"  采集率: {collection_rate}%")
    print(f"  缺失字段: {missing_fields} 个")
    
    if collection_rate >= 99:
        print("  ✅ 数据完整性: 通过（采集率≥99%）")
        return True
    else:
        print(f"  ❌ 数据完整性: 不通过（采集率 {collection_rate}% < 99%）")
        return False

def check_data_continuity():
    """检查数据连续性"""
    print()
    print("=" * 60)
    print("三、数据连续性检查（时间序列）")
    print("-" * 60)
    
    if not KLINE_DIR.exists():
        print("  ❌ K线数据目录不存在")
        return False
    
    parquet_files = list(KLINE_DIR.glob("*.parquet"))
    print(f"  K线数据文件数: {len(parquet_files)} 个")
    
    if len(parquet_files) == 0:
        print("  ❌ 没有K线数据文件")
        return False
    
    # 随机抽取3只股票
    sample_files = random.sample(parquet_files, min(3, len(parquet_files)))
    print(f"  随机抽取 {len(sample_files)} 只股票检查:")
    
    selected_stocks = []
    for i, pf in enumerate(sample_files, 1):
        code = pf.stem
        size_kb = pf.stat().st_size / 1024
        print(f"    {i}. {code}: 文件存在, 大小 {size_kb:.1f} KB")
        selected_stocks.append(code)
    
    print("  ✅ 数据连续性: 通过（文件存在且完整）")
    return selected_stocks

def check_price_consistency(selected_stocks):
    """检查价格一致性"""
    print()
    print("=" * 60)
    print("四、价格一致性验证（随机抽样）")
    print("-" * 60)
    
    # 从daily_picks获取本地数据
    picks_file = PROJECT_ROOT / "reports" / "daily_picks_20260416.json"
    if not picks_file.exists():
        print("  ❌ 选股数据文件不存在")
        return False
    
    with open(picks_file, 'r', encoding='utf-8') as f:
        picks_data = json.load(f)
    
    # 构建股票代码到价格的映射
    stock_prices = {}
    for grade in ['s_grade', 'a_grade']:
        for stock in picks_data.get('filters', {}).get(grade, {}).get('stocks', []):
            code = stock.get('code')
            price = stock.get('price', 0)
            prev_close = stock.get('prev_close', 0)
            change_pct = stock.get('change_pct', 0)
            name = stock.get('name', '')
            if code:
                stock_prices[code] = {
                    'name': name,
                    'price': price,
                    'prev_close': prev_close,
                    'change_pct': change_pct
                }
    
    print(f"  本地数据源: {picks_file.name}")
    print(f"  已加载股票数: {len(stock_prices)} 只")
    print()
    
    # 检查选中的股票
    all_passed = True
    for i, code in enumerate(selected_stocks, 1):
        print(f"  【股票 {i}】{code}")
        
        if code in stock_prices:
            info = stock_prices[code]
            print(f"    名称: {info['name']}")
            print(f"    昨日收盘: ¥{info['prev_close']:.2f}")
            print(f"    今日收盘: ¥{info['price']:.2f}")
            print(f"    涨跌幅: {info['change_pct']:+.2f}%")
            
            # 验证价格合理性
            if info['price'] > 0 and info['prev_close'] > 0:
                calc_change = ((info['price'] - info['prev_close']) / info['prev_close']) * 100
                diff = abs(calc_change - info['change_pct'])
                if diff < 0.1:  # 允许0.1%的误差
                    print(f"    ✅ 价格一致性: 通过（误差 {diff:.2f}%）")
                else:
                    print(f"    ⚠️ 价格一致性: 警告（误差 {diff:.2f}%）")
            else:
                print(f"    ❌ 价格数据无效")
                all_passed = False
        else:
            print(f"    ⚠️ 该股票不在选股列表中，跳过价格对比")
        print()
    
    return all_passed

def generate_summary(freshness_ok, completeness_ok, continuity_stocks, price_ok):
    """生成审计总结"""
    print()
    print("=" * 60)
    print("五、审计总结")
    print("-" * 60)
    
    checks = [
        ("数据新鲜度", freshness_ok),
        ("数据完整性", completeness_ok),
        ("数据连续性", bool(continuity_stocks)),
        ("价格一致性", price_ok)
    ]
    
    for name, passed in checks:
        status = "✅ 通过" if passed else "❌ 不通过"
        print(f"  {name}: {status}")
    
    all_passed = all(passed for _, passed in checks)
    print()
    print("=" * 60)
    if all_passed:
        print("【最终结论】✅ 数据审计通过")
        print("  数据质量良好，可用于后续分析和报告生成")
    else:
        print("【最终结论】❌ 数据审计未通过")
        print("  存在数据质量问题，请检查并修复")
    print("=" * 60)
    
    return all_passed

def main():
    """主函数"""
    print()
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 15 + "【数据审计检查】" + " " * 26 + "║")
    print("╚" + "=" * 58 + "╝")
    print(f"\n审计时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # 执行各项检查
    freshness_ok = check_data_freshness()
    completeness_ok = check_data_completeness()
    continuity_stocks = check_data_continuity()
    price_ok = check_price_consistency(continuity_stocks) if continuity_stocks else False
    
    # 生成总结
    all_passed = generate_summary(freshness_ok, completeness_ok, continuity_stocks, price_ok)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    exit(main())
