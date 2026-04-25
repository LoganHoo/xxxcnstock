#!/usr/bin/env python3
"""
分析数据采集失败原因
检查失败股票的共同特征
"""
import sys
import json
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))


def analyze_failed_stocks():
    """分析失败股票"""
    print("\n" + "=" * 70)
    print("数据采集失败原因分析")
    print("=" * 70)
    
    # 1. 检查K线数据目录
    kline_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
    if not kline_dir.exists():
        print("\n❌ K线数据目录不存在")
        return
    
    # 2. 获取所有股票代码
    existing_files = list(kline_dir.glob('*.parquet'))
    existing_codes = {f.stem for f in existing_files}
    
    print(f"\n📊 数据概况:")
    print(f"   已有数据文件: {len(existing_files)} 个")
    
    # 3. 尝试从断点文件获取失败列表
    checkpoint_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/checkpoints')
    failed_codes = set()
    
    # 检查最新的断点文件
    checkpoint_files = sorted(checkpoint_dir.glob('*.json'), key=lambda x: x.stat().st_mtime, reverse=True)
    
    for cp_file in checkpoint_files[:5]:  # 检查最近5个
        try:
            with open(cp_file, 'r') as f:
                data = json.load(f)
                if 'failed_items' in data:
                    failed_codes.update(data['failed_items'])
        except:
            pass
    
    # 4. 如果没有找到失败列表，尝试从其他来源推断
    if not failed_codes:
        # 检查股票列表
        stock_list_path = Path('/Volumes/Xdata/workstation/xxxcnstock/data/stock_list.parquet')
        if stock_list_path.exists():
            try:
                import polars as pl
                stock_list = pl.read_parquet(stock_list_path)
                all_codes = set(stock_list['code'].to_list())
                failed_codes = all_codes - existing_codes
            except Exception as e:
                print(f"   无法读取股票列表: {e}")
    
    if failed_codes:
        print(f"   失败股票数量: {len(failed_codes)} 只")
        
        # 5. 分析失败股票的特征
        print("\n🔍 失败股票特征分析:")
        
        # 按代码前缀分类
        prefixes = Counter()
        for code in failed_codes:
            if code.startswith('6'):
                prefixes['沪市主板(600/601/603/605)'] += 1
            elif code.startswith('688'):
                prefixes['科创板'] += 1
            elif code.startswith('000'):
                prefixes['深市主板(000)'] += 1
            elif code.startswith('001'):
                prefixes['深市主板(001)'] += 1
            elif code.startswith('002'):
                prefixes['中小板'] += 1
            elif code.startswith('003'):
                prefixes['深市主板(003)'] += 1
            elif code.startswith('300'):
                prefixes['创业板'] += 1
            elif code.startswith('301'):
                prefixes['创业板(301)'] += 1
            elif code.startswith('8') or code.startswith('4'):
                prefixes['北交所/新三板'] += 1
            else:
                prefixes['其他'] += 1
        
        print("\n   板块分布:")
        for prefix, count in sorted(prefixes.items(), key=lambda x: -x[1]):
            print(f"      {prefix}: {count} 只")
        
        # 6. 可能的失败原因
        print("\n📋 可能的失败原因:")
        
        reasons = []
        
        # 检查是否有北交所/新三板股票
        if prefixes.get('北交所/新三板', 0) > 0:
            reasons.append(("北交所/新三板股票", "Baostock可能不支持这些股票的数据"))
        
        # 检查是否有大量科创板/创业板
        if prefixes.get('科创板', 0) > 50 or prefixes.get('创业板', 0) > 100:
            reasons.append(("大量科创板/创业板", "这些板块数据可能不完整"))
        
        # 检查是否有退市股票
        if any('退市' in str(code) or '*ST' in str(code) for code in failed_codes):
            reasons.append(("退市/ST股票", "已退市或即将退市的股票无数据"))
        
        # 检查是否为新上市股票
        recent_codes = [c for c in failed_codes if c.startswith('60') and len(c) == 6]
        if len(recent_codes) > 10:
            reasons.append(("新上市股票", "新上市股票历史数据较短"))
        
        if reasons:
            for reason, detail in reasons:
                print(f"   ⚠️  {reason}: {detail}")
        else:
            print("   ℹ️  未识别到明显的失败模式")
        
        # 7. 显示部分失败股票代码
        print("\n📄 部分失败股票代码 (前20只):")
        for code in sorted(list(failed_codes))[:20]:
            print(f"      {code}")
        if len(failed_codes) > 20:
            print(f"      ... 还有 {len(failed_codes) - 20} 只")
    
    else:
        print("   ℹ️  未找到失败股票记录")
    
    # 8. 建议
    print("\n💡 建议:")
    print("   1. 运行断点续传模式补充失败股票:")
    print("      python scripts/pipeline/data_collect.py --retry")
    print("   2. 检查数据源配置:")
    print("      cat config/datasource.yaml")
    print("   3. 检查网络连接:")
    print("      python scripts/test_baostock_only.py")
    print("   4. 如果大量失败，可能是数据源问题，考虑更换数据源")
    
    print("\n" + "=" * 70)


def check_data_completeness():
    """检查数据完整性"""
    print("\n" + "=" * 70)
    print("数据完整性检查")
    print("=" * 70)
    
    kline_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
    if not kline_dir.exists():
        print("\n❌ K线数据目录不存在")
        return
    
    files = list(kline_dir.glob('*.parquet'))
    print(f"\n📁 数据文件数量: {len(files)}")
    
    # 检查文件大小分布
    sizes = [f.stat().st_size for f in files]
    if sizes:
        import statistics
        print(f"\n📊 文件大小统计:")
        print(f"   平均大小: {statistics.mean(sizes)/1024:.1f} KB")
        print(f"   最小大小: {min(sizes)/1024:.1f} KB")
        print(f"   最大大小: {max(sizes)/1024:.1f} KB")
        
        # 检查异常小的文件
        small_files = [f for f in files if f.stat().st_size < 1024]  # 小于1KB
        if small_files:
            print(f"\n⚠️  异常小的文件 ({len(small_files)} 个):")
            for f in small_files[:10]:
                print(f"      {f.name}: {f.stat().st_size} bytes")
    
    print("\n" + "=" * 70)


if __name__ == '__main__':
    analyze_failed_stocks()
    check_data_completeness()
