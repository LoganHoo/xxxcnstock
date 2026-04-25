#!/usr/bin/env python3
"""
数据采集完整测试套件

运行所有数据采集测试:
1. K线数据全量采集
2. K线数据增量更新
3. 断点续传功能
4. 财务数据采集
5. 市场行为数据采集
6. 公告数据采集

使用方式:
    python scripts/run_all_data_tests.py

输出:
    - 控制台输出测试结果
    - 日志文件保存在 system/ 目录
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# 测试脚本列表
TEST_SCRIPTS = [
    ('K线数据全量采集', 'scripts/test_kline_full_collection.py'),
    ('K线数据增量更新', 'scripts/test_kline_incremental.py'),
    ('断点续传功能', 'scripts/test_checkpoint_resume.py'),
    ('财务数据采集', 'scripts/test_financial_collection.py'),
    ('市场行为数据采集', 'scripts/test_market_behavior.py'),
    ('公告数据采集', 'scripts/test_announcement.py'),
]


def run_test(name: str, script: str) -> bool:
    """运行单个测试脚本"""
    print(f"\n{'='*70}")
    print(f"开始测试: {name}")
    print(f"脚本: {script}")
    print(f"{'='*70}\n")
    
    try:
        result = subprocess.run(
            [sys.executable, script],
            cwd='/Volumes/Xdata/workstation/xxxcnstock',
            capture_output=False,
            text=True,
            timeout=300  # 5分钟超时
        )
        
        success = result.returncode == 0
        
        print(f"\n{'='*70}")
        if success:
            print(f"✅ {name} 测试完成")
        else:
            print(f"❌ {name} 测试失败 (返回码: {result.returncode})")
        print(f"{'='*70}")
        
        return success
        
    except subprocess.TimeoutExpired:
        print(f"\n❌ {name} 测试超时")
        return False
    except Exception as e:
        print(f"\n❌ {name} 测试异常: {e}")
        return False


def main():
    """主函数"""
    print("="*70)
    print("数据采集完整测试套件")
    print("="*70)
    print(f"\n测试开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试项目数: {len(TEST_SCRIPTS)}")
    
    results = []
    
    for name, script in TEST_SCRIPTS:
        success = run_test(name, script)
        results.append((name, success))
    
    # 汇总报告
    print("\n" + "="*70)
    print("测试汇总报告")
    print("="*70)
    print(f"\n测试完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    passed = sum(1 for _, s in results if s)
    total = len(results)
    
    print(f"\n总计: {passed}/{total} 通过 ({passed/total*100:.1f}%)")
    print("\n详细结果:")
    
    for name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"  {status} - {name}")
    
    print("\n" + "="*70)
    
    # 返回码
    return 0 if passed == total else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
