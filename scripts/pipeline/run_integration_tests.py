#!/usr/bin/env python3
"""
集成测试工作流运行器

统一运行所有集成测试，生成测试报告

使用方式:
    python scripts/pipeline/run_integration_tests.py
    python scripts/pipeline/run_integration_tests.py --quick
    python scripts/pipeline/run_integration_tests.py --full
"""
import sys
import argparse
import subprocess
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='运行集成测试工作流')
    parser.add_argument(
        '--quick',
        action='store_true',
        help='快速模式：只运行核心测试'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='完整模式：运行所有测试包括性能测试'
    )
    parser.add_argument(
        '--category',
        type=str,
        choices=['data', 'factor', 'selection', 'report', 'all'],
        default='all',
        help='测试类别'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='data/test_reports',
        help='报告输出目录'
    )
    return parser.parse_args()


def run_test_suite(test_path: str, markers: str = "") -> Dict:
    """运行测试套件"""
    cmd = f"pytest {test_path} -v --tb=short"
    if markers:
        cmd += f" -m {markers}"
    
    print(f"\n{'='*60}")
    print(f"运行: {test_path}")
    print(f"{'='*60}")
    
    result = subprocess.run(
        cmd.split(),
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT
    )
    
    success = result.returncode == 0
    
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    
    return {
        'path': test_path,
        'success': success,
        'returncode': result.returncode,
        'output': result.stdout,
        'errors': result.stderr if result.stderr else ""
    }


def run_data_freshness_check() -> Dict:
    """运行数据新鲜度检查"""
    print(f"\n{'='*60}")
    print("运行数据新鲜度检查")
    print(f"{'='*60}")
    
    from scripts.pipeline.check_data_freshness import check_data_freshness
    
    today = datetime.now().strftime('%Y-%m-%d')
    result = check_data_freshness(target_date=today, max_age_days=3)
    
    success = result.get('up_to_date_rate', 0) >= 95.0
    
    print(f"总股票数: {result.get('total', 0)}")
    print(f"最新数据: {result.get('up_to_date', 0)} ({result.get('up_to_date_rate', 0):.1f}%)")
    print(f"过期数据: {result.get('outdated', 0)}")
    print(f"缺失数据: {result.get('missing', 0)}")
    
    return {
        'name': '数据新鲜度检查',
        'success': success,
        'details': result
    }


def run_data_validation() -> Dict:
    """运行数据验证"""
    print(f"\n{'='*60}")
    print("运行数据验证")
    print(f"{'='*60}")
    
    import polars as pl
    from pathlib import Path
    
    kline_dir = Path('data/kline')
    if not kline_dir.exists():
        return {
            'name': '数据验证',
            'success': False,
            'error': 'K线目录不存在'
        }
    
    parquet_files = list(kline_dir.glob('*.parquet'))
    
    valid_count = 0
    invalid_count = 0
    errors = []
    
    for f in parquet_files[:100]:  # 抽样检查
        try:
            df = pl.read_parquet(f)
            if len(df) > 0 and 'date' in df.columns:
                valid_count += 1
            else:
                invalid_count += 1
                errors.append(f"{f.stem}: 数据为空或缺少列")
        except Exception as e:
            invalid_count += 1
            errors.append(f"{f.stem}: {str(e)}")
    
    success = invalid_count == 0
    
    print(f"检查文件数: {len(parquet_files[:100])}")
    print(f"有效: {valid_count}")
    print(f"无效: {invalid_count}")
    
    return {
        'name': '数据验证',
        'success': success,
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'errors': errors[:10]  # 只保留前10个错误
    }


def run_workflow_integration_tests(quick: bool = False) -> List[Dict]:
    """运行工作流集成测试"""
    results = []
    
    # 核心测试
    core_tests = [
        ('tests/integration/test_workflow_integration.py::TestDataFreshnessWorkflow', '数据新鲜度'),
        ('tests/integration/test_workflow_integration.py::TestDataCollectionWorkflow', '数据采集'),
        ('tests/integration/test_workflow_integration.py::TestFactorCalculationWorkflow', '因子计算'),
    ]
    
    # 完整测试
    full_tests = [
        ('tests/integration/test_workflow_integration.py::TestStockSelectionWorkflow', '选股策略'),
        ('tests/integration/test_workflow_integration.py::TestReportGenerationWorkflow', '报告生成'),
        ('tests/integration/test_workflow_integration.py::TestEndToEndWorkflow', '端到端'),
    ]
    
    tests_to_run = core_tests if quick else core_tests + full_tests
    
    for test_path, name in tests_to_run:
        result = run_test_suite(test_path)
        results.append({
            'name': name,
            'success': result['success'],
            'details': result
        })
    
    return results


def generate_report(results: List[Dict], output_dir: Path) -> Path:
    """生成测试报告"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total': len(results),
            'passed': sum(1 for r in results if r['success']),
            'failed': sum(1 for r in results if not r['success'])
        },
        'results': results
    }
    
    # JSON报告
    json_path = output_dir / f'integration_test_report_{datetime.now():%Y%m%d_%H%M%S}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # Markdown报告
    md_path = output_dir / f'integration_test_report_{datetime.now():%Y%m%d_%H%M%S}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# 集成测试报告\n\n')
        f.write(f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        f.write('## 摘要\n\n')
        f.write(f'- 总测试数: {report["summary"]["total"]}\n')
        f.write(f'- 通过: {report["summary"]["passed"]} ✅\n')
        f.write(f'- 失败: {report["summary"]["failed"]} ❌\n')
        f.write(f'- 成功率: {report["summary"]["passed"]/report["summary"]["total"]*100:.1f}%\n\n')
        
        f.write('## 详细结果\n\n')
        for result in results:
            status = '✅' if result['success'] else '❌'
            f.write(f'### {status} {result["name"]}\n\n')
            
            if 'details' in result and isinstance(result['details'], dict):
                for key, value in result['details'].items():
                    if key not in ['output', 'errors']:
                        f.write(f'- {key}: {value}\n')
            
            f.write('\n')
    
    return md_path


def main():
    """主函数"""
    args = parse_args()
    
    print('='*60)
    print('🧪 集成测试工作流')
    print('='*60)
    print(f'模式: {"快速" if args.quick else "完整"}')
    print(f'类别: {args.category}')
    print('='*60)
    
    all_results = []
    
    # 1. 数据新鲜度检查
    if args.category in ['data', 'all']:
        all_results.append(run_data_freshness_check())
        all_results.append(run_data_validation())
    
    # 2. 工作流集成测试
    if args.category in ['data', 'factor', 'selection', 'all']:
        workflow_results = run_workflow_integration_tests(quick=args.quick)
        all_results.extend(workflow_results)
    
    # 3. 生成报告
    output_dir = Path(args.output)
    report_path = generate_report(all_results, output_dir)
    
    # 4. 输出摘要
    print('\n' + '='*60)
    print('📊 测试摘要')
    print('='*60)
    
    passed = sum(1 for r in all_results if r['success'])
    failed = sum(1 for r in all_results if not r['success'])
    
    print(f'总测试数: {len(all_results)}')
    print(f'通过: {passed} ✅')
    print(f'失败: {failed} ❌')
    print(f'成功率: {passed/len(all_results)*100:.1f}%')
    
    print(f'\n📄 报告已保存: {report_path}')
    
    # 5. 返回退出码
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
