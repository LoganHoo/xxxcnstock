#!/usr/bin/env python3
"""
工作流验证脚本

验证完整的选股流水线是否正常工作：
1. 数据新鲜度检查
2. 数据完整性验证
3. 因子计算验证
4. 选股策略验证
5. 报告生成验证

使用方式:
    python scripts/pipeline/validate_workflow.py
    python scripts/pipeline/validate_workflow.py --verbose
"""
import sys
import argparse
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("workflow_validator")

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import pandas as pd


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='验证选股工作流')
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='详细输出'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='目标日期 (默认今天)'
    )
    return parser.parse_args()


def validate_data_freshness(target_date: str) -> Tuple[bool, Dict]:
    """验证数据新鲜度"""
    logger.info("=" * 60)
    logger.info("📊 步骤1: 验证数据新鲜度")
    logger.info("=" * 60)
    
    try:
        kline_dir = Path('data/kline')
        if not kline_dir.exists():
            return False, {'error': 'K线目录不存在'}
        
        parquet_files = list(kline_dir.glob('*.parquet'))
        total = len(parquet_files)
        
        if total == 0:
            return False, {'error': '没有K线文件'}
        
        # 抽样检查最新日期
        sample_size = min(100, total)
        latest_count = 0
        
        for f in parquet_files[:sample_size]:
            try:
                df = pl.read_parquet(f)
                if 'date' in df.columns:
                    latest = str(df['date'].max())
                elif 'trade_date' in df.columns:
                    latest = str(df['trade_date'].max())
                else:
                    continue
                
                if latest == target_date:
                    latest_count += 1
            except:
                pass
        
        freshness_rate = latest_count / sample_size * 100
        
        result = {
            'total_files': total,
            'sample_size': sample_size,
            'latest_count': latest_count,
            'freshness_rate': freshness_rate,
            'target_date': target_date
        }
        
        logger.info(f"  总文件数: {total}")
        logger.info(f"  抽样检查: {sample_size}")
        logger.info(f"  最新数据: {latest_count} ({freshness_rate:.1f}%)")
        
        # 新鲜度>95%认为通过
        passed = freshness_rate >= 95.0
        
        if passed:
            logger.info("  ✅ 数据新鲜度检查通过")
        else:
            logger.warning(f"  ⚠️  数据新鲜度不足: {freshness_rate:.1f}%")
        
        return passed, result
        
    except Exception as e:
        logger.error(f"  ❌ 数据新鲜度检查失败: {e}")
        return False, {'error': str(e)}


def validate_data_integrity() -> Tuple[bool, Dict]:
    """验证数据完整性"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 步骤2: 验证数据完整性")
    logger.info("=" * 60)
    
    try:
        kline_dir = Path('data/kline')
        parquet_files = list(kline_dir.glob('*.parquet'))
        
        valid_count = 0
        invalid_count = 0
        errors = []
        
        # 抽样检查
        sample_files = parquet_files[:50]
        
        for f in sample_files:
            try:
                df = pl.read_parquet(f)
                
                # 检查必要列
                required_cols = ['code', 'date', 'open', 'high', 'low', 'close', 'volume']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    invalid_count += 1
                    errors.append(f"{f.stem}: 缺少列 {missing_cols}")
                    continue
                
                # 检查数据行数
                if len(df) < 10:
                    invalid_count += 1
                    errors.append(f"{f.stem}: 数据行数不足 ({len(df)}行)")
                    continue
                
                valid_count += 1
                
            except Exception as e:
                invalid_count += 1
                errors.append(f"{f.stem}: {str(e)}")
        
        result = {
            'checked': len(sample_files),
            'valid': valid_count,
            'invalid': invalid_count,
            'errors': errors[:5]  # 只保留前5个错误
        }
        
        logger.info(f"  检查文件数: {len(sample_files)}")
        logger.info(f"  有效: {valid_count}")
        logger.info(f"  无效: {invalid_count}")
        
        if errors:
            logger.info(f"  错误示例:")
            for error in errors[:3]:
                logger.info(f"    - {error}")
        
        passed = invalid_count == 0
        
        if passed:
            logger.info("  ✅ 数据完整性检查通过")
        else:
            logger.warning(f"  ⚠️  发现 {invalid_count} 个无效文件")
        
        return passed, result
        
    except Exception as e:
        logger.error(f"  ❌ 数据完整性检查失败: {e}")
        return False, {'error': str(e)}


def validate_factor_calculation() -> Tuple[bool, Dict]:
    """验证因子计算"""
    logger.info("\n" + "=" * 60)
    logger.info("🔢 步骤3: 验证因子计算")
    logger.info("=" * 60)
    
    try:
        from core.indicators.technical import TechnicalIndicators
        
        # 读取样本数据
        sample_file = Path('data/kline/000001.parquet')
        if not sample_file.exists():
            return False, {'error': '样本文件不存在'}
        
        df = pl.read_parquet(sample_file)
        
        # 测试指标计算
        indicators_tested = []
        
        # 1. EMA
        try:
            ema = TechnicalIndicators.calculate_ema(df['close'].to_pandas(), 20)
            indicators_tested.append('EMA')
        except Exception as e:
            logger.warning(f"  EMA计算失败: {e}")
        
        # 2. MACD
        try:
            macd_line, signal_line, hist = TechnicalIndicators.calculate_macd(df['close'].to_pandas())
            indicators_tested.append('MACD')
        except Exception as e:
            logger.warning(f"  MACD计算失败: {e}")
        
        # 3. RSI
        try:
            rsi = TechnicalIndicators.calculate_rsi(df['close'].to_pandas(), 14)
            indicators_tested.append('RSI')
        except Exception as e:
            logger.warning(f"  RSI计算失败: {e}")
        
        result = {
            'indicators_tested': indicators_tested,
            'total': 3,
            'passed': len(indicators_tested)
        }
        
        logger.info(f"  测试指标: {indicators_tested}")
        logger.info(f"  通过: {len(indicators_tested)}/3")
        
        passed = len(indicators_tested) >= 2  # 至少2个指标通过
        
        if passed:
            logger.info("  ✅ 因子计算检查通过")
        else:
            logger.warning("  ⚠️  因子计算检查未通过")
        
        return passed, result
        
    except Exception as e:
        logger.error(f"  ❌ 因子计算检查失败: {e}")
        return False, {'error': str(e)}


def validate_selection_strategy() -> Tuple[bool, Dict]:
    """验证选股策略"""
    logger.info("\n" + "=" * 60)
    logger.info("🎯 步骤4: 验证选股策略")
    logger.info("=" * 60)
    
    try:
        # 检查策略配置文件
        strategy_configs = [
            'config/strategies/champion.yaml',
            'config/factors_config.yaml',
            'config/filters_config.yaml'
        ]
        
        existing_configs = []
        missing_configs = []
        
        for config_path in strategy_configs:
            if Path(config_path).exists():
                existing_configs.append(config_path)
            else:
                missing_configs.append(config_path)
        
        # 检查策略执行脚本
        strategy_scripts = [
            'scripts/pipeline/run_stock_selection.py',
            'scripts/pipeline/run_daily_workflow.py'
        ]
        
        existing_scripts = []
        for script_path in strategy_scripts:
            if Path(script_path).exists():
                existing_scripts.append(script_path)
        
        result = {
            'configs': {
                'existing': existing_configs,
                'missing': missing_configs
            },
            'scripts': {
                'existing': existing_scripts
            }
        }
        
        logger.info(f"  配置文件: {len(existing_configs)}/{len(strategy_configs)}")
        logger.info(f"  执行脚本: {len(existing_scripts)}/{len(strategy_scripts)}")
        
        if missing_configs:
            logger.info(f"  缺失配置: {missing_configs}")
        
        passed = len(existing_configs) >= 2  # 至少2个配置存在
        
        if passed:
            logger.info("  ✅ 选股策略检查通过")
        else:
            logger.warning("  ⚠️  选股策略检查未通过")
        
        return passed, result
        
    except Exception as e:
        logger.error(f"  ❌ 选股策略检查失败: {e}")
        return False, {'error': str(e)}


def validate_report_generation() -> Tuple[bool, Dict]:
    """验证报告生成"""
    logger.info("\n" + "=" * 60)
    logger.info("📄 步骤5: 验证报告生成")
    logger.info("=" * 60)
    
    try:
        # 检查报告目录
        report_dirs = [
            'data/reports',
            'data/test_reports'
        ]
        
        existing_dirs = []
        for dir_path in report_dirs:
            path = Path(dir_path)
            path.mkdir(parents=True, exist_ok=True)
            existing_dirs.append(dir_path)
        
        # 测试生成报告
        test_report = {
            'generated_at': datetime.now().isoformat(),
            'test': True,
            'message': '工作流验证测试报告'
        }
        
        report_path = Path('data/test_reports/workflow_validation_test.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(test_report, f, ensure_ascii=False, indent=2)
        
        result = {
            'report_dirs': existing_dirs,
            'test_report_generated': report_path.exists(),
            'test_report_path': str(report_path)
        }
        
        logger.info(f"  报告目录: {existing_dirs}")
        logger.info(f"  测试报告: {'✅ 生成成功' if report_path.exists() else '❌ 生成失败'}")
        
        passed = report_path.exists()
        
        if passed:
            logger.info("  ✅ 报告生成检查通过")
        else:
            logger.warning("  ⚠️  报告生成检查未通过")
        
        return passed, result
        
    except Exception as e:
        logger.error(f"  ❌ 报告生成检查失败: {e}")
        return False, {'error': str(e)}


def generate_validation_report(results: List[Tuple[str, bool, Dict]], output_dir: Path) -> Path:
    """生成验证报告"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 计算统计
    total = len(results)
    passed = sum(1 for _, success, _ in results if success)
    failed = total - passed
    
    # JSON报告
    json_report = {
        'validated_at': datetime.now().isoformat(),
        'summary': {
            'total': total,
            'passed': passed,
            'failed': failed,
            'success_rate': passed / total * 100 if total > 0 else 0
        },
        'results': [
            {
                'step': name,
                'passed': success,
                'details': details
            }
            for name, success, details in results
        ]
    }
    
    json_path = output_dir / f'workflow_validation_{timestamp}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_report, f, ensure_ascii=False, indent=2)
    
    # Markdown报告
    md_path = output_dir / f'workflow_validation_{timestamp}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# 工作流验证报告\n\n')
        f.write(f'验证时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        f.write('## 摘要\n\n')
        f.write(f'- 总检查项: {total}\n')
        f.write(f'- 通过: {passed} ✅\n')
        f.write(f'- 失败: {failed} ❌\n')
        f.write(f'- 成功率: {passed/total*100:.1f}%\n\n')
        
        f.write('## 详细结果\n\n')
        for name, success, details in results:
            status = '✅' if success else '❌'
            f.write(f'### {status} {name}\n\n')
            
            if isinstance(details, dict):
                for key, value in details.items():
                    if key != 'errors':
                        f.write(f'- {key}: {value}\n')
                
                if 'errors' in details and details['errors']:
                    f.write('\n**错误:**\n')
                    for error in details['errors'][:5]:
                        f.write(f'- {error}\n')
            
            f.write('\n')
        
        # 结论
        f.write('## 结论\n\n')
        if failed == 0:
            f.write('✅ **所有检查项通过，工作流正常运行**\n')
        elif failed <= 2:
            f.write('⚠️ **部分检查项未通过，工作流基本可用但需关注**\n')
        else:
            f.write('❌ **多个检查项失败，工作流需要修复**\n')
    
    return md_path


def main():
    """主函数"""
    args = parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print('=' * 60)
    print('🧪 选股工作流验证')
    print('=' * 60)
    print(f'目标日期: {args.date}')
    print('=' * 60)
    
    # 执行验证步骤
    results = []
    
    # 步骤1: 数据新鲜度
    success, details = validate_data_freshness(args.date)
    results.append(('数据新鲜度', success, details))
    
    # 步骤2: 数据完整性
    success, details = validate_data_integrity()
    results.append(('数据完整性', success, details))
    
    # 步骤3: 因子计算
    success, details = validate_factor_calculation()
    results.append(('因子计算', success, details))
    
    # 步骤4: 选股策略
    success, details = validate_selection_strategy()
    results.append(('选股策略', success, details))
    
    # 步骤5: 报告生成
    success, details = validate_report_generation()
    results.append(('报告生成', success, details))
    
    # 生成报告
    output_dir = Path('data/test_reports')
    report_path = generate_validation_report(results, output_dir)
    
    # 输出摘要
    print('\n' + '=' * 60)
    print('📊 验证摘要')
    print('=' * 60)
    
    total = len(results)
    passed = sum(1 for _, success, _ in results if success)
    failed = total - passed
    
    print(f'总检查项: {total}')
    print(f'通过: {passed} ✅')
    print(f'失败: {failed} ❌')
    print(f'成功率: {passed/total*100:.1f}%')
    
    print('\n详细结果:')
    for name, success, _ in results:
        status = '✅' if success else '❌'
        print(f'  {status} {name}')
    
    print(f'\n📄 报告已保存: {report_path}')
    
    # 返回退出码
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
