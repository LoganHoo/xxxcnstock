#!/usr/bin/env python3
"""
09:26任务链测试脚本
================================================================================
测试内容：
1. 资源保障脚本各phase执行
2. 核心任务守护程序检查
3. 依赖链完整性验证
================================================================================
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subprocess
import json
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent


def run_command(cmd, description, timeout=300):
    """运行命令并返回结果"""
    print(f"\n{'='*70}")
    print(f"测试: {description}")
    print(f"命令: {cmd}")
    print(f"{'='*70}")
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.stdout:
            print(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
        
        if result.returncode == 0:
            print(f"\n✅ {description} - 通过")
            return True
        else:
            print(f"\n❌ {description} - 失败 (exit code: {result.returncode})")
            if result.stderr:
                print(f"错误: {result.stderr[-500:]}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"\n⏱️ {description} - 超时")
        return False
    except Exception as e:
        print(f"\n❌ {description} - 异常: {e}")
        return False


def test_resource_guardian_prepare():
    """测试资源保障 - prepare阶段"""
    return run_command(
        "python3 scripts/pipeline/fund_behavior_resource_guardian.py --phase prepare",
        "资源保障 - prepare阶段 (预热+锁定)"
    )


def test_resource_guardian_validate():
    """测试资源保障 - validate阶段"""
    return run_command(
        "python3 scripts/pipeline/fund_behavior_resource_guardian.py --phase validate",
        "资源保障 - validate阶段 (检查+快速通道)"
    )


def test_core_guardian_check():
    """测试核心守护程序 - 检查"""
    return run_command(
        "python3 scripts/core_task_guardian.py check",
        "核心守护程序 - 前置检查"
    )


def test_core_guardian_status():
    """测试核心守护程序 - 状态"""
    return run_command(
        "python3 scripts/core_task_guardian.py status",
        "核心守护程序 - 状态查询"
    )


def test_cron_config():
    """测试cron配置"""
    return run_command(
        "python3 scripts/validate_cron_config.py",
        "Cron配置验证"
    )


def test_script_existence():
    """测试脚本存在性"""
    return run_command(
        "python3 scripts/check_scripts_existence.py",
        "脚本存在性检查"
    )


def generate_test_report(results):
    """生成测试报告"""
    report = {
        "timestamp": datetime.now().isoformat(),
        "test_results": results,
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results.values() if r),
            "failed": sum(1 for r in results.values() if not r)
        }
    }
    
    report_file = project_root / "logs" / "test_0926_task_chain_report.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    return report_file


def main():
    """主函数"""
    print("="*70)
    print("09:26任务链完整测试")
    print("="*70)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"项目路径: {project_root}")
    print("="*70)
    
    # 执行所有测试
    results = {}
    
    # 1. 基础检查
    results["script_existence"] = test_script_existence()
    results["cron_config"] = test_cron_config()
    
    # 2. 资源保障测试
    results["resource_prepare"] = test_resource_guardian_prepare()
    results["resource_validate"] = test_resource_guardian_validate()
    
    # 3. 核心守护程序测试
    results["core_guardian_check"] = test_core_guardian_check()
    results["core_guardian_status"] = test_core_guardian_status()
    
    # 生成报告
    report_file = generate_test_report(results)
    
    # 汇总
    print("\n" + "="*70)
    print("测试结果汇总")
    print("="*70)
    
    for name, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {status}: {name}")
    
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    
    print("\n" + "="*70)
    print(f"总计: {passed}/{total} 通过 ({passed/total*100:.1f}%)")
    print(f"报告已保存: {report_file}")
    print("="*70)
    
    if passed == total:
        print("\n🎉 所有测试通过！09:26任务链已就绪")
        return 0
    else:
        print(f"\n⚠️ {total - passed} 个测试失败，请检查")
        return 1


if __name__ == "__main__":
    sys.exit(main())
