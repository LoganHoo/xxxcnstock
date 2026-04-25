#!/usr/bin/env python3
"""
Kestra 工作流脚本测试

检查所有工作流引用的脚本是否存在且可导入

使用方式:
    python kestra/test_workflow_scripts.py
"""
import os
import sys
import yaml
import importlib.util
from pathlib import Path
from typing import Dict, List, Tuple, Set

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def extract_script_imports(flow_content: str) -> List[str]:
    """从工作流内容中提取脚本导入"""
    imports = []
    lines = flow_content.split('\n')
    
    for line in lines:
        line = line.strip()
        # 匹配 from scripts.xxx import
        if 'from scripts' in line and 'import' in line:
            # 提取模块路径
            parts = line.split()
            if 'from' in parts:
                idx = parts.index('from')
                if idx + 1 < len(parts):
                    module_path = parts[idx + 1]
                    # 转换为文件路径
                    if module_path.startswith('scripts'):
                        imports.append(module_path)
        # 匹配 import scripts.xxx
        elif 'import scripts' in line:
            parts = line.split()
            if 'import' in parts:
                idx = parts.index('import')
                if idx + 1 < len(parts):
                    module_path = parts[idx + 1]
                    if module_path.startswith('scripts'):
                        imports.append(module_path)
    
    return list(set(imports))


def module_to_file_path(module_path: str) -> Path:
    """将模块路径转换为文件路径"""
    # scripts.pipeline.xxx -> scripts/pipeline/xxx.py
    parts = module_path.split('.')
    return Path(*parts).with_suffix('.py')


def check_script_exists(module_path: str) -> Tuple[bool, Path]:
    """检查脚本是否存在"""
    file_path = module_to_file_path(module_path)
    full_path = PROJECT_ROOT / file_path
    return full_path.exists(), full_path


def check_script_importable(module_path: str) -> Tuple[bool, str]:
    """检查脚本是否可导入"""
    file_path = module_to_file_path(module_path)
    full_path = PROJECT_ROOT / file_path
    
    if not full_path.exists():
        return False, "文件不存在"
    
    try:
        spec = importlib.util.spec_from_file_location(
            module_path.replace('.', '_'),
            full_path
        )
        if spec is None or spec.loader is None:
            return False, "无法加载模块"
        
        module = importlib.util.module_from_spec(spec)
        # 不实际执行，只检查语法
        return True, "可导入"
    except SyntaxError as e:
        return False, f"语法错误: {e}"
    except Exception as e:
        return False, f"导入错误: {e}"


def analyze_workflow(flow_file: Path) -> Dict:
    """分析单个工作流"""
    result = {
        'file': flow_file.name,
        'exists': True,
        'valid_yaml': True,
        'imports': [],
        'scripts': []
    }
    
    try:
        with open(flow_file, 'r', encoding='utf-8') as f:
            content = f.read()
            flow = yaml.safe_load(content)
    except yaml.YAMLError as e:
        result['valid_yaml'] = False
        result['yaml_error'] = str(e)
        return result
    except Exception as e:
        result['exists'] = False
        result['error'] = str(e)
        return result
    
    # 提取脚本导入
    imports = extract_script_imports(content)
    result['imports'] = imports
    
    # 检查每个脚本
    for module_path in imports:
        exists, full_path = check_script_exists(module_path)
        importable, import_error = check_script_importable(module_path) if exists else (False, "文件不存在")
        
        result['scripts'].append({
            'module': module_path,
            'file': str(full_path.relative_to(PROJECT_ROOT)) if exists else None,
            'exists': exists,
            'importable': importable,
            'error': None if importable else import_error
        })
    
    return result


def test_all_workflows():
    """测试所有工作流"""
    print("=" * 70)
    print("🔍 Kestra 工作流脚本测试")
    print("=" * 70)
    
    flows_dir = PROJECT_ROOT / 'kestra' / 'flows'
    if not flows_dir.exists():
        print("❌ 工作流目录不存在")
        return False
    
    flow_files = list(flows_dir.glob('*.yml'))
    print(f"\n发现 {len(flow_files)} 个工作流文件\n")
    
    all_results = []
    total_scripts = 0
    existing_scripts = 0
    importable_scripts = 0
    
    for flow_file in sorted(flow_files):
        print(f"\n{'=' * 70}")
        print(f"📋 {flow_file.name}")
        print("=" * 70)
        
        result = analyze_workflow(flow_file)
        all_results.append(result)
        
        if not result['exists']:
            print(f"  ❌ 文件读取失败: {result.get('error', '未知错误')}")
            continue
        
        if not result['valid_yaml']:
            print(f"  ❌ YAML解析失败: {result.get('yaml_error', '未知错误')}")
            continue
        
        if not result['scripts']:
            print(f"  ℹ️  未引用脚本")
            continue
        
        print(f"  引用脚本: {len(result['scripts'])} 个")
        
        for script in result['scripts']:
            total_scripts += 1
            
            if script['exists']:
                existing_scripts += 1
                exist_mark = "✅"
            else:
                exist_mark = "❌"
            
            if script['importable']:
                importable_scripts += 1
                import_mark = "✅"
            else:
                import_mark = "❌"
            
            print(f"\n    {exist_mark} {script['module']}")
            if script['file']:
                print(f"       文件: {script['file']}")
            
            if script['exists']:
                print(f"       存在: {exist_mark}")
                print(f"       可导入: {import_mark}")
            else:
                print(f"       ❌ 文件不存在")
            
            if script['error']:
                print(f"       ⚠️  {script['error']}")
    
    # 汇总
    print(f"\n{'=' * 70}")
    print("📊 测试汇总")
    print("=" * 70)
    print(f"工作流总数: {len(flow_files)}")
    print(f"脚本引用总数: {total_scripts}")
    print(f"存在脚本: {existing_scripts}/{total_scripts} ({existing_scripts/total_scripts*100:.1f}%)")
    print(f"可导入脚本: {importable_scripts}/{total_scripts} ({importable_scripts/total_scripts*100:.1f}%)")
    
    # 问题统计
    missing_scripts = []
    import_errors = []
    
    for result in all_results:
        for script in result['scripts']:
            if not script['exists']:
                missing_scripts.append((result['file'], script['module']))
            elif not script['importable']:
                import_errors.append((result['file'], script['module'], script['error']))
    
    if missing_scripts:
        print(f"\n❌ 缺失脚本 ({len(missing_scripts)}):")
        for flow, module in missing_scripts:
            print(f"  - {flow}: {module}")
    
    if import_errors:
        print(f"\n⚠️  导入错误 ({len(import_errors)}):")
        for flow, module, error in import_errors:
            print(f"  - {flow}: {module}")
            print(f"    错误: {error}")
    
    # 结论
    print(f"\n{'=' * 70}")
    if missing_scripts or import_errors:
        print("❌ 测试未通过，存在问题需要修复")
        return False
    else:
        print("✅ 所有工作流脚本检查通过")
        return True


def generate_report(all_results: List[Dict]) -> Path:
    """生成测试报告"""
    report_dir = PROJECT_ROOT / 'data' / 'test_reports'
    report_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Markdown报告
    md_path = report_dir / f'kestra_scripts_test_{timestamp}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# Kestra 工作流脚本测试报告\n\n')
        f.write(f'测试时间: {__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        # 汇总
        total_scripts = sum(len(r['scripts']) for r in all_results)
        existing = sum(1 for r in all_results for s in r['scripts'] if s['exists'])
        importable = sum(1 for r in all_results for s in r['scripts'] if s['importable'])
        
        f.write('## 汇总\n\n')
        f.write(f'- 工作流总数: {len(all_results)}\n')
        f.write(f'- 脚本引用总数: {total_scripts}\n')
        f.write(f'- 存在脚本: {existing}/{total_scripts}\n')
        f.write(f'- 可导入脚本: {importable}/{total_scripts}\n\n')
        
        # 详细结果
        f.write('## 详细结果\n\n')
        for result in all_results:
            f.write(f'### {result["file"]}\n\n')
            
            if not result['valid_yaml']:
                f.write(f'❌ YAML解析失败: {result.get("yaml_error", "")}\n\n')
                continue
            
            if not result['scripts']:
                f.write('ℹ️ 未引用脚本\n\n')
                continue
            
            for script in result['scripts']:
                status = "✅" if script['importable'] else ("❌" if not script['exists'] else "⚠️")
                f.write(f'{status} **{script["module"]}**\n')
                if script['file']:
                    f.write(f'   - 文件: `{script["file"]}`\n')
                if script['error']:
                    f.write(f'   - 错误: {script["error"]}\n')
                f.write('\n')
    
    return md_path


if __name__ == '__main__':
    success = test_all_workflows()
    sys.exit(0 if success else 1)
