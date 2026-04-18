#!/usr/bin/env python3
"""
脚本重复功能分析器
================================================================================
分析所有脚本，识别功能重复的脚本
================================================================================
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
import re
from collections import defaultdict

project_root = Path(__file__).parent.parent
scripts_dir = project_root / "scripts"

# 定义功能类别和关键词
FUNCTION_CATEGORIES = {
    "数据采集": ["fetch", "collect", "采集", "获取", "download"],
    "数据分析": ["analyze", "analysis", "分析", "calculate", "compute", "统计"],
    "数据检查": ["check", "verify", "validate", "检查", "验证", "质检", "audit"],
    "报告生成": ["report", "send", "推送", "邮件", "生成报告"],
    "选股策略": ["pick", "select", "选股", "策略", "strategy", "factor"],
    "回测": ["backtest", "回测"],
    "监控": ["monitor", "监控", "watchdog"],
    "数据维护": ["clean", "fix", "update", "维护", "修复", "清理"],
}

def extract_description(file_path: Path) -> str:
    """提取文件描述"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read(2000)  # 读取前2000字符
            
        # 查找docstring
        docstring_match = re.search(r'"""(.*?)"""', content, re.DOTALL)
        if docstring_match:
            return docstring_match.group(1).strip()[:200]
        
        # 查找单行注释
        comment_lines = []
        for line in content.split('\n')[:10]:
            if line.strip().startswith('#'):
                comment_lines.append(line.strip('# ').strip())
        if comment_lines:
            return ' '.join(comment_lines)[:200]
            
        return ""
    except:
        return ""

def categorize_script(file_path: Path) -> list:
    """根据文件名和描述分类脚本"""
    categories = []
    name_lower = file_path.stem.lower()
    desc_lower = extract_description(file_path).lower()
    
    for category, keywords in FUNCTION_CATEGORIES.items():
        for keyword in keywords:
            if keyword in name_lower or keyword in desc_lower:
                categories.append(category)
                break
    
    return categories if categories else ["其他"]

def find_duplicates():
    """查找重复功能的脚本"""
    print("=" * 80)
    print("脚本重复功能分析")
    print("=" * 80)
    
    # 收集所有脚本
    all_scripts = []
    for py_file in scripts_dir.rglob("*.py"):
        # 排除archive目录
        if "archive" in str(py_file):
            continue
        all_scripts.append(py_file)
    
    print(f"\n总脚本数（排除archive）: {len(all_scripts)}")
    
    # 按功能分类
    category_scripts = defaultdict(list)
    for script in all_scripts:
        categories = categorize_script(script)
        for category in categories:
            category_scripts[category].append(script)
    
    # 显示分类结果
    print("\n" + "=" * 80)
    print("按功能分类")
    print("=" * 80)
    
    for category in sorted(category_scripts.keys()):
        scripts = category_scripts[category]
        print(f"\n【{category}】({len(scripts)}个)")
        for script in sorted(scripts, key=lambda x: x.name):
            desc = extract_description(script)
            desc_short = desc[:60] + "..." if len(desc) > 60 else desc
            print(f"  - {script.relative_to(project_root)}")
            if desc_short:
                print(f"    {desc_short}")
    
    # 识别潜在重复
    print("\n" + "=" * 80)
    print("潜在重复分析")
    print("=" * 80)
    
    # 按文件名相似度分组
    name_groups = defaultdict(list)
    for script in all_scripts:
        # 提取核心名称（去掉常见前缀后缀）
        name = script.stem.lower()
        name = re.sub(r'(_v\d+|_optimized|_enhanced|_fast|_complete|_backup)$', '', name)
        name = re.sub(r'^(fetch_|collect_|check_|analyze_|run_)', '', name)
        name_groups[name].append(script)
    
    duplicates_found = []
    for name, scripts in name_groups.items():
        if len(scripts) > 1:
            duplicates_found.append((name, scripts))
    
    if duplicates_found:
        print(f"\n发现 {len(duplicates_found)} 组潜在重复:")
        for name, scripts in sorted(duplicates_found, key=lambda x: -len(x[1])):
            print(f"\n  【{name}】({len(scripts)}个)")
            for script in scripts:
                print(f"    - {script.relative_to(project_root)}")
    else:
        print("\n未发现明显的文件名重复")
    
    # 按cron配置检查
    print("\n" + "=" * 80)
    print("Cron配置中的脚本")
    print("=" * 80)
    
    try:
        import yaml
        with open(project_root / "config" / "cron_tasks.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        cron_scripts = set()
        for task in config.get('tasks', []):
            script = task.get('script', '')
            if script:
                parts = script.split()
                for part in parts:
                    if part.endswith('.py'):
                        cron_scripts.add(part)
                        break
        
        print(f"\nCron配置中的脚本数: {len(cron_scripts)}")
        print("\n未在cron中使用的脚本（可能需要清理）:")
        
        unused_count = 0
        for script in all_scripts:
            rel_path = str(script.relative_to(project_root))
            if rel_path not in cron_scripts and script.name not in cron_scripts:
                # 检查是否在pipeline目录（可能是被调用的子脚本）
                if "pipeline" not in str(script):
                    print(f"  - {rel_path}")
                    unused_count += 1
        
        if unused_count == 0:
            print("  所有脚本都在cron中或被pipeline调用")
            
    except Exception as e:
        print(f"读取cron配置失败: {e}")
    
    print("\n" + "=" * 80)
    print("分析完成")
    print("=" * 80)

if __name__ == "__main__":
    find_duplicates()
