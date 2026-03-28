"""
系统状态分析脚本
分析整个系统的健康状况和配置状态
"""
import sys
from pathlib import Path
import yaml
import json
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl


def print_section(title):
    """打印章节标题"""
    print("\n" + "=" * 80)
    print(f"【{title}】")
    print("=" * 80)


def analyze_directory_structure():
    """分析目录结构"""
    print_section("目录结构分析")
    
    directories = {
        "数据目录": "data",
        "配置目录": "config",
        "脚本目录": "scripts",
        "服务目录": "services",
        "因子目录": "factors",
        "过滤器目录": "filters",
        "优化目录": "optimization",
        "形态目录": "patterns",
        "报告目录": "reports",
        "日志目录": "logs",
        "核心目录": "core",
    }
    
    for name, path in directories.items():
        p = Path(path)
        exists = "✓" if p.exists() else "✗"
        print(f"  {exists} {name}: {path}")


def analyze_data_quality():
    """分析数据质量"""
    print_section("数据质量分析")
    
    data_dir = Path("data")
    
    # K线数据
    kline_dir = data_dir / "kline"
    if kline_dir.exists():
        parquet_files = list(kline_dir.glob("*.parquet"))
        print(f"  K线数据文件: {len(parquet_files)} 个")
        
        if parquet_files:
            # 抽样检查
            sample_file = parquet_files[0]
            try:
                df = pl.read_parquet(sample_file)
                print(f"    样本文件: {sample_file.name}")
                print(f"    记录数: {len(df)}")
                print(f"    字段: {', '.join(df.columns[:5])}...")
            except Exception as e:
                print(f"    读取失败: {e}")
    
    # 指数数据
    index_file = data_dir / "index" / "000001.parquet"
    if index_file.exists():
        try:
            df = pl.read_parquet(index_file)
            print(f"  指数数据: {len(df)} 条记录")
        except Exception as e:
            print(f"  指数数据读取失败: {e}")
    else:
        print(f"  指数数据: 文件不存在")
    
    # 股票列表
    stock_list_file = data_dir / "stock_list.parquet"
    if stock_list_file.exists():
        try:
            df = pl.read_parquet(stock_list_file)
            print(f"  股票列表: {len(df)} 只股票")
        except Exception as e:
            print(f"  股票列表读取失败: {e}")
    else:
        print(f"  股票列表: 文件不存在")


def analyze_config_files():
    """分析配置文件"""
    print_section("配置文件分析")
    
    config_files = {
        "主配置": "config/xcn_comm.yaml",
        "多因子策略": "config/strategies/multi_factor.yaml",
        "趋势跟踪策略": "config/strategies/trend_following.yaml",
        "技术因子": "config/factors/technical.yaml",
        "基本面过滤器": "config/filters/fundamental_filter.yaml",
        "技术过滤器": "config/filters/technical_filter.yaml",
        "市场过滤器": "config/filters/market_filter.yaml",
        "形态配置": "config/patterns/pattern_config.yaml",
    }
    
    for name, path in config_files.items():
        p = Path(path)
        if p.exists():
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                
                # 提取关键信息
                info = []
                if isinstance(config, dict):
                    if 'factors' in config:
                        info.append(f"因子: {len(config['factors'])}")
                    if 'filters' in config:
                        info.append(f"过滤器: {len(config['filters'])}")
                    if 'recommendation' in config:
                        info.append("有推荐配置")
                
                info_str = f" ({', '.join(info)})" if info else ""
                print(f"  ✓ {name}: {path}{info_str}")
            except Exception as e:
                print(f"  ✗ {name}: {path} (解析错误: {e})")
        else:
            print(f"  ✗ {name}: {path} (不存在)")


def analyze_optimization_results():
    """分析优化结果"""
    print_section("优化结果分析")
    
    results_dir = Path("optimization/results")
    
    if not results_dir.exists():
        print("  优化结果目录不存在")
        return
    
    # 冠军策略
    champion_files = sorted(results_dir.glob("champion_strategy_*.yaml"), reverse=True)
    print(f"  冠军策略文件: {len(champion_files)} 个")
    
    if champion_files:
        latest = champion_files[0]
        print(f"    最新: {latest.name}")
        try:
            with open(latest, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if 'strategy' in config:
                print(f"    适应度: {config['strategy'].get('fitness', 'N/A')}")
            if 'factors' in config:
                print(f"    因子数: {len(config['factors'].get('selected', []))}")
            if 'filters' in config:
                print(f"    过滤器数: {len(config['filters'].get('selected', []))}")
        except Exception as e:
            print(f"    读取失败: {e}")
    
    # 优化报告
    report_files = sorted(results_dir.glob("optimization_report_*.md"), reverse=True)
    print(f"  优化报告: {len(report_files)} 个")
    
    # 部署报告
    deployment_files = sorted(results_dir.glob("production_deployment_*.txt"), reverse=True)
    print(f"  部署报告: {len(deployment_files)} 个")


def analyze_reports():
    """分析报告"""
    print_section("选股报告分析")
    
    reports_dir = Path("reports")
    
    if not reports_dir.exists():
        print("  报告目录不存在")
        return
    
    # JSON 报告
    json_files = sorted(reports_dir.glob("daily_picks_*.json"), reverse=True)
    print(f"  JSON报告: {len(json_files)} 个")
    
    if json_files:
        latest = json_files[0]
        print(f"    最新: {latest.name}")
        try:
            with open(latest, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'filters' in data:
                total_stocks = sum(len(f.get('stocks', [])) for f in data['filters'].values())
                print(f"    推荐股票总数: {total_stocks}")
                for filter_name, filter_data in data['filters'].items():
                    count = len(filter_data.get('stocks', []))
                    if count > 0:
                        print(f"      - {filter_name}: {count} 只")
        except Exception as e:
            print(f"    读取失败: {e}")
    
    # HTML 报告
    html_files = list(reports_dir.glob("daily_picks_*.html"))
    print(f"  HTML报告: {len(html_files)} 个")
    
    # 文本报告
    txt_files = list(reports_dir.glob("daily_picks_*.txt"))
    print(f"  文本报告: {len(txt_files)} 个")


def analyze_factor_system():
    """分析因子系统"""
    print_section("因子系统分析")
    
    factors_dir = Path("factors")
    
    # 技术因子
    technical_dir = factors_dir / "technical"
    if technical_dir.exists():
        py_files = list(technical_dir.glob("*.py"))
        py_files = [f for f in py_files if f.name != "__init__.py"]
        print(f"  技术因子: {len(py_files)} 个")
        for f in sorted(py_files)[:5]:
            print(f"    - {f.stem}")
        if len(py_files) > 5:
            print(f"    ... 还有 {len(py_files) - 5} 个")
    
    # 量价因子
    volume_price_dir = factors_dir / "volume_price"
    if volume_price_dir.exists():
        py_files = list(volume_price_dir.glob("*.py"))
        py_files = [f for f in py_files if f.name != "__init__.py"]
        print(f"  量价因子: {len(py_files)} 个")
        for f in sorted(py_files)[:5]:
            print(f"    - {f.stem}")
        if len(py_files) > 5:
            print(f"    ... 还有 {len(py_files) - 5} 个")


def analyze_filter_system():
    """分析过滤器系统"""
    print_section("过滤器系统分析")
    
    filters_dir = Path("filters")
    
    if filters_dir.exists():
        py_files = list(filters_dir.glob("*.py"))
        py_files = [f for f in py_files if f.name != "__init__.py"]
        print(f"  过滤器: {len(py_files)} 个")
        for f in sorted(py_files):
            print(f"    - {f.stem}")


def analyze_pattern_system():
    """分析形态系统"""
    print_section("K线形态系统分析")
    
    patterns_dir = Path("patterns")
    
    if patterns_dir.exists():
        py_files = list(patterns_dir.glob("*.py"))
        py_files = [f for f in py_files if f.name != "__init__.py"]
        print(f"  形态识别: {len(py_files)} 个模块")
        for f in sorted(py_files):
            print(f"    - {f.stem}")


def analyze_docker_setup():
    """分析 Docker 配置"""
    print_section("Docker 配置分析")
    
    docker_files = {
        "主 Dockerfile": "Dockerfile",
        "Cron Dockerfile": "Dockerfile.cron",
        "优化版 Dockerfile": "Dockerfile.cron.optimized",
        "主 Compose": "docker-compose.yml",
        "Cron Compose": "docker-compose.cron.yml",
        "优化版 Compose": "docker-compose.cron.optimized.yml",
    }
    
    for name, path in docker_files.items():
        p = Path(path)
        exists = "✓" if p.exists() else "✗"
        print(f"  {exists} {name}: {path}")
    
    # 检查镜像
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "images", "xcnstock", "--format", "{{.Repository}}:{{.Tag}} {{.Size}}"],
            capture_output=True,
            text=True
        )
        if result.stdout.strip():
            print(f"\n  Docker 镜像:")
            for line in result.stdout.strip().split('\n'):
                print(f"    - {line}")
        else:
            print(f"\n  Docker 镜像: 未找到 xcnstock 镜像")
    except Exception as e:
        print(f"\n  Docker 镜像检查失败: {e}")


def analyze_git_status():
    """分析 Git 状态"""
    print_section("Git 状态分析")
    
    import subprocess
    
    try:
        # 检查分支
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        branch = result.stdout.strip()
        print(f"  当前分支: {branch}")
        
        # 检查远程仓库
        result = subprocess.run(
            ["git", "remote", "-v"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        print(f"  远程仓库:")
        for line in result.stdout.strip().split('\n'):
            if line:
                print(f"    {line}")
        
        # 检查未提交更改
        result = subprocess.run(
            ["git", "status", "--short"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        if result.stdout.strip():
            print(f"  未提交更改:")
            for line in result.stdout.strip().split('\n')[:10]:
                print(f"    {line}")
            if len(result.stdout.strip().split('\n')) > 10:
                print(f"    ... 还有更多")
        else:
            print(f"  工作区: 干净")
        
        # 最近提交
        result = subprocess.run(
            ["git", "log", "--oneline", "-5"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        print(f"  最近提交:")
        for line in result.stdout.strip().split('\n'):
            print(f"    {line}")
    
    except Exception as e:
        print(f"  Git 检查失败: {e}")


def generate_summary():
    """生成总结"""
    print_section("系统状态总结")
    
    checks = []
    
    # 检查关键目录
    for path in ["data", "config", "scripts", "reports"]:
        checks.append((path, Path(path).exists()))
    
    # 检查关键文件
    for path in ["config/xcn_comm.yaml", "Dockerfile", "docker-compose.yml"]:
        checks.append((path, Path(path).exists()))
    
    # 统计
    passed = sum(1 for _, status in checks if status)
    total = len(checks)
    
    print(f"  检查项: {passed}/{total} 通过")
    print(f"  状态: {'✓ 良好' if passed == total else '⚠ 需关注'}")
    
    print("\n  关键检查项:")
    for name, status in checks:
        symbol = "✓" if status else "✗"
        print(f"    {symbol} {name}")


def main():
    """主函数"""
    print("\n" + "=" * 80)
    print("XCNStock 系统状态分析报告")
    print("=" * 80)
    print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    analyze_directory_structure()
    analyze_data_quality()
    analyze_config_files()
    analyze_factor_system()
    analyze_filter_system()
    analyze_pattern_system()
    analyze_optimization_results()
    analyze_reports()
    analyze_docker_setup()
    analyze_git_status()
    generate_summary()
    
    print("\n" + "=" * 80)
    print("分析完成")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
