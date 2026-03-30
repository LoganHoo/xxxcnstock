"""
最终分析结果存储脚本
使用 Parquet + Polars + DuckDB 存储分析结果
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
from pathlib import Path
from datetime import datetime

# 安装检测
try:
    import polars as pl
    print(f"Polars {pl.__version__} 已安装")
except ImportError:
    print("正在安装 polars...")
    os.system("pip install polars -q")
    import polars as pl

try:
    import duckdb
    print(f"DuckDB {duckdb.__version__} 已安装")
except ImportError:
    print("正在安装 duckdb...")
    os.system("pip install duckdb -q")
    import duckdb

import pandas as pd
from services.data_service.storage.enhanced_storage import EnhancedStorage, get_storage


def finalize_results():
    """处理最终分析结果"""
    print("\n" + "="*60)
    print("XCNStock 增强型存储分析")
    print("="*60)
    
    data_dir = Path("data")
    
    # 读取临时结果
    temp_file = data_dir / "enhanced_full_temp.parquet"
    if not temp_file.exists():
        print("错误: 未找到临时数据文件")
        return
    
    # 使用Polars读取
    print("\n[1] 读取分析数据...")
    df = pl.read_parquet(str(temp_file))
    print(f"    总计: {len(df)} 只股票")
    
    # 初始化存储引擎
    print("\n[2] 初始化存储引擎...")
    storage = EnhancedStorage(data_dir=str(data_dir))
    
    # 保存最终结果
    print("\n[3] 保存最终结果...")
    today = datetime.now().strftime("%Y%m%d")
    
    # 保存完整结果
    final_path = "enhanced_scores_full.parquet"
    storage.save_parquet(df, final_path)
    
    # 按日期保存快照
    snapshot_path = f"enhanced_scores_{today}.parquet"
    storage.save_parquet(df, snapshot_path)
    
    # 使用DuckDB进行统计分析
    print("\n[4] DuckDB 统计分析...")
    conn = duckdb.connect(":memory:")
    
    # 注册DataFrame
    conn.register('stocks', df)
    
    # 统计各等级数量
    grade_stats = conn.execute("""
        SELECT grade, COUNT(*) as count, 
               AVG(enhanced_score) as avg_score,
               AVG(rsi) as avg_rsi,
               AVG(momentum_10d) as avg_momentum
        FROM stocks
        GROUP BY grade
        ORDER BY 
            CASE grade 
                WHEN 'S' THEN 1 
                WHEN 'A' THEN 2 
                WHEN 'B' THEN 3 
                ELSE 4 
            END
    """).pl()
    print(grade_stats)
    
    # 获取S级股票详情
    print("\n[5] S级股票 Top 30...")
    top_s = conn.execute("""
        SELECT code, name, price, change_pct, enhanced_score, rsi, momentum_10d, reasons
        FROM stocks
        WHERE grade = 'S'
        ORDER BY enhanced_score DESC
        LIMIT 30
    """).pl()
    print(top_s)
    
    # 趋势分析
    print("\n[6] 趋势分布统计...")
    trend_stats = conn.execute("""
        SELECT 
            CASE 
                WHEN reasons LIKE '%多头排列%' THEN '多头排列'
                WHEN reasons LIKE '%偏多趋势%' THEN '偏多趋势'
                WHEN reasons LIKE '%震荡%' THEN '震荡'
                WHEN reasons LIKE '%偏空趋势%' THEN '偏空趋势'
                WHEN reasons LIKE '%空头排列%' THEN '空头排列'
                ELSE '其他'
            END as trend_type,
            COUNT(*) as count,
            AVG(enhanced_score) as avg_score
        FROM stocks
        GROUP BY 1
        ORDER BY avg_score DESC
    """).pl()
    print(trend_stats)
    
    # 动量分布
    print("\n[7] 动量分布...")
    momentum_dist = conn.execute("""
        SELECT 
            CASE 
                WHEN momentum_10d > 20 THEN '强势上涨(>20%)'
                WHEN momentum_10d > 10 THEN '较强上涨(10-20%)'
                WHEN momentum_10d > 5 THEN '温和上涨(5-10%)'
                WHEN momentum_10d > 0 THEN '微涨(0-5%)'
                WHEN momentum_10d > -5 THEN '微跌(0至-5%)'
                WHEN momentum_10d > -10 THEN '温和下跌(-5至-10%)'
                ELSE '较大下跌(<-10%)'
            END as momentum_range,
            COUNT(*) as count,
            AVG(enhanced_score) as avg_score
        FROM stocks
        GROUP BY 1
        ORDER BY avg_score DESC
    """).pl()
    print(momentum_dist)
    
    # 导出CSV供查看
    print("\n[8] 导出结果...")
    export_dir = data_dir / "results"
    export_dir.mkdir(exist_ok=True)
    
    # S级导出
    s_stocks = df.filter(pl.col("grade") == "S").sort("enhanced_score", descending=True)
    s_stocks.write_csv(str(export_dir / f"s_grade_{today}.csv"))
    print(f"    S级股票已导出: results/s_grade_{today}.csv")
    
    # A级导出
    a_stocks = df.filter(pl.col("grade") == "A").sort("enhanced_score", descending=True)
    a_stocks.write_csv(str(export_dir / f"a_grade_{today}.csv"))
    print(f"    A级股票已导出: results/a_grade_{today}.csv")
    
    # 完整结果导出
    df.write_csv(str(export_dir / f"all_stocks_{today}.csv"))
    print(f"    全部结果已导出: results/all_stocks_{today}.csv")
    
    # 关闭连接
    conn.close()
    storage.close()
    
    print("\n" + "="*60)
    print("分析完成!")
    print("="*60)
    
    # 最终统计
    print(f"""
📊 最终统计报告
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 总分析股票: {len(df)} 只

⭐ S级 (强烈推荐): {len(df.filter(pl.col("grade") == "S"))} 只
📈 A级 (推荐关注): {len(df.filter(pl.col("grade") == "A"))} 只
📊 B级 (观望): {len(df.filter(pl.col("grade") == "B"))} 只
⚠️  C级 (谨慎): {len(df.filter(pl.col("grade") == "C"))} 只

💾 存储位置:
   - Parquet: data/enhanced_scores_full.parquet
   - CSV导出: data/results/
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")


if __name__ == "__main__":
    finalize_results()
