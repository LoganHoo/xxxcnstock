"""
简化版最终分析 - 仅使用 Polars
"""

import sys
from pathlib import Path
from datetime import datetime
import polars as pl

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def run():
    data_dir = project_root / "data"
    
    # 读取临时数据
    temp_file = data_dir / "enhanced_full_temp.parquet"
    if not temp_file.exists():
        print("错误: 未找到数据文件")
        return
    
    print("\n" + "="*60)
    print("XCNStock 最终分析结果")
    print("="*60)
    
    df = pl.read_parquet(str(temp_file))
    print(f"\n总分析股票: {len(df)} 只")
    
    # 等级统计
    print("\n[等级分布]")
    for grade in ['S', 'A', 'B', 'C']:
        filtered_df = df.filter(pl.col("grade") == grade)
        count = len(filtered_df)
        if count > 0:
            avg_score = filtered_df.select(pl.col("enhanced_score").mean()).item()
        else:
            avg_score = 0.0
        print(f"  {grade}级: {count} 只, 平均分: {avg_score:.1f}")
    
    # 保存最终结果
    print("\n[保存结果]")
    
    # 完整数据
    df.write_parquet(str(data_dir / "enhanced_scores_full.parquet"))
    print(f"  Parquet: data/enhanced_scores_full.parquet")
    
    # 按日期快照
    today = datetime.now().strftime("%Y%m%d")
    df.write_parquet(str(data_dir / f"enhanced_scores_{today}.parquet"))
    print(f"  快照: data/enhanced_scores_{today}.parquet")
    
    # CSV导出
    export_dir = data_dir / "results"
    export_dir.mkdir(exist_ok=True)
    
    # S级
    s_df = df.filter(pl.col("grade") == "S").sort("enhanced_score", descending=True)
    s_df.write_csv(str(export_dir / f"s_grade_{today}.csv"))
    print(f"  S级CSV: results/s_grade_{today}.csv ({len(s_df)}只)")
    
    # A级
    a_df = df.filter(pl.col("grade") == "A").sort("enhanced_score", descending=True)
    a_df.write_csv(str(export_dir / f"a_grade_{today}.csv"))
    print(f"  A级CSV: results/a_grade_{today}.csv ({len(a_df)}只)")
    
    # 全部
    df.write_csv(str(export_dir / f"all_stocks_{today}.csv"))
    print(f"  全部CSV: results/all_stocks_{today}.csv")
    
    # S级 Top 20
    print("\n[S级 Top 20]")
    top_s = s_df.select([
        "code", "name", "price", "change_pct", "enhanced_score", "rsi", "momentum_10d", "reasons"
    ]).head(20)
    print(top_s)
    
    # 趋势分析
    print("\n[趋势分布]")
    trend_df = df.select([
        pl.when(pl.col("reasons").str.contains("多头排列")).then(pl.lit("多头排列"))
        .when(pl.col("reasons").str.contains("偏多趋势")).then(pl.lit("偏多趋势"))
        .when(pl.col("reasons").str.contains("震荡")).then(pl.lit("震荡"))
        .when(pl.col("reasons").str.contains("偏空")).then(pl.lit("偏空趋势"))
        .when(pl.col("reasons").str.contains("空头")).then(pl.lit("空头排列"))
        .otherwise(pl.lit("其他")).alias("trend")
    ]).group_by("trend").len().sort("len", descending=True)
    print(trend_df)
    
    print("\n" + "="*60)
    print("分析完成!")
    print("="*60)

if __name__ == "__main__":
    run()
