"""
运行选股策略
"""
import sys
from pathlib import Path
import argparse
import polars as pl
import json
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import factors
from core.factor_engine import FactorEngine
from core.strategy_engine import StrategyEngine


def main():
    parser = argparse.ArgumentParser(description="运行选股策略")
    parser.add_argument(
        "--strategy", "-s",
        default="config/strategies/trend_following.yaml",
        help="策略配置文件路径"
    )
    parser.add_argument(
        "--output", "-o",
        default="reports/strategy_result.json",
        help="输出文件路径"
    )
    parser.add_argument(
        "--top-n", "-n",
        type=int,
        default=20,
        help="输出股票数量"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"策略选股系统")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    print("初始化因子引擎...")
    factor_engine = FactorEngine()
    
    print(f"加载策略: {args.strategy}")
    strategy_engine = StrategyEngine(args.strategy, factor_engine)
    
    info = strategy_engine.get_strategy_info()
    print(f"\n策略名称: {info['name']}")
    print(f"策略描述: {info['description']}")
    print(f"\n因子配置:")
    for f in info["factors"]:
        print(f"  - {f['name']}: 权重 {f['weight']:.0%}")
    
    print("\n加载股票数据...")
    kline_pattern = str(project_root / "data" / "kline" / "*.parquet")
    
    try:
        stock_data = pl.read_parquet(kline_pattern)
        print(f"加载了 {len(stock_data)} 条记录")
    except Exception as e:
        print(f"加载数据失败: {e}")
        return
    
    print("\n执行选股...")
    
    codes = stock_data.select("code").unique()["code"].to_list()
    print(f"共 {len(codes)} 只股票")
    
    results = []
    processed = 0
    
    for code in codes:
        code_data = stock_data.filter(pl.col("code") == code).sort("trade_date")
        
        if len(code_data) < 30:
            continue
        
        try:
            code_result = strategy_engine.select_stocks(code_data)
            if len(code_result) > 0:
                latest = code_result.sort("trade_date", descending=True).head(1)
                results.append(latest)
        except Exception as e:
            pass
        
        processed += 1
        if processed % 500 == 0:
            print(f"已处理 {processed}/{len(codes)} 只股票...")
    
    if results:
        result = pl.concat(results, how="diagonal").sort("strategy_score", descending=True)
    else:
        result = pl.DataFrame()
    
    print(f"\n选出 {len(result)} 只股票:")
    print("-" * 60)
    
    if len(result) > 0:
        display_cols = ["code", "close", "strategy_score"]
        available_cols = [c for c in display_cols if c in result.columns]
        
        print(result.select(available_cols))
        
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        result_json = {
            "timestamp": datetime.now().isoformat(),
            "strategy": info,
            "stocks": result.to_dicts()
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n结果已保存到: {output_path}")
    else:
        print("未选出符合条件的股票")
    
    print("\n" + "=" * 60)
    print("选股完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
