#!/usr/bin/env python3
"""
使用Tushare获取真实基本面数据
需要设置TUSHARE_TOKEN环境变量或在.env文件中配置
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import time
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 尝试加载.env文件
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass


def get_tushare_api():
    """获取Tushare API对象"""
    try:
        import tushare as ts
    except ImportError:
        print("安装 tushare...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "tushare", "-q"])
        import tushare as ts
    
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        print("错误: 未设置TUSHARE_TOKEN环境变量")
        print("请在.env文件中添加: TUSHARE_TOKEN=your_token_here")
        print("或运行: export TUSHARE_TOKEN=your_token_here")
        return None
    
    pro = ts.pro_api(token)
    return pro


def fetch_daily_valuation(pro, trade_date: str = None):
    """获取每日估值数据"""
    if trade_date is None:
        trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    print(f"获取 {trade_date} 的估值数据...")
    
    # 获取日线行情（包含PE/PB）
    try:
        df_daily = pro.daily_basic(trade_date=trade_date, 
                                   fields='ts_code,trade_date,pe,pb,ps,total_mv,circ_mv,turnover_rate,volume_ratio')
        if df_daily is None or len(df_daily) == 0:
            print(f"警告: {trade_date} 无数据，尝试前一个交易日")
            # 尝试前5天
            for i in range(1, 6):
                prev_date = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=i)).strftime('%Y%m%d')
                df_daily = pro.daily_basic(trade_date=prev_date,
                                           fields='ts_code,trade_date,pe,pb,ps,total_mv,circ_mv,turnover_rate,volume_ratio')
                if df_daily is not None and len(df_daily) > 0:
                    print(f"使用 {prev_date} 的数据")
                    break
        
        if df_daily is not None and len(df_daily) > 0:
            # 转换代码格式
            df_daily['code'] = df_daily['ts_code'].str.split('.').str[0]
            df_daily = df_daily.rename(columns={
                'pe': 'pe_ttm',
                'pb': 'pb',
                'ps': 'ps_ttm',
                'total_mv': 'total_mv',
                'circ_mv': 'float_mv',
                'turnover_rate': 'turnover',
                'volume_ratio': 'volume_ratio'
            })
            
            # 过滤无效数据
            df_daily = df_daily[df_daily['pe_ttm'] > 0]
            df_daily = df_daily[df_daily['pe_ttm'] < 1000]  # 过滤极端值
            
            print(f"获取到 {len(df_daily)} 条有效估值数据")
            return df_daily[['code', 'pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'float_mv', 'turnover', 'volume_ratio']]
        else:
            print("未获取到估值数据")
            return None
            
    except Exception as e:
        print(f"获取估值数据失败: {e}")
        return None


def fetch_financial_indicators(pro, codes: list = None, batch_size: int = 100):
    """获取财务指标数据"""
    print("\n获取财务指标数据...")
    
    all_data = []
    
    # 获取所有股票的最新财务指标
    try:
        # 按季度获取
        year = datetime.now().year
        for quarter in [4, 3, 2, 1]:  # 优先获取最新季度
            print(f"  尝试获取 {year}年{quarter}季度数据...")
            
            try:
                df_fin = pro.fina_indicator(period=f"{year}{quarter:02d}30" if quarter != 4 else f"{year}1231",
                                            fields='ts_code,period,roe,roa,grossprofit_margin,netprofit_margin,'
                                                   'revenue_yoy,profit_yoy,debt_to_assets,current_ratio,quick_ratio,'
                                                   'eps,bps')
                
                if df_fin is not None and len(df_fin) > 0:
                    print(f"  获取到 {len(df_fin)} 条财务数据")
                    
                    # 转换代码格式
                    df_fin['code'] = df_fin['ts_code'].str.split('.').str[0]
                    
                    # 去重，保留最新数据
                    df_fin = df_fin.sort_values('period', ascending=False).drop_duplicates('code', keep='first')
                    
                    all_data.append(df_fin)
                    break  # 获取到数据就退出
                    
            except Exception as e:
                print(f"  获取{quarter}季度数据失败: {e}")
                continue
        
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            df_combined = df_combined.rename(columns={
                'roe': 'roe',
                'roa': 'roa',
                'grossprofit_margin': 'gross_margin',
                'netprofit_margin': 'net_margin',
                'revenue_yoy': 'revenue_growth',
                'profit_yoy': 'profit_growth',
                'debt_to_assets': 'debt_ratio',
                'current_ratio': 'current_ratio',
                'quick_ratio': 'quick_ratio',
                'eps': 'eps',
                'bps': 'bps'
            })
            
            print(f"共获取 {len(df_combined)} 只股票的财务数据")
            return df_combined[['code', 'roe', 'roa', 'gross_margin', 'net_margin',
                               'revenue_growth', 'profit_growth', 'debt_ratio',
                               'current_ratio', 'quick_ratio', 'eps', 'bps']]
        else:
            print("未获取到财务数据")
            return None
            
    except Exception as e:
        print(f"获取财务数据失败: {e}")
        return None


def fetch_industry_data(pro):
    """获取行业数据"""
    print("\n获取行业数据...")
    
    try:
        # 获取股票基础信息（包含行业）
        df_stock = pro.stock_basic(exchange='', list_status='L',
                                   fields='ts_code,name,industry,area')
        
        if df_stock is not None and len(df_stock) > 0:
            df_stock['code'] = df_stock['ts_code'].str.split('.').str[0]
            df_stock = df_stock.rename(columns={'industry': 'industry_sw'})
            
            print(f"获取到 {len(df_stock)} 条行业数据")
            return df_stock[['code', 'name', 'industry_sw', 'area']]
        else:
            print("未获取到行业数据")
            return None
            
    except Exception as e:
        print(f"获取行业数据失败: {e}")
        return None


def merge_all_data():
    """合并所有数据到股票列表"""
    print("\n" + "=" * 80)
    print("合并所有数据到股票列表")
    print("=" * 80)
    
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    valuation_file = PROJECT_ROOT / "data" / "fundamental" / "valuation_tushare.parquet"
    financial_file = PROJECT_ROOT / "data" / "fundamental" / "financial_tushare.parquet"
    industry_file = PROJECT_ROOT / "data" / "fundamental" / "industry_tushare.parquet"
    
    if not stock_list_file.exists():
        print("错误: 股票列表不存在")
        return None
    
    df_stocks = pl.read_parquet(stock_list_file)
    print(f"原始股票列表: {len(df_stocks)} 只")
    
    # 合并估值数据
    if valuation_file.exists():
        df_val = pl.read_parquet(valuation_file)
        df_stocks = df_stocks.join(df_val, on='code', how='left')
        pe_count = df_stocks.filter(pl.col('pe_ttm').is_not_null()).shape[0]
        print(f"合并估值数据: {pe_count}/{len(df_stocks)} 只 ({pe_count/len(df_stocks)*100:.1f}%)")
    
    # 合并财务数据
    if financial_file.exists():
        df_fin = pl.read_parquet(financial_file)
        df_stocks = df_stocks.join(df_fin, on='code', how='left')
        roe_count = df_stocks.filter(pl.col('roe').is_not_null()).shape[0]
        print(f"合并财务数据: {roe_count}/{len(df_stocks)} 只 ({roe_count/len(df_stocks)*100:.1f}%)")
    
    # 合并行业数据
    if industry_file.exists():
        df_ind = pl.read_parquet(industry_file)
        df_stocks = df_stocks.join(df_ind.select(['code', 'industry_sw']), on='code', how='left')
        # 使用申万行业替代原有行业
        df_stocks = df_stocks.with_columns([
            pl.when(pl.col('industry_sw').is_not_null())
            .then(pl.col('industry_sw'))
            .otherwise(pl.col('industry'))
            .alias('industry')
        ])
        ind_count = df_stocks.filter(pl.col('industry_sw').is_not_null()).shape[0]
        print(f"合并行业数据: {ind_count}/{len(df_stocks)} 只 ({ind_count/len(df_stocks)*100:.1f}%)")
    
    # 保存
    df_stocks.write_parquet(stock_list_file)
    print(f"\n已更新股票列表: {stock_list_file}")
    
    # 显示统计
    print("\n数据覆盖情况:")
    for col in ['pe_ttm', 'pb', 'roe', 'revenue_growth', 'industry']:
        if col in df_stocks.columns:
            null_count = df_stocks.filter(pl.col(col).is_null()).shape[0]
            coverage = (len(df_stocks) - null_count) / len(df_stocks) * 100
            print(f"  {col}: {len(df_stocks) - null_count}/{len(df_stocks)} ({coverage:.1f}%)")
    
    # 显示有完整数据的股票示例
    print("\n有完整基本面数据的股票示例:")
    sample = df_stocks.filter(
        pl.col('pe_ttm').is_not_null() & 
        pl.col('roe').is_not_null()
    ).head(10)
    if len(sample) > 0:
        print(sample.select(['code', 'name', 'industry', 'pe_ttm', 'pb', 'roe', 'revenue_growth']))
    
    return df_stocks


def main():
    """主函数"""
    print("=" * 80)
    print("使用Tushare获取真实基本面数据")
    print("=" * 80)
    
    # 获取API
    pro = get_tushare_api()
    if pro is None:
        return
    
    # 创建输出目录
    output_dir = PROJECT_ROOT / "data" / "fundamental"
    output_dir.mkdir(exist_ok=True)
    
    # 获取估值数据
    df_val = fetch_daily_valuation(pro)
    if df_val is not None:
        output_file = output_dir / "valuation_tushare.parquet"
        pl.from_pandas(df_val).write_parquet(output_file)
        print(f"估值数据已保存: {output_file}")
    
    # 获取财务数据
    df_fin = fetch_financial_indicators(pro)
    if df_fin is not None:
        output_file = output_dir / "financial_tushare.parquet"
        pl.from_pandas(df_fin).write_parquet(output_file)
        print(f"财务数据已保存: {output_file}")
    
    # 获取行业数据
    df_ind = fetch_industry_data(pro)
    if df_ind is not None:
        output_file = output_dir / "industry_tushare.parquet"
        pl.from_pandas(df_ind).write_parquet(output_file)
        print(f"行业数据已保存: {output_file}")
    
    # 合并数据
    merge_all_data()
    
    print("\n" + "=" * 80)
    print("完成!")
    print("=" * 80)


if __name__ == "__main__":
    main()
