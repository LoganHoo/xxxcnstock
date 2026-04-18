#!/usr/bin/env python3
"""
使用Baostock获取完整的基本面数据
参考文档: https://www.baostock.com/mainContent?file=stockKData.md
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def convert_code(code):
    """转换代码格式为baostock格式"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz.{code}"
    return f"sz.{code}"


def get_stock_list_from_baostock(bs):
    """
    从Baostock获取所有股票列表
    使用query_all_stock()
    如果当天没有数据，自动尝试前几个交易日
    """
    print("\n" + "=" * 80)
    print("从Baostock获取所有股票列表")
    print("=" * 80)

    # 尝试最近10个交易日
    for i in range(10):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        print(f"  尝试日期: {date}")

        rs = bs.query_all_stock(day=date)

        stock_list = []
        while rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            # row[0]=code_with_prefix (e.g., sh.000001), row[1]=code_id, row[2]=code_name
            # 真正的股票代码在 row[0] 中
            code_with_prefix = row[0]
            code = code_with_prefix.split('.')[-1] if '.' in code_with_prefix else code_with_prefix
            stock_list.append({
                'code': code,
                'name': row[2] if len(row) > 2 else '',
                'ipo_date': row[3] if len(row) > 3 else '',
            })

        if len(stock_list) > 0:
            print(f"获取到 {len(stock_list)} 只股票 (日期: {date})")
            break

    if len(stock_list) == 0:
        print("警告: 无法获取股票列表，请检查网络连接")
        return []

    # 保存到本地
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(exist_ok=True)

    df = pd.DataFrame(stock_list)
    output_file = output_dir / "stock_list.parquet"
    pl.from_pandas(df).write_parquet(output_file)
    print(f"股票列表已保存: {output_file}")

    return [s['code'] for s in stock_list]


def get_stock_list():
    """从本地获取股票列表"""
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    if not stock_list_file.exists():
        print("错误: 股票列表不存在")
        return []

    df = pl.read_parquet(stock_list_file)
    return df['code'].to_list()


def fetch_kline_data(bs, codes, days=365*3):
    """
    获取K线历史行情数据
    使用query_history_k_data_plus获取OHLCV数据

    参数:
        bs: baostock实例
        codes: 股票代码列表
        days: 获取多少天的数据，默认3年
    """
    print("\n" + "=" * 80)
    print(f"获取K线历史行情数据 (最近{days}天)")
    print("=" * 80)

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    kline_dir = PROJECT_ROOT / "data" / "kline"
    kline_dir.mkdir(exist_ok=True)

    success_count = 0
    failed_codes = []

    for i, code in enumerate(codes):
        try:
            bs_code = convert_code(code)

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"  # 前复权
            )

            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    row = rs.get_row_data()
                    data_list.append({
                        'trade_date': row[0],
                        'code': code,
                        'open': float(row[2]) if row[2] else None,
                        'high': float(row[3]) if row[3] else None,
                        'low': float(row[4]) if row[4] else None,
                        'close': float(row[5]) if row[5] else None,
                        'preclose': float(row[6]) if row[6] else None,
                        'volume': int(row[7]) if row[7] else None,
                        'amount': float(row[8]) if row[8] else None,
                        'turnover': float(row[9]) if row[9] else None,
                        'pct_chg': float(row[10]) if row[10] else None,
                    })

                if data_list:
                    # 保存到parquet文件
                    df = pd.DataFrame(data_list)
                    output_file = kline_dir / f"{code}.parquet"
                    pl.from_pandas(df).write_parquet(output_file)
                    success_count += 1
            else:
                failed_codes.append(code)

            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")

            time.sleep(0.02)  # 控制请求频率

        except Exception as e:
            failed_codes.append(code)
            continue

    print(f"\nK线数据获取完成: {success_count}/{len(codes)} 只")
    if failed_codes:
        print(f"失败: {len(failed_codes)} 只")

    return success_count


def fetch_valuation_data(bs, codes, days=365*3):
    """
    获取估值数据历史
    使用query_history_k_data_plus获取PE/PB/PS/PCF历史数据
    每只股票保存为一个parquet文件，按日期追加

    参数:
        bs: baostock实例
        codes: 股票代码列表
        days: 获取多少天的数据，默认3年
    """
    print("\n" + "=" * 80)
    print(f"获取估值数据历史 (最近{days}天)")
    print("=" * 80)

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    valuation_dir = PROJECT_ROOT / "data" / "fundamental" / "valuation_daily"
    valuation_dir.mkdir(exist_ok=True, parents=True)

    success_count = 0
    failed_codes = []

    for i, code in enumerate(codes):
        try:
            bs_code = convert_code(code)

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM,turn",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )

            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    row = rs.get_row_data()
                    pe = float(row[2]) if row[2] and row[2] != '' else None
                    pb = float(row[3]) if row[3] and row[3] != '' else None
                    ps = float(row[4]) if row[4] and row[4] != '' else None
                    pcf = float(row[5]) if row[5] and row[5] != '' else None
                    turnover = float(row[6]) if row[6] and row[6] != '' else None

                    # 过滤异常值
                    if pe and 0 < pe < 1000:
                        data_list.append({
                            'trade_date': row[0],
                            'code': code,
                            'pe_ttm': pe,
                            'pb': pb if pb and 0 < pb < 100 else None,
                            'ps_ttm': ps if ps and 0 < ps < 1000 else None,
                            'pcf': pcf if pcf and -1000 < pcf < 1000 else None,
                            'turnover': turnover,
                        })

                if data_list:
                    # 读取现有数据（如果有）
                    output_file = valuation_dir / f"{code}.parquet"
                    df_new = pd.DataFrame(data_list)

                    if output_file.exists():
                        df_existing = pl.read_parquet(output_file).to_pandas()
                        # 合并并去重
                        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                        df_combined = df_combined.drop_duplicates(subset=['trade_date'], keep='last')
                        df_combined = df_combined.sort_values('trade_date')
                        df_new = df_combined

                    # 保存到parquet文件
                    pl.from_pandas(df_new).write_parquet(output_file)
                    success_count += 1
            else:
                failed_codes.append(code)

            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")

            time.sleep(0.03)  # 控制请求频率

        except Exception as e:
            failed_codes.append(code)
            continue

    print(f"\n估值数据获取完成: {success_count}/{len(codes)} 只")
    if failed_codes:
        print(f"失败: {len(failed_codes)} 只")

    return success_count


def fetch_profit_data(bs, codes, years=3):
    """
    获取盈利能力数据历史
    使用query_profit_data获取ROE/ROA/毛利率/净利率历史季度数据
    每只股票保存为一个parquet文件，按季度追加

    参数:
        bs: baostock实例
        codes: 股票代码列表
        years: 获取多少年的历史数据，默认3年
    """
    print("\n" + "=" * 80)
    print(f"获取盈利能力数据历史 (最近{years}年)")
    print("=" * 80)

    profit_dir = PROJECT_ROOT / "data" / "fundamental" / "profit_quarterly"
    profit_dir.mkdir(exist_ok=True, parents=True)

    success_count = 0
    failed_codes = []

    # 计算需要查询的季度范围
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    for i, code in enumerate(codes):
        try:
            bs_code = convert_code(code)
            data_list = []

            # 查询最近N年的所有季度
            for year in range(current_year - years + 1, current_year + 1):
                for quarter in range(1, 5):
                    # 跳过未来的季度
                    if year == current_year and quarter > current_quarter:
                        continue

                    rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)

                    if rs.error_code == '0' and rs.next():
                        row = rs.get_row_data()

                        roe = float(row[4]) if len(row) > 4 and row[4] else None
                        roa = float(row[5]) if len(row) > 5 and row[5] else None
                        gross_margin = float(row[6]) if len(row) > 6 and row[6] else None
                        net_margin = float(row[7]) if len(row) > 7 and row[7] else None

                        # 过滤异常值
                        if roe and abs(roe) < 200:
                            data_list.append({
                                'code': code,
                                'year': year,
                                'quarter': quarter,
                                'roe': roe,
                                'roa': roa if roa and abs(roa) < 200 else None,
                                'gross_margin': gross_margin if gross_margin and abs(gross_margin) < 200 else None,
                                'net_margin': net_margin if net_margin and abs(net_margin) < 200 else None,
                            })

                    time.sleep(0.02)

            if data_list:
                # 读取现有数据（如果有）
                output_file = profit_dir / f"{code}.parquet"
                df_new = pd.DataFrame(data_list)

                if output_file.exists():
                    df_existing = pl.read_parquet(output_file).to_pandas()
                    # 合并并去重
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=['year', 'quarter'], keep='last')
                    df_combined = df_combined.sort_values(['year', 'quarter'])
                    df_new = df_combined

                # 保存到parquet文件
                pl.from_pandas(df_new).write_parquet(output_file)
                success_count += 1

            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")

            time.sleep(0.03)

        except Exception as e:
            failed_codes.append(code)
            continue

    print(f"\n盈利能力数据获取完成: {success_count}/{len(codes)} 只")
    if failed_codes:
        print(f"失败: {len(failed_codes)} 只")

    return success_count


def fetch_growth_data(bs, codes, years=3):
    """
    获取成长能力数据历史
    使用query_growth_data获取营收增长率/利润增长率历史季度数据
    每只股票保存为一个parquet文件，按季度追加
    """
    print("\n" + "=" * 80)
    print(f"获取成长能力数据历史 (最近{years}年)")
    print("=" * 80)

    growth_dir = PROJECT_ROOT / "data" / "fundamental" / "growth_quarterly"
    growth_dir.mkdir(exist_ok=True, parents=True)

    success_count = 0
    failed_codes = []

    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    for i, code in enumerate(codes):
        try:
            bs_code = convert_code(code)
            data_list = []

            for year in range(current_year - years + 1, current_year + 1):
                for quarter in range(1, 5):
                    if year == current_year and quarter > current_quarter:
                        continue

                    rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)

                    if rs.error_code == '0' and rs.next():
                        row = rs.get_row_data()
                        revenue_growth = float(row[3]) if len(row) > 3 and row[3] else None
                        profit_growth = float(row[4]) if len(row) > 4 and row[4] else None

                        if revenue_growth and abs(revenue_growth) < 1000:
                            data_list.append({
                                'code': code,
                                'year': year,
                                'quarter': quarter,
                                'revenue_growth': revenue_growth,
                                'profit_growth': profit_growth if profit_growth and abs(profit_growth) < 1000 else None,
                            })
                    time.sleep(0.02)

            if data_list:
                output_file = growth_dir / f"{code}.parquet"
                df_new = pd.DataFrame(data_list)

                if output_file.exists():
                    df_existing = pl.read_parquet(output_file).to_pandas()
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=['year', 'quarter'], keep='last')
                    df_combined = df_combined.sort_values(['year', 'quarter'])
                    df_new = df_combined

                pl.from_pandas(df_new).write_parquet(output_file)
                success_count += 1

            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")
            time.sleep(0.03)

        except Exception as e:
            failed_codes.append(code)
            continue

    print(f"\n成长能力数据获取完成: {success_count}/{len(codes)} 只")
    return success_count


def fetch_operation_data(bs, codes, years=3):
    """
    获取运营能力数据历史
    使用query_operation_data获取存货周转率/应收账款周转率历史季度数据
    """
    print("\n" + "=" * 80)
    print(f"获取运营能力数据历史 (最近{years}年)")
    print("=" * 80)

    operation_dir = PROJECT_ROOT / "data" / "fundamental" / "operation_quarterly"
    operation_dir.mkdir(exist_ok=True, parents=True)

    success_count = 0
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    for i, code in enumerate(codes):
        try:
            bs_code = convert_code(code)
            data_list = []

            for year in range(current_year - years + 1, current_year + 1):
                for quarter in range(1, 5):
                    if year == current_year and quarter > current_quarter:
                        continue

                    rs = bs.query_operation_data(code=bs_code, year=year, quarter=quarter)

                    if rs.error_code == '0' and rs.next():
                        row = rs.get_row_data()
                        inventory_turnover = float(row[3]) if len(row) > 3 and row[3] else None
                        ar_turnover = float(row[4]) if len(row) > 4 and row[4] else None

                        data_list.append({
                            'code': code,
                            'year': year,
                            'quarter': quarter,
                            'inventory_turnover': inventory_turnover,
                            'ar_turnover': ar_turnover,
                        })
                    time.sleep(0.02)

            if data_list:
                output_file = operation_dir / f"{code}.parquet"
                df_new = pd.DataFrame(data_list)

                if output_file.exists():
                    df_existing = pl.read_parquet(output_file).to_pandas()
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=['year', 'quarter'], keep='last')
                    df_combined = df_combined.sort_values(['year', 'quarter'])
                    df_new = df_combined

                pl.from_pandas(df_new).write_parquet(output_file)
                success_count += 1

            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")
            time.sleep(0.03)

        except Exception as e:
            continue

    print(f"\n运营能力数据获取完成: {success_count}/{len(codes)} 只")
    return success_count


def fetch_balance_data(bs, codes, years=3):
    """
    获取偿债能力数据历史
    使用query_balance_data获取资产负债率/流动比率/速动比率历史季度数据
    """
    print("\n" + "=" * 80)
    print(f"获取偿债能力数据历史 (最近{years}年)")
    print("=" * 80)

    balance_dir = PROJECT_ROOT / "data" / "fundamental" / "balance_quarterly"
    balance_dir.mkdir(exist_ok=True, parents=True)

    success_count = 0
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    for i, code in enumerate(codes):
        try:
            bs_code = convert_code(code)
            data_list = []

            for year in range(current_year - years + 1, current_year + 1):
                for quarter in range(1, 5):
                    if year == current_year and quarter > current_quarter:
                        continue

                    rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)

                    if rs.error_code == '0' and rs.next():
                        row = rs.get_row_data()
                        current_ratio = float(row[3]) if len(row) > 3 and row[3] else None
                        quick_ratio = float(row[4]) if len(row) > 4 and row[4] else None

                        data_list.append({
                            'code': code,
                            'year': year,
                            'quarter': quarter,
                            'current_ratio': current_ratio,
                            'quick_ratio': quick_ratio,
                        })
                    time.sleep(0.02)

            if data_list:
                output_file = balance_dir / f"{code}.parquet"
                df_new = pd.DataFrame(data_list)

                if output_file.exists():
                    df_existing = pl.read_parquet(output_file).to_pandas()
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=['year', 'quarter'], keep='last')
                    df_combined = df_combined.sort_values(['year', 'quarter'])
                    df_new = df_combined

                pl.from_pandas(df_new).write_parquet(output_file)
                success_count += 1

            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")
            time.sleep(0.03)

        except Exception as e:
            continue

    print(f"\n偿债能力数据获取完成: {success_count}/{len(codes)} 只")
    return success_count


def fetch_dupont_data(bs, codes, years=3):
    """
    获取杜邦分析数据历史
    使用query_dupont_data获取权益乘数/总资产周转率历史季度数据
    """
    print("\n" + "=" * 80)
    print(f"获取杜邦分析数据历史 (最近{years}年)")
    print("=" * 80)

    dupont_dir = PROJECT_ROOT / "data" / "fundamental" / "dupont_quarterly"
    dupont_dir.mkdir(exist_ok=True, parents=True)

    success_count = 0
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    for i, code in enumerate(codes):
        try:
            bs_code = convert_code(code)
            data_list = []

            for year in range(current_year - years + 1, current_year + 1):
                for quarter in range(1, 5):
                    if year == current_year and quarter > current_quarter:
                        continue

                    rs = bs.query_dupont_data(code=bs_code, year=year, quarter=quarter)

                    if rs.error_code == '0' and rs.next():
                        row = rs.get_row_data()
                        equity_multi = float(row[3]) if len(row) > 3 and row[3] else None
                        asset_turnover = float(row[4]) if len(row) > 4 and row[4] else None

                        data_list.append({
                            'code': code,
                            'year': year,
                            'quarter': quarter,
                            'equity_multiplier': equity_multi,
                            'asset_turnover': asset_turnover,
                        })
                    time.sleep(0.02)

            if data_list:
                output_file = dupont_dir / f"{code}.parquet"
                df_new = pd.DataFrame(data_list)

                if output_file.exists():
                    df_existing = pl.read_parquet(output_file).to_pandas()
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=['year', 'quarter'], keep='last')
                    df_combined = df_combined.sort_values(['year', 'quarter'])
                    df_new = df_combined

                pl.from_pandas(df_new).write_parquet(output_file)
                success_count += 1

            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")
            time.sleep(0.03)

        except Exception as e:
            continue

    print(f"\n杜邦分析数据获取完成: {success_count}/{len(codes)} 只")
    return success_count


def fetch_industry_data(bs):
    """
    获取行业分类数据
    使用query_stock_industry获取所有股票的行业分类
    返回字段: code, industry_code, industry, industry_classification
    """
    print("\n" + "=" * 80)
    print("获取行业分类数据")
    print("=" * 80)

    # 获取所有股票的行业分类
    rs = bs.query_stock_industry()

    industry_data = []
    while rs.error_code == '0' and rs.next():
        row = rs.get_row_data()
        # row[0]=updateDate, row[1]=code, row[2]=industry_code, row[3]=industry_name, row[4]=industry_classification
        code = row[1].split('.')[-1] if '.' in row[1] else row[1]
        industry_data.append({
            'code': code,
            'industry_code': row[2] if len(row) > 2 else '',
            'industry': row[3] if len(row) > 3 else '未知',
            'industry_classification': row[4] if len(row) > 4 else '',
            'update_date': row[0] if len(row) > 0 else '',
        })

    print(f"获取到 {len(industry_data)} 条行业数据")

    # 保存行业分类数据
    if industry_data:
        output_dir = PROJECT_ROOT / "data" / "fundamental"
        output_dir.mkdir(exist_ok=True)

        df = pd.DataFrame(industry_data)
        output_file = output_dir / "industry_data.parquet"
        pl.from_pandas(df).write_parquet(output_file)
        print(f"行业数据已保存: {output_file}")

    return industry_data


def save_data(data, filename):
    """保存数据到parquet文件"""
    if not data:
        print(f"没有数据需要保存: {filename}")
        return
    
    output_dir = PROJECT_ROOT / "data" / "fundamental"
    output_dir.mkdir(exist_ok=True)
    
    df = pd.DataFrame(data)
    output_file = output_dir / filename
    pl.from_pandas(df).write_parquet(output_file)
    print(f"数据已保存: {output_file}")
    print(f"共 {len(df)} 条记录")
    print("\n数据预览:")
    print(df.head())
    print("\n数据统计:")
    print(df.describe())


def merge_all_data():
    """合并所有数据到股票列表（从历史数据中提取最新值）"""
    print("\n" + "=" * 80)
    print("合并所有数据到股票列表")
    print("=" * 80)

    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    df_stocks = pl.read_parquet(stock_list_file)
    print(f"原始股票列表: {len(df_stocks)} 只")

    # 从日频估值数据中提取最新值
    valuation_dir = PROJECT_ROOT / "data" / "fundamental" / "valuation_daily"
    if valuation_dir.exists():
        valuation_latest = []
        for f in valuation_dir.glob("*.parquet"):
            try:
                df_val = pl.read_parquet(f)
                if len(df_val) > 0:
                    # 取最新日期
                    latest = df_val.sort('trade_date').tail(1)
                    valuation_latest.append({
                        'code': latest['code'][0],
                        'pe_ttm': latest['pe_ttm'][0] if 'pe_ttm' in latest.columns else None,
                        'pb': latest['pb'][0] if 'pb' in latest.columns else None,
                        'ps_ttm': latest['ps_ttm'][0] if 'ps_ttm' in latest.columns else None,
                    })
            except:
                continue

        if valuation_latest:
            df_val_latest = pl.DataFrame(valuation_latest)
            df_stocks = df_stocks.join(df_val_latest, on='code', how='left')
            pe_count = df_stocks.filter(pl.col('pe_ttm').is_not_null()).shape[0]
            print(f"合并估值数据: {pe_count}/{len(df_stocks)} 只 ({pe_count/len(df_stocks)*100:.1f}%)")

    # 从季度盈利数据中提取最新值
    profit_dir = PROJECT_ROOT / "data" / "fundamental" / "profit_quarterly"
    if profit_dir.exists():
        profit_latest = []
        for f in profit_dir.glob("*.parquet"):
            try:
                df_profit = pl.read_parquet(f)
                if len(df_profit) > 0:
                    # 取最新季度
                    latest = df_profit.sort(['year', 'quarter']).tail(1)
                    profit_latest.append({
                        'code': latest['code'][0],
                        'roe': latest['roe'][0] if 'roe' in latest.columns else None,
                        'roa': latest['roa'][0] if 'roa' in latest.columns else None,
                    })
            except:
                continue

        if profit_latest:
            df_profit_latest = pl.DataFrame(profit_latest)
            df_stocks = df_stocks.join(df_profit_latest, on='code', how='left')
            roe_count = df_stocks.filter(pl.col('roe').is_not_null()).shape[0]
            print(f"合并盈利数据: {roe_count}/{len(df_stocks)} 只 ({roe_count/len(df_stocks)*100:.1f}%)")

    # 从季度成长数据中提取最新值
    growth_dir = PROJECT_ROOT / "data" / "fundamental" / "growth_quarterly"
    if growth_dir.exists():
        growth_latest = []
        for f in growth_dir.glob("*.parquet"):
            try:
                df_growth = pl.read_parquet(f)
                if len(df_growth) > 0:
                    latest = df_growth.sort(['year', 'quarter']).tail(1)
                    growth_latest.append({
                        'code': latest['code'][0],
                        'revenue_growth': latest['revenue_growth'][0] if 'revenue_growth' in latest.columns else None,
                    })
            except:
                continue

        if growth_latest:
            df_growth_latest = pl.DataFrame(growth_latest)
            df_stocks = df_stocks.join(df_growth_latest, on='code', how='left')
            rev_count = df_stocks.filter(pl.col('revenue_growth').is_not_null()).shape[0]
            print(f"合并成长数据: {rev_count}/{len(df_stocks)} 只 ({rev_count/len(df_stocks)*100:.1f}%)")

    # 合并行业分类数据
    industry_file = PROJECT_ROOT / "data" / "fundamental" / "industry_data.parquet"
    if industry_file.exists():
        df_industry = pl.read_parquet(industry_file)
        df_stocks = df_stocks.join(
            df_industry.select(['code', 'industry', 'industry_classification']),
            on='code',
            how='left'
        )
        ind_count = df_stocks.filter(pl.col('industry').is_not_null()).shape[0]
        print(f"合并行业数据: {ind_count}/{len(df_stocks)} 只 ({ind_count/len(df_stocks)*100:.1f}%)")

    # 保存
    df_stocks.write_parquet(stock_list_file)
    print(f"\n已更新股票列表")

    return df_stocks


def main():
    """主函数"""
    print("=" * 80)
    print("使用Baostock获取股票数据")
    print("流程: 更新股票列表 -> 采集K线数据 -> 采集基本面数据")
    print("=" * 80)

    try:
        import baostock as bs
    except ImportError:
        print("安装 baostock...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "baostock", "-q"])
        import baostock as bs

    # 登录
    print("\n登录Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return
    print("登录成功!")

    # 第1步：获取股票列表（从Baostock获取最新列表）
    codes = get_stock_list_from_baostock(bs)
    if not codes:
        return

    print(f"\n总共 {len(codes)} 只股票需要处理")

    # 第2步：采集K线历史行情数据
    fetch_kline_data(bs, codes, days=365*3)  # 获取3年K线数据

    # 第3步：采集基本面数据（历史数据模式，每只股票独立保存）
    # 3.1 估值数据（日频历史）
    fetch_valuation_data(bs, codes, days=365*3)

    # 3.2 盈利能力数据（季度历史）
    fetch_profit_data(bs, codes, years=3)

    # 3.3 成长能力数据（季度历史）
    fetch_growth_data(bs, codes, years=3)

    # 3.4 运营能力数据（季度历史）
    fetch_operation_data(bs, codes, years=3)

    # 3.5 偿债能力数据（季度历史）
    fetch_balance_data(bs, codes, years=3)

    # 3.6 杜邦分析数据（季度历史）
    fetch_dupont_data(bs, codes, years=3)

    # 3.7 行业分类数据
    fetch_industry_data(bs)

    # 登出
    bs.logout()

    # 第4步：合并所有数据到股票列表
    merge_all_data()

    print("\n" + "=" * 80)
    print("完成!")
    print("=" * 80)


if __name__ == "__main__":
    main()
