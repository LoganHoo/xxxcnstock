#!/usr/bin/env python3
"""
热门龙头股扫描 - 每日收盘后运行

修复清单:
  P0: enforce_market_closed() 市场时间守护
  P0: 结果持久化到 MySQL (hot_stocks_daily 表)
  P0: 区分板块涨跌幅 (主板10%/科创板20%/北交所30%)
  P1: 换手率 + 连板天数 + 均线多头判断
  P1: group_by 取最新日期 + 数据新鲜度检查
  P2: logging / argparse / 行业分类 / 热度权重可配置
"""
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

import polars as pl
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'workflows'))
from nextai.key_levels_client import KeyLevelsClient


def import_duckdb():
    try:
        import duckdb
        return True
    except ImportError:
        return False

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.market_guardian import enforce_market_closed
from core.logger import get_logger

logger = get_logger("hot_stocks")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
KLINE_DIR = DATA_DIR / 'kline'
STOCK_LIST = DATA_DIR / 'stock_list.parquet'

BOARD_LIMIT = {
    'main_sh': 10.0,
    'main_sz': 10.0,
    'star': 20.0,
    'gem': 20.0,
    'bse': 30.0,
}

HOT_SCORE_WEIGHTS = {
    'pct_change': 2.0,
    'pct_5d': 1.5,
    'pct_10d': 1.0,
    'pct_20d': 0.5,
    'vol_ratio': 5.0,
    'turnover_bonus': 3.0,
    'ma_bonus': 15.0,
    'limit_up_bonus': 30.0,
    'limit_up_20_bonus': 50.0,
    'key_level_bonus': 20.0,
}

DATA_FRESHNESS_DAYS = 30


def get_board_type(code: str) -> str:
    if code.startswith('688') or code.startswith('689'):
        return 'star'
    if code.startswith('30') or code.startswith('31'):
        return 'gem'
    if code.startswith('8') or code.startswith('4'):
        return 'bse'
    if code.startswith('6'):
        return 'main_sh'
    return 'main_sz'


def get_limit_pct(code: str) -> float:
    return BOARD_LIMIT.get(get_board_type(code), 10.0)


def load_stock_list() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], set]:
    name_map = {}
    industry_map = {}
    board_map = {}
    valid_codes = set()

    if not STOCK_LIST.exists():
        logger.warning("stock_list.parquet 不存在，将使用K线文件名作为代码")
        return name_map, industry_map, board_map, valid_codes

    sl = None
    try:
        import pandas as pd
        pdf = pd.read_parquet(STOCK_LIST)
        sl = pl.from_pandas(pdf)
        logger.info("Pandas读取成功")
    except Exception as e2:
        logger.error(f"Pandas读取失败: {e2}，返回空列表")
        return name_map, industry_map, board_map, valid_codes

    for row in sl.iter_rows(named=True):
        code = str(row.get('code', row.get('ts_code', ''))).strip()
        name = str(row.get('name', '')).strip()
        industry = str(row.get('industry', '')).strip()
        if code and name and code not in ('', 'None', 'nan'):
            clean_code = code.replace('.SZ', '').replace('.SH', '').replace('.BJ', '')
            if len(clean_code) == 6 and clean_code.isdigit():
                if clean_code[:2] in ('15', '51', '56', '58', '52', '53', '55'):
                    continue
                name_map[clean_code] = name
                industry_map[clean_code] = industry if industry and industry != 'None' else ''
                board_map[clean_code] = get_board_type(clean_code)
                valid_codes.add(clean_code)

    industry_file = DATA_DIR / 'fundamental' / 'industry_baostock.parquet'
    if industry_file.exists():
        try:
            import pandas as pd
            ind_df = pl.from_pandas(pd.read_parquet(industry_file))
        except Exception as e:
            logger.warning(f"读取industry文件失败: {e}，跳过")
            ind_df = None
        
        if ind_df is not None:
            filled = 0
            for row in ind_df.iter_rows(named=True):
                c = str(row.get('code', '')).strip()
                ind = str(row.get('industry', '')).strip()
                if c and ind and c in name_map and (not industry_map.get(c) or industry_map.get(c) == ''):
                    industry_map[c] = ind
                    filled += 1
            logger.info(f"从 industry_baostock.parquet 补充行业: {filled} 只")

    logger.info(f"股票列表: {len(valid_codes)} 只有效A股, 有行业: {sum(1 for v in industry_map.values() if v)}")
    return name_map, industry_map, board_map, valid_codes


def load_kline_data(valid_codes: set, target_date: Optional[str] = None) -> pl.DataFrame:
    """
    加载K线数据

    Args:
        valid_codes: 有效的股票代码集合
        target_date: 目标日期 (YYYY-MM-DD)，如果指定则加载该日期及之前的数据，否则加载最新数据
    """
    if target_date:
        logger.info(f"历史模式: 目标日期={target_date}, 加载该日期前30天数据")
    else:
        logger.info("使用Polars逐文件读取模式...")
    
    if target_date:
        from datetime import datetime
        target_dt = datetime.strptime(target_date, '%Y-%m-%d')
        start_date = (target_dt - timedelta(days=45)).strftime('%Y-%m-%d')
        logger.info(f"历史模式: 目标日期={target_date}, 读取范围: {start_date} 到 {target_date}")

    logger.warning("直接使用Polars逐文件读取模式...")

    parquet_files = sorted(KLINE_DIR.glob('*.parquet'))
    logger.info(f"发现 {len(parquet_files)} 个K线文件, valid_codes: {len(valid_codes)}")
    
    all_dfs = []
    errors = 0
    skipped = 0

    for pf in parquet_files:
        code = pf.stem
        if code not in valid_codes:
            skipped += 1
            continue
        try:
            import pandas as pd
            pdf = pd.read_parquet(pf)
            
            if pdf.empty or len(pdf) < 20:
                continue

            if 'trade_date' in pdf.columns and 'date' in pdf.columns:
                pdf['trade_date'] = pdf['trade_date'].combine_first(pdf['date'])
            elif 'date' in pdf.columns and 'trade_date' not in pdf.columns:
                pdf = pdf.rename(columns={'date': 'trade_date'})

            if 'code' in pdf.columns:
                pdf['code'] = pdf['code'].astype(str).str.replace(r'^(sh|sz|bj)\.', '', regex=True)
                pdf = pdf[pdf['code'] == code]

            if 'trade_date' not in pdf.columns:
                continue
            if 'close' not in pdf.columns or 'volume' not in pdf.columns:
                continue

            pdf = pdf[pdf['trade_date'].notna()]
            pdf['trade_date'] = pdf['trade_date'].astype(str)
            
            if target_date:
                pdf = pdf[pdf['trade_date'] <= target_date]
            
            pdf = pdf.sort_values('trade_date').tail(30)
            pdf['code'] = code

            cols_to_select = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
            if 'turnover' in pdf.columns:
                cols_to_select.append('turnover')
            if 'amount' in pdf.columns:
                cols_to_select.append('amount')
            existing = [c for c in cols_to_select if c in pdf.columns]
            pdf = pdf[existing]
            
            for col in pdf.columns:
                if col not in ['code', 'trade_date']:
                    pdf[col] = pd.to_numeric(pdf[col], errors='coerce')
            
            df = pl.from_pandas(pdf)
            all_dfs.append(df)
        except Exception as e:
            errors += 1
            logger.debug(f"读取失败 {code}: {e}")

    logger.info(f"有效: {len(all_dfs)} 只, 跳过(非A股): {skipped}, 错误: {errors}")

    if not all_dfs:
        logger.error("无有效K线数据")
        sys.exit(1)

    import pandas as pd
    all_pdfs = [df.to_pandas() for df in all_dfs]
    combined_pdf = pd.concat(all_pdfs, ignore_index=True)
    combined = pl.from_pandas(combined_pdf)
    
    return combined


def calc_technical_indicators(combined: pl.DataFrame) -> pl.DataFrame:
    logger.info("计算技术指标...")
    
    pdf = combined.to_pandas()
    pdf = pdf.sort_values(['code', 'trade_date'])
    
    pdf['prev_close'] = pdf.groupby('code')['close'].shift(1)
    pdf['pct_change'] = (pdf['close'] - pdf['prev_close']) / pdf['prev_close'] * 100
    
    pdf['close_5d_ago'] = pdf.groupby('code')['close'].shift(5)
    pdf['close_10d_ago'] = pdf.groupby('code')['close'].shift(10)
    pdf['close_20d_ago'] = pdf.groupby('code')['close'].shift(20)
    pdf['avg_vol_20d'] = pdf.groupby('code')['volume'].transform(lambda x: x.rolling(20).mean())
    pdf['ma5'] = pdf.groupby('code')['close'].transform(lambda x: x.rolling(5).mean())
    pdf['ma10'] = pdf.groupby('code')['close'].transform(lambda x: x.rolling(10).mean())
    pdf['ma20'] = pdf.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())
    
    pdf['pct_5d'] = (pdf['close'] - pdf['close_5d_ago']) / pdf['close_5d_ago'] * 100
    pdf['pct_10d'] = (pdf['close'] - pdf['close_10d_ago']) / pdf['close_10d_ago'] * 100
    pdf['pct_20d'] = (pdf['close'] - pdf['close_20d_ago']) / pdf['close_20d_ago'] * 100
    pdf['vol_ratio'] = pdf['volume'] / pdf['avg_vol_20d']
    pdf['amplitude'] = (pdf['high'] - pdf['low']) / pdf['prev_close'] * 100
    
    pdf['ma_bullish'] = (pdf['close'] > pdf['ma5']) & (pdf['ma5'] > pdf['ma10']) & (pdf['ma10'] > pdf['ma20'])
    
    if 'turnover' in pdf.columns:
        pdf['turnover_rate'] = pdf['turnover']
    elif 'amount' in pdf.columns:
        pdf['turnover_rate'] = pdf['amount'] / 1e8
    else:
        pdf['turnover_rate'] = None
    
    return pl.from_pandas(pdf)


def calc_consecutive_limit_ups(result: pl.DataFrame, board_map: Dict[str, str]) -> pl.DataFrame:
    logger.info("计算连板天数...")
    
    pdf = result.to_pandas()
    
    def get_limit_pct_for_row(code):
        return get_limit_pct(code)
    
    pdf['limit_pct'] = pdf['code'].apply(get_limit_pct_for_row)
    pdf['is_limit_up'] = pdf['pct_change'] >= pdf['limit_pct'] * 0.98
    
    pdf = pdf.sort_values(['code', 'trade_date'])
    
    pdf['consecutive_limit_days'] = 0
    prev_code = None
    counter = 0
    
    for idx in range(len(pdf)):
        code = pdf.iloc[idx]['code']
        is_limit = pdf.iloc[idx]['is_limit_up']
        
        if code != prev_code:
            counter = 0
        
        if is_limit:
            counter += 1
        else:
            counter = 0
        
        pdf.iloc[idx, pdf.columns.get_loc('consecutive_limit_days')] = counter
        prev_code = code
    
    return pl.from_pandas(pdf)


_key_levels_client = None


def get_key_levels_client() -> Optional[KeyLevelsClient]:
    global _key_levels_client
    if _key_levels_client is None:
        try:
            _key_levels_client = KeyLevelsClient()
            _key_levels_client.health_check()
            logger.info("关键价格微服务连接成功")
        except Exception as e:
            logger.warning(f"关键价格微服务连接失败: {e}")
            _key_levels_client = None
    return _key_levels_client


def enrich_key_levels_batch(pdf: pd.DataFrame, kline_data: pl.DataFrame) -> pd.DataFrame:
    """批量获取关键位评分"""
    client = get_key_levels_client()
    if client is None or pdf.empty:
        pdf['key_level_score'] = 50.0
        pdf['is_breakout'] = False
        return pdf

    kl_scores = {}
    breakout_flags = {}

    codes = pdf['code'].unique()
    for code in codes[:100]:
        try:
            stock_kline = kline_data.filter(pl.col('code') == code)
            if stock_kline.height < 20:
                kl_scores[code] = 50.0
                breakout_flags[code] = False
                continue

            result = client.calculate_from_data(
                symbol=code,
                kline_data=stock_kline.to_pandas(),
                current_price=stock_kline.tail(1)['close'][0]
            )
            kl_scores[code] = result.get('key_level_score', 50.0)
            breakout_flags[code] = result.get('is_breakout', False)
        except Exception as e:
            logger.debug(f"获取关键位失败 {code}: {e}")
            kl_scores[code] = 50.0
            breakout_flags[code] = False

    pdf['key_level_score'] = pdf['code'].map(lambda x: kl_scores.get(x, 50.0))
    pdf['is_breakout'] = pdf['code'].map(lambda x: breakout_flags.get(x, False))
    return pdf


def filter_and_get_latest(
    result: pl.DataFrame,
    name_map: Dict[str, str],
    industry_map: Dict[str, str],
    board_map: Dict[str, str],
) -> pl.DataFrame:
    logger.info("筛选有效股票...")
    
    pdf = result.to_pandas()
    pdf = pdf[
        (pdf['prev_close'] > 0) &
        (pdf['close_5d_ago'] > 0) &
        (pdf['close_10d_ago'] > 0) &
        (pdf['close_20d_ago'] > 0) &
        (pdf['avg_vol_20d'] > 0) &
        (pdf['close'] > 1.5)
    ]
    
    latest = pdf.sort_values('trade_date', ascending=False).groupby('code').first().reset_index()
    
    cutoff = (datetime.now() - timedelta(days=DATA_FRESHNESS_DAYS)).strftime('%Y-%m-%d')
    latest = latest[latest['trade_date'] >= cutoff]
    
    latest['name'] = latest['code'].map(lambda x: name_map.get(x, ''))
    latest['industry'] = latest['code'].map(lambda x: industry_map.get(x, ''))
    latest['board_type'] = latest['code'].map(lambda x: board_map.get(x, ''))
    latest['limit_pct'] = latest['code'].map(lambda x: get_limit_pct(x))
    
    latest = latest[~latest['name'].str.contains('ST|退市', na=False)]
    latest = latest[~latest['code'].str.contains('退', na=False)]
    latest = latest[latest['pct_change'].abs() <= 50]
    
    total_valid = len(latest)
    logger.info(f"有效A股(排除ST/退市/异常涨幅, 数据新鲜度{DATA_FRESHNESS_DAYS}天): {total_valid} 只")
    
    return pl.from_pandas(latest)


def classify_stocks(latest: pl.DataFrame, weights: Dict = None, kline_data: pl.DataFrame = None) -> Dict[str, pl.DataFrame]:
    if weights is None:
        weights = HOT_SCORE_WEIGHTS
    
    if kline_data is None:
        kline_data = pl.DataFrame()

    pdf = latest.to_pandas()

    pdf['pct_change'] = pd.to_numeric(pdf['pct_change'], errors='coerce')
    pdf['limit_pct'] = pd.to_numeric(pdf['limit_pct'], errors='coerce')

    pdf['is_limit_up'] = (pdf['pct_change'] >= pdf['limit_pct'] * 0.98) & (pdf['pct_change'] > 0) & pdf['pct_change'].notna()

    limit_ups = pdf[pdf['is_limit_up']].copy()
    limit_ups = limit_ups.sort_values('pct_change', ascending=False)

    hot = pdf[
        (pdf['pct_5d'] > 10) &
        (pdf['vol_ratio'] > 1.5)
    ].copy()
    hot = hot.sort_values('pct_5d', ascending=False)

    leading = pdf[pdf['pct_20d'] > 30].copy()
    leading = leading.sort_values('pct_20d', ascending=False)

    breakout = pdf[
        (pdf['vol_ratio'] > 3) &
        (pdf['pct_change'] > 5)
    ].copy()
    breakout = breakout.sort_values('vol_ratio', ascending=False)

    w = weights
    pdf['hot_score'] = (
        pdf['pct_change'].clip(lower=0) * w['pct_change'] +
        pdf['pct_5d'].clip(lower=0) * w['pct_5d'] +
        pdf['pct_10d'].clip(lower=0) * w['pct_10d'] +
        pdf['pct_20d'].clip(lower=0) * w['pct_20d'] +
        (pdf['vol_ratio'] - 1).clip(lower=0) * w['vol_ratio'] +
        pdf['ma_bullish'].astype(float).fillna(0) * w['ma_bonus'] +
        pdf['is_limit_up'].astype(float).fillna(0) * w['limit_up_bonus']
    )
    
    if 'limit_pct' in pdf.columns:
        limit_pct = pdf['limit_pct'].fillna(10.0)
        pdf['hot_score'] = pdf['hot_score'] + (pdf['pct_change'] >= limit_pct * 0.99).astype(float).fillna(0) * w.get('limit_up_20_bonus', 50.0)

    if 'turnover_rate' in pdf.columns and pdf['turnover_rate'].notna().any():
        tr = pdf['turnover_rate'].fillna(0)
        pdf['hot_score'] = pdf['hot_score'] + tr.clip(lower=0, upper=20) * w['turnover_bonus']

    if 'consecutive_limit_days' in pdf.columns:
        pdf['hot_score'] = pdf['hot_score'] + pdf['consecutive_limit_days'].fillna(0) * 10

    kl_client = get_key_levels_client()
    if kl_client is not None:
        pdf = enrich_key_levels_batch(pdf, kline_data)
        if 'key_level_score' in pdf.columns:
            w = weights
            pdf['hot_score'] = pdf['hot_score'] + (pdf['key_level_score'] - 50) * 0.3 + pdf['is_breakout'].astype(float).fillna(0) * w.get('key_level_bonus', 20)
    else:
        pdf['key_level_score'] = 50.0
        pdf['is_breakout'] = False

    top_hot = pdf.nlargest(30, 'hot_score')

    return {
        'limit_ups': limit_ups,
        'hot': hot,
        'leading': leading,
        'breakout': breakout,
        'top_hot': top_hot,
        'all_scored': pdf,
    }


def _fmt_date(val) -> str:
    try:
        s = str(val)[:10]
        if s in ('NaT', 'None', 'nan', ''):
            return '-'
        return s
    except Exception:
        return '-'


def print_results(classified: Dict[str, pl.DataFrame]):
    limit_ups = classified['limit_ups']
    hot = classified['hot']
    leading = classified['leading']
    breakout = classified['breakout']
    top_hot = classified['top_hot']

    print(f"\n{'='*100}")
    print(f"  🔥 涨停股 ({len(limit_ups)} 只)")
    print(f"{'='*100}")
    if not limit_ups.empty:
        print(f'  {"排名":<5}{"代码":<10}{"名称":<12}{"涨幅%":<9}{"板块":<7}{"连板":<6}{"量比":<7}{"5日%":<9}{"行业":<14}{"日期":<12}')
        print(f'  {"-"*90}')
        for i, (_, s) in enumerate(limit_ups.head(30).iterrows(), 1):
            name = str(s['name'])[:10]
            board = str(s.get('board_type', ''))[:5]
            consec = int(s.get('consecutive_limit_days', 0))
            industry = str(s.get('industry', ''))[:14]
            print(f'  {i:<5}{s["code"]:<10}{name:<12}{s["pct_change"]:<9.2f}{board:<7}{consec:<6}{s["vol_ratio"]:<7.1f}{s["pct_5d"]:<9.2f}{industry:<14}{_fmt_date(s["trade_date"]):<12}')
    else:
        print("  无涨停股")

    print(f"\n{'='*100}")
    print(f"  📈 热门股 - 5日涨幅>10% 且 量比>1.5 ({len(hot)} 只)")
    print(f"{'='*100}")
    if not hot.empty:
        print(f'  {"排名":<5}{"代码":<10}{"名称":<12}{"涨幅%":<9}{"5日%":<9}{"10日%":<9}{"量比":<7}{"均线多头":<8}{"行业":<14}{"日期":<12}')
        print(f'  {"-"*99}')
        for i, (_, s) in enumerate(hot.head(30).iterrows(), 1):
            name = str(s['name'])[:10]
            ma_bull = '✅' if s.get('ma_bullish', False) else '❌'
            industry = str(s.get('industry', ''))[:14]
            print(f'  {i:<5}{s["code"]:<10}{name:<12}{s["pct_change"]:<9.2f}{s["pct_5d"]:<9.2f}{s["pct_10d"]:<9.2f}{s["vol_ratio"]:<7.1f}{ma_bull:<8}{industry:<14}{_fmt_date(s["trade_date"]):<12}')
    else:
        print("  无热门股")

    print(f"\n{'='*100}")
    print(f"  👑 龙头股 - 20日涨幅>30% ({len(leading)} 只)")
    print(f"{'='*100}")
    if not leading.empty:
        print(f'  {"排名":<5}{"代码":<10}{"名称":<12}{"20日%":<9}{"10日%":<9}{"5日%":<9}{"连板":<6}{"行业":<14}{"日期":<12}')
        print(f'  {"-"*91}')
        for i, (_, s) in enumerate(leading.head(30).iterrows(), 1):
            name = str(s['name'])[:10]
            consec = int(s.get('consecutive_limit_days', 0))
            industry = str(s.get('industry', ''))[:14]
            print(f'  {i:<5}{s["code"]:<10}{name:<12}{s["pct_20d"]:<9.2f}{s["pct_10d"]:<9.2f}{s["pct_5d"]:<9.2f}{consec:<6}{industry:<14}{_fmt_date(s["trade_date"]):<12}')
    else:
        print("  无龙头股")

    print(f"\n{'='*100}")
    print(f"  💥 放量突破 - 量比>3 且 涨幅>5% ({len(breakout)} 只)")
    print(f"{'='*100}")
    if not breakout.empty:
        print(f'  {"排名":<5}{"代码":<10}{"名称":<12}{"涨幅%":<9}{"量比":<7}{"5日%":<9}{"均线多头":<8}{"行业":<14}{"日期":<12}')
        print(f'  {"-"*91}')
        for i, (_, s) in enumerate(breakout.head(20).iterrows(), 1):
            name = str(s['name'])[:10]
            ma_bull = '✅' if s.get('ma_bullish', False) else '❌'
            industry = str(s.get('industry', ''))[:14]
            print(f'  {i:<5}{s["code"]:<10}{name:<12}{s["pct_change"]:<9.2f}{s["vol_ratio"]:<7.1f}{s["pct_5d"]:<9.2f}{ma_bull:<8}{industry:<14}{_fmt_date(s["trade_date"]):<12}')
    else:
        print("  无放量突破")

    print(f"\n{'='*100}")
    print(f"  🏆 综合热门龙头 TOP 30")
    print(f"{'='*100}")
    print(f'  {"排名":<5}{"代码":<10}{"名称":<12}{"热度":<8}{"涨幅%":<9}{"5日%":<9}{"量比":<7}{"连板":<6}{"均线":<6}{"行业":<14}{"日期":<12}')
    print(f'  {"-"*106}')
    for i, (_, s) in enumerate(top_hot.iterrows(), 1):
        name = str(s['name'])[:10]
        consec = int(s.get('consecutive_limit_days', 0))
        ma_bull = '✅' if s.get('ma_bullish', False) else '❌'
        industry = str(s.get('industry', ''))[:14]
        print(f'  {i:<5}{s["code"]:<10}{name:<12}{s["hot_score"]:<8.1f}{s["pct_change"]:<9.2f}{s["pct_5d"]:<9.2f}{s["vol_ratio"]:<7.1f}{consec:<6}{ma_bull:<6}{industry:<14}{_fmt_date(s["trade_date"]):<12}')


def save_to_mysql(classified: Dict[str, pl.DataFrame], predict_date: str):
    """保存分类结果到MySQL"""
    import math
    import pymysql
    
    def _to_sql_val(val):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        if isinstance(val, (int, float)):
            return float(val)
        return val
    
    mysql_host = os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost"))
    mysql_port = int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306")))
    mysql_user = os.getenv("MYSQL_USER", os.getenv("DB_USER", "root"))
    logger.info(f"MySQL配置: host={mysql_host}, port={mysql_port}, user={mysql_user}")
    
    conn = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "")),
        database=os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "xcn_db")),
        charset="utf8mb4",
    )
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_prediction (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                predict_date DATE NOT NULL,
                code VARCHAR(10) NOT NULL,
                name VARCHAR(32) NOT NULL DEFAULT '',
                industry VARCHAR(64) NOT NULL DEFAULT '',
                market VARCHAR(8) DEFAULT NULL,
                status VARCHAR(16) DEFAULT 'active',
                board_type VARCHAR(16) NOT NULL DEFAULT '',
                category VARCHAR(32) NOT NULL,
                source VARCHAR(32) NOT NULL DEFAULT 'hot_stocks',
                grade VARCHAR(20) DEFAULT NULL,
                score DECIMAL(10,2) DEFAULT NULL,
                recommend_price DECIMAL(10,3) DEFAULT NULL,
                predict_target_price DECIMAL(10,3) DEFAULT NULL,
                predict_stop_price DECIMAL(10,3) DEFAULT NULL,
                hot_score DOUBLE NOT NULL DEFAULT 0,
                pct_change DOUBLE NOT NULL DEFAULT 0,
                pct_5d DOUBLE NOT NULL DEFAULT 0,
                pct_10d DOUBLE NOT NULL DEFAULT 0,
                pct_20d DOUBLE NOT NULL DEFAULT 0,
                vol_ratio DOUBLE NOT NULL DEFAULT 0,
                amplitude DOUBLE NOT NULL DEFAULT 0,
                turnover_rate DOUBLE DEFAULT NULL,
                consecutive_limit_days INT NOT NULL DEFAULT 0,
                ma_bullish TINYINT NOT NULL DEFAULT 0,
                close_price DOUBLE NOT NULL DEFAULT 0,
                trade_date DATE DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_date_code_cat_source (predict_date, code, category, source),
                INDEX idx_predict_date (predict_date),
                INDEX idx_code (code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        total_saved = 0
        now = datetime.now()

        category_map = {
            'limit_ups': '涨停',
            'hot': '热门',
            'leading': '龙头',
            'breakout': '放量突破',
            'top_hot': '综合TOP',
        }

        logger.info(f"准备保存到MySQL, 分类: {[(k, len(v)) for k, v in [(k, classified[k]) for k in category_map.keys()]]}")

        for key, cat_name in category_map.items():
            df = classified[key]
            if df.empty:
                logger.info(f"  {key} 为空，跳过")
                continue

            logger.info(f"  处理 {key}: {len(df)} 条记录")

            insert_sql = """
                INSERT INTO daily_prediction (
                    predict_date, code, name, industry, board_type, category,
                    source, hot_score, pct_change, pct_5d, pct_10d, pct_20d,
                    vol_ratio, amplitude, turnover_rate, consecutive_limit_days,
                    ma_bullish, close_price, trade_date, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    industry = VALUES(industry),
                    board_type = VALUES(board_type),
                    hot_score = VALUES(hot_score),
                    pct_change = VALUES(pct_change),
                    pct_5d = VALUES(pct_5d),
                    pct_10d = VALUES(pct_10d),
                    pct_20d = VALUES(pct_20d),
                    vol_ratio = VALUES(vol_ratio),
                    amplitude = VALUES(amplitude),
                    turnover_rate = VALUES(turnover_rate),
                    consecutive_limit_days = VALUES(consecutive_limit_days),
                    ma_bullish = VALUES(ma_bullish),
                    close_price = VALUES(close_price),
                    trade_date = VALUES(trade_date),
                    updated_at = CURRENT_TIMESTAMP
            """

            for _, row in df.iterrows():
                try:
                    tr = row.get('turnover_rate')
                    tr_val = _to_sql_val(tr)

                    trade_date_val = str(row.get('trade_date', ''))[:10]
                    if not trade_date_val or trade_date_val == 'NaT':
                        trade_date_val = None

                    cursor.execute(insert_sql, (
                        predict_date,
                        row['code'],
                        str(row.get('name', ''))[:32],
                        str(row.get('industry', ''))[:64],
                        str(row.get('board_type', ''))[:16],
                        cat_name,
                        'hot_stocks',
                        _to_sql_val(row.get('hot_score', 0)),
                        _to_sql_val(row.get('pct_change', 0)),
                        _to_sql_val(row.get('pct_5d', 0)),
                        _to_sql_val(row.get('pct_10d', 0)),
                        _to_sql_val(row.get('pct_20d', 0)),
                        _to_sql_val(row.get('vol_ratio', 0)),
                        _to_sql_val(row.get('amplitude', 0)),
                        tr_val,
                        int(_to_sql_val(row.get('consecutive_limit_days', 0)) or 0),
                        1 if row.get('ma_bullish', False) else 0,
                        _to_sql_val(row.get('close', 0)),
                        trade_date_val,
                        now,
                    ))
                    total_saved += 1
                except Exception as e:
                    logger.warning(f"保存失败 {row.get('code', '?')}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"已保存 {total_saved} 条记录到 daily_prediction 表 (predict_date={predict_date}, source=hot_stocks)")

    except Exception as e:
        logger.error(f"MySQL存储失败: {e}")


def save_to_csv(classified: Dict[str, pl.DataFrame], predict_date: str):
    output_dir = PROJECT_ROOT / 'data' / 'hot_stocks'
    output_dir.mkdir(parents=True, exist_ok=True)

    all_rows = []
    category_map = {
        'limit_ups': '涨停',
        'hot': '热门',
        'leading': '龙头',
        'breakout': '放量突破',
        'top_hot': '综合TOP',
    }

    for key, cat_name in category_map.items():
        df = classified[key]
        if df.empty:
            continue
        df_copy = df.copy()
        df_copy['category'] = cat_name
        df_copy['predict_date'] = predict_date
        all_rows.append(df_copy)

    if all_rows:
        import pandas as pd
        combined = pd.concat(all_rows, ignore_index=True)
        csv_path = output_dir / f'hot_stocks_{predict_date}.csv'
        combined.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"CSV已保存: {csv_path}")


def parse_args():
    parser = argparse.ArgumentParser(description='热门龙头股扫描')
    parser.add_argument('--date', type=str, default=None,
                        help='指定扫描日期 (YYYY-MM-DD)，默认使用K线最新日期')
    parser.add_argument('--top', type=int, default=30,
                        help='综合热门TOP数量 (默认30)')
    parser.add_argument('--no-db', action='store_true',
                        help='不存储到MySQL')
    parser.add_argument('--no-csv', action='store_true',
                        help='不存储到CSV')
    parser.add_argument('--force', action='store_true',
                        help='强制运行，跳过市场时间检查 (仅限历史数据回溯)')
    parser.add_argument('--weights', type=str, default=None,
                        help='热度评分权重JSON格式，如: \'{"pct_change":2.0,"pct_5d":1.5}\'')
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 100)
    print("  🔥 热门龙头股扫描 (增强版)")
    print("=" * 100)

    weights = None
    if args.weights:
        import json
        try:
            weights = json.loads(args.weights)
            logger.info(f"使用自定义权重: {weights}")
        except json.JSONDecodeError as e:
            logger.warning(f"权重JSON解析失败: {e}，使用默认权重")

    if not args.force:
        enforce_market_closed()
        logger.info("市场时间检查通过")
    else:
        logger.warning("⚠️ 强制模式：跳过市场时间检查，仅用于历史数据回溯")

    name_map, industry_map, board_map, valid_codes = load_stock_list()

    combined = load_kline_data(valid_codes, target_date=args.date)

    result = calc_technical_indicators(combined)

    result = calc_consecutive_limit_ups(result, board_map)

    latest = filter_and_get_latest(result, name_map, industry_map, board_map)

    classified = classify_stocks(latest, weights, combined)

    print_results(classified)

    predict_date = args.date or datetime.now().strftime('%Y-%m-%d')

    if not args.no_db:
        save_to_mysql(classified, predict_date)

    if not args.no_csv:
        save_to_csv(classified, predict_date)

    print(f"\n{'='*100}")
    print("  扫描完成")
    print(f"{'='*100}")


if __name__ == '__main__':
    main()
