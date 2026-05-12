#!/usr/bin/env python3
"""
实时盘面扫描 - 盘中实时监控市场机会

功能:
  - 实时行情接入 (AKShare)
  - 涨停板监控
  - 快速拉升信号
  - 量价齐升扫描
  - 突破新高检测
  - 低位反转识别

⚠️ 重要提示:
  本脚本仅作为选股参考，不做实盘买入！
  盘中运行，无需市场时间检查
"""
import sys
import os
import argparse
import asyncio
from pathlib import Path
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import polars as pl
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from dotenv import load_dotenv
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'workflows'))
sys.path.insert(0, str(PROJECT_ROOT))

from core.logger import get_logger

logger = get_logger("realtime_scanner")

from nextai.key_levels_client import KeyLevelsClient


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


@dataclass
class SignalStock:
    """信号股票"""
    code: str
    name: str
    price: float
    change_pct: float
    volume: int
    amount: float
    turnover_rate: float
    high: float
    low: float
    open: float
    pre_close: float
    signal_type: str
    signal_score: float
    reason: str
    industry: str = ''
    board_type: str = ''
    limit_time: str = ''
    consecutive_limit: int = 0
    amplitude: float = 0.0
    volume_ratio: float = 0.0


@dataclass
class ScanResult:
    """扫描结果"""
    scan_time: str
    total_stocks: int
    limit_ups: List[SignalStock] = field(default_factory=list)
    fast_rise: List[SignalStock] = field(default_factory=list)
    volume_price_up: List[SignalStock] = field(default_factory=list)
    breakout_high: List[SignalStock] = field(default_factory=list)
    reversal: List[SignalStock] = field(default_factory=list)
    strong_turnover: List[SignalStock] = field(default_factory=list)
    strong_signal: List[SignalStock] = field(default_factory=list)


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


def load_stock_list() -> Tuple[Dict[str, str], Dict[str, str], set]:
    name_map = {}
    industry_map = {}
    valid_codes = set()

    if not STOCK_LIST.exists():
        logger.warning("stock_list.parquet 不存在")
        return name_map, industry_map, valid_codes

    sl = pl.read_parquet(STOCK_LIST)
    for row in sl.iter_rows(named=True):
        code = str(row.get('code', row.get('ts_code', ''))).strip()
        name = str(row.get('name', '')).strip()
        industry = str(row.get('industry', '')).strip()
        if code and name:
            clean_code = code.replace('.SZ', '').replace('.SH', '').replace('.BJ', '')
            if len(clean_code) == 6 and clean_code.isdigit():
                name_map[clean_code] = name
                industry_map[clean_code] = industry if industry and industry != 'None' else ''
                valid_codes.add(clean_code)

    industry_file = DATA_DIR / 'fundamental' / 'industry_baostock.parquet'
    if industry_file.exists():
        ind_df = pl.read_parquet(industry_file)
        for row in ind_df.iter_rows(named=True):
            c = str(row.get('code', '')).strip()
            ind = str(row.get('industry', '')).strip()
            if c and ind and c in name_map and not industry_map.get(c):
                industry_map[c] = ind

    logger.info(f"股票列表: {len(valid_codes)} 只有效A股")
    return name_map, industry_map, valid_codes


def load_realtime_quotes() -> pd.DataFrame:
    try:
        import akshare as ak
        logger.info("正在获取实时行情...")
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            logger.error("实时行情数据为空")
            return pd.DataFrame()

        df = df.rename(columns={
            '代码': 'code',
            '名称': 'name',
            '最新价': 'price',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '量比': 'volume_ratio',
            '最高': 'high',
            '最低': 'low',
            '今开': 'open',
            '昨收': 'pre_close',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe',
            '总市值': 'total_mv',
            '流通市值': 'circ_mv',
        })

        df = df[df['code'].str.match(r'^\d{6}$')]
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['change_pct'] = pd.to_numeric(df['change_pct'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        df['pre_close'] = pd.to_numeric(df['pre_close'], errors='coerce')
        df['turnover_rate'] = pd.to_numeric(df['turnover_rate'], errors='coerce')
        df['amplitude'] = pd.to_numeric(df['amplitude'], errors='coerce')
        df['volume_ratio'] = pd.to_numeric(df['volume_ratio'], errors='coerce')

        df = df[df['price'] > 0]

        if 'name' in df.columns:
            st_mask = df['name'].str.contains('ST|退市', case=False, na=False)
            df = df[~st_mask]

        logger.info(f"实时行情: {len(df)} 只股票")
        return df

    except Exception as e:
        logger.error(f"获取实时行情失败: {e}")
        return pd.DataFrame()


def load_realtime_quotes_tencent(name_map: Dict) -> pd.DataFrame:
    """腾讯财经实时行情接口"""
    import requests
    import os
    import re
    import time

    logger.info("正在通过腾讯财经获取实时行情...")

    old_env = {k: os.environ.pop(k, None) for k in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']}

    try:
        all_data = []

        codes_list = list(name_map.keys())
        logger.info(f"共 {len(codes_list)} 只股票待查询")

        for i in range(0, len(codes_list), 100):
            batch_codes = codes_list[i:i+100]
            batch = []
            for code in batch_codes:
                prefix = 'sh' if code.startswith('6') else 'sz'
                batch.append(f"{prefix}{code}")

            symbols = ','.join(batch)
            url = f'https://qt.gtimg.cn/q={symbols}'

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            try:
                response = requests.get(url, headers=headers, timeout=30)
            except Exception as e:
                continue

            if response.status_code != 200:
                continue

            time.sleep(0.2)

            content = response.text
            for line in content.split(';'):
                if not line.strip():
                    continue

                match = re.match(r'v_(\w+)="(.+)"', line)
                if not match:
                    continue

                raw = match.group(2)
                parts = raw.split('~')

                if len(parts) < 10:
                    continue

                try:
                    code = parts[2]
                    if code.startswith('sh'):
                        code = code[2:]
                    elif code.startswith('sz'):
                        code = code[2:]
                    code = code.zfill(6)

                    name = parts[1]
                    price = float(parts[3]) if parts[3] else 0
                    pre_close = float(parts[4]) if parts[4] else 0
                    open_price = float(parts[5]) if parts[5] else 0
                    volume = int(parts[7]) if parts[7] else 0
                    amount = float(parts[8]) if parts[8] else 0
                    high = float(parts[33]) if len(parts) > 33 and parts[33] else 0
                    low = float(parts[34]) if len(parts) > 34 and parts[34] else 0

                    change_pct = 0
                    if pre_close > 0:
                        change_pct = (price - pre_close) / pre_close * 100

                    if price > 0:
                        all_data.append({
                            'code': code,
                            'name': name,
                            'price': price,
                            'change_pct': change_pct,
                            'volume': volume,
                            'amount': amount,
                            'high': high,
                            'low': low,
                            'open': open_price,
                            'pre_close': pre_close,
                            'turnover_rate': 0,
                            'volume_ratio': 0,
                            'amplitude': 0,
                        })
                except (ValueError, IndexError):
                    continue

        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"腾讯财经实时行情: {len(df)} 只股票")
            return df
        else:
            logger.warning("腾讯财经未获取到数据")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"腾讯财经获取实时行情失败: {e}")
        return pd.DataFrame()
    finally:
        for k, v in old_env.items():
            if v is not None:
                os.environ[k] = v


def load_limit_pool() -> pd.DataFrame:
    try:
        import akshare as ak
        today = datetime.now().strftime("%Y%m%d")
        logger.info(f"获取涨停池: {today}")
        df = ak.stock_zt_pool_em(date=today)
        if df is None or df.empty:
            logger.warning("涨停池为空")
            return pd.DataFrame()

        df = df.rename(columns={
            '代码': 'code',
            '名称': 'name',
            '涨跌幅': 'change_pct',
            '最新价': 'price',
            '首次封板时间': 'limit_time',
            '最后封板时间': 'last_limit_time',
            '封板资金': 'seal_amount',
            '炸板次数': 'open_count',
            '连板数': 'consecutive_limit',
            '所属行业': 'industry',
            '成交额': 'amount',
            '换手率': 'turnover_rate',
        })

        df['code'] = df['code'].astype(str).str.zfill(6)
        df['change_pct'] = pd.to_numeric(df['change_pct'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['turnover_rate'] = pd.to_numeric(df['turnover_rate'], errors='coerce')

        def parse_consecutive(x):
            try:
                s = str(x)
                if '/' in s:
                    return int(s.split('/')[0])
                return int(float(x))
            except:
                return 1

        df['consecutive_limit'] = df['consecutive_limit'].apply(parse_consecutive)
        df['limit_time'] = df['limit_time'].astype(str)
        df['industry'] = df['industry'].astype(str)
        logger.info(f"涨停池: {len(df)} 只")
        return df

    except Exception as e:
        logger.error(f"获取涨停池失败: {e}")
        return pd.DataFrame()


def load_hist_kline(code: str, days: int = 60) -> Optional[pl.DataFrame]:
    kline_file = KLINE_DIR / f"{code}.parquet"
    if not kline_file.exists():
        return None

    try:
        df = pl.read_parquet(kline_file)
        if 'trade_date' in df.columns:
            df = df.sort('trade_date').tail(days)
        elif 'date' in df.columns:
            df = df.rename({'date': 'trade_date'}).sort('trade_date').tail(days)
        return df
    except Exception as e:
        logger.debug(f"读取K线失败 {code}: {e}")
        return None


def scan_limit_ups(quotes: pd.DataFrame, limit_pool: pd.DataFrame) -> List[SignalStock]:
    results = []
    if limit_pool.empty:
        return results

    limit_pct_map = {row['code']: get_limit_pct(row['code']) for _, row in limit_pool.iterrows()}

    for _, row in limit_pool.iterrows():
        code = str(row['code'])
        limit_pct = limit_pct_map.get(code, 10.0)
        is_limit = row['change_pct'] >= limit_pct * 0.98
        consec = int(row.get('consecutive_limit', 1)) if pd.notna(row.get('consecutive_limit')) else 1

        if is_limit:
            signal = SignalStock(
                code=code,
                name=str(row.get('name', '')),
                price=float(row.get('price', 0)),
                change_pct=float(row['change_pct']),
                volume=0,
                amount=float(row.get('amount', 0) or 0),
                turnover_rate=float(row.get('turnover_rate', 0) or 0),
                high=0,
                low=0,
                open=0,
                pre_close=0,
                signal_type='涨停',
                signal_score=90 + consec * 5,
                reason='首板' if consec == 1 else f'{consec}连板',
                industry=str(row.get('industry', '')),
                board_type=get_board_type(code),
                limit_time=str(row.get('limit_time', '')),
                consecutive_limit=consec,
            )
            results.append(signal)

    return results


def scan_fast_rise(quotes: pd.DataFrame, threshold: float = 5.0) -> List[SignalStock]:
    results = []
    df = quotes.copy()
    df['_limit_pct'] = df['code'].apply(get_limit_pct)
    df = df[df['change_pct'] >= threshold]
    df = df[df['change_pct'] < df['_limit_pct'] * 0.98]
    df = df.drop(columns=['_limit_pct'])
    df = df.sort_values('change_pct', ascending=False)

    for _, row in df.head(30).iterrows():
        code = str(row['code'])
        signal = SignalStock(
            code=code,
            name=str(row['name']),
            price=float(row['price']),
            change_pct=float(row['change_pct']),
            volume=int(row['volume']),
            amount=float(row['amount']),
            turnover_rate=float(row.get('turnover_rate', 0) or 0),
            high=float(row['high']),
            low=float(row['low']),
            open=float(row['open']),
            pre_close=float(row['pre_close']),
            signal_type='快速拉升',
            signal_score=float(row['change_pct']) * 10,
            reason=f"涨幅{row['change_pct']:.1f}%，有望冲击涨停",
        )
        results.append(signal)

    return results


def scan_volume_price_up(quotes: pd.DataFrame, vol_ratio_threshold: float = 2.0, min_turnover: float = 5.0, min_change: float = 3.0) -> List[SignalStock]:
    results = []
    df = quotes.copy()
    df = df[df['change_pct'] >= min_change]
    df = df[df['turnover_rate'] >= min_turnover]
    if 'volume_ratio' in df.columns:
        df = df[df['volume_ratio'] >= vol_ratio_threshold]
    df = df.sort_values('change_pct', ascending=False)

    for _, row in df.head(20).iterrows():
        code = str(row['code'])
        signal = SignalStock(
            code=code,
            name=str(row['name']),
            price=float(row['price']),
            change_pct=float(row['change_pct']),
            volume=int(row['volume']),
            amount=float(row['amount']),
            turnover_rate=float(row.get('turnover_rate', 0) or 0),
            high=float(row['high']),
            low=float(row['low']),
            open=float(row['open']),
            pre_close=float(row['pre_close']),
            signal_type='量价齐升',
            signal_score=float(row.get('turnover_rate', 0) or 0) * 20,
            reason=f"换手率{row.get('turnover_rate', 0):.1f}%，量价配合",
        )
        results.append(signal)

    return results


def scan_breakout_high(quotes: pd.DataFrame, name_map: Dict) -> List[SignalStock]:
    results = []
    df = quotes.copy()
    df = df[df['price'] > 0]
    df = df[df['change_pct'] > 2]
    df = df.sort_values('change_pct', ascending=False)

    for _, row in df.head(1000).iterrows():
        code = str(row['code'])
        if code not in name_map:
            continue

        kline = load_hist_kline(code, 60)
        if kline is None or len(kline) < 30:
            continue

        high_60d = kline['high'].max()
        current_price = row['price']

        if current_price >= high_60d * 0.98 and row['change_pct'] > 3:
            signal = SignalStock(
                code=code,
                name=str(row['name']),
                price=float(row['price']),
                change_pct=float(row['change_pct']),
                volume=int(row['volume']),
                amount=float(row['amount']),
                turnover_rate=float(row.get('turnover_rate', 0) or 0),
                high=float(row['high']),
                low=float(row['low']),
                open=float(row['open']),
                pre_close=float(row['pre_close']),
                signal_type='突破新高',
                signal_score=80 + row['change_pct'] * 5,
                reason=f"60日新高 {high_60d:.2f}",
            )
            results.append(signal)

    results.sort(key=lambda x: x.signal_score, reverse=True)
    return results[:20]


def scan_reversal(quotes: pd.DataFrame, name_map: Dict) -> List[SignalStock]:
    results = []
    df = quotes.copy()
    df = df[df['change_pct'] >= -3]
    df = df[df['change_pct'] <= 2]
    df = df[df['turnover_rate'] >= 5]
    df = df.sort_values('change_pct', ascending=False)

    for _, row in df.head(30).iterrows():
        code = str(row['code'])
        if code not in name_map:
            continue

        kline = load_hist_kline(code, 60)
        if kline is None or len(kline) < 30:
            continue

        closes = kline['close'].to_list()
        if len(closes) < 20:
            continue

        ma5 = sum(closes[-5:]) / 5
        ma10 = sum(closes[-10:]) / 10
        ma20 = sum(closes[-20:]) / 20

        current_price = row['price']
        if current_price < ma20 * 1.1 and current_price > ma5:
            signal = SignalStock(
                code=code,
                name=str(row['name']),
                price=float(row['price']),
                change_pct=float(row['change_pct']),
                volume=int(row['volume']),
                amount=float(row['amount']),
                turnover_rate=float(row.get('turnover_rate', 0) or 0),
                high=float(row['high']),
                low=float(row['low']),
                open=float(row['open']),
                pre_close=float(row['pre_close']),
                signal_type='低位反转',
                signal_score=70 + row['change_pct'] * 5,
                reason=f"站上均线，回调到位",
            )
            results.append(signal)

    return results


def scan_strong_turnover(quotes: pd.DataFrame) -> List[SignalStock]:
    results = []
    df = quotes.copy()
    df = df[df['turnover_rate'] >= 10]
    df = df[df['change_pct'] > 0]
    df = df.sort_values('turnover_rate', ascending=False)

    for _, row in df.head(30).iterrows():
        code = str(row['code'])
        signal = SignalStock(
            code=code,
            name=str(row['name']),
            price=float(row['price']),
            change_pct=float(row['change_pct']),
            volume=int(row['volume']),
            amount=float(row['amount']),
            turnover_rate=float(row.get('turnover_rate', 0) or 0),
            high=float(row['high']),
            low=float(row['low']),
            open=float(row['open']),
            pre_close=float(row['pre_close']),
            signal_type='高换手',
            signal_score=float(row.get('turnover_rate', 0) or 0) * 5,
            reason=f"换手率{row.get('turnover_rate', 0):.1f}%，资金活跃",
        )
        results.append(signal)

    return results


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


def enrich_with_key_levels(code: str, current_price: float) -> dict:
    """获取股票的关键价格信息"""
    client = get_key_levels_client()
    if client is None:
        return {
            'high_20d': None,
            'low_20d': None,
            'is_breakout': False,
            'key_level_score': 50.0,
        }

    try:
        kline = load_hist_kline(code, 60)
        if kline is None or len(kline) < 20:
            return {
                'high_20d': None,
                'low_20d': None,
                'is_breakout': False,
                'key_level_score': 50.0,
            }

        result = client.calculate_from_data(
            symbol=code,
            kline_data=kline.to_pandas(),
            current_price=current_price
        )
        return result
    except Exception as e:
        logger.debug(f"获取关键位失败 {code}: {e}")
        return {
            'high_20d': None,
            'low_20d': None,
            'is_breakout': False,
            'key_level_score': 50.0,
        }


def scan_strong_signal(quotes: pd.DataFrame, name_map: Dict) -> List[SignalStock]:
    results = []
    df = quotes.copy()

    if 'volume_ratio' not in df.columns:
        df['volume_ratio'] = df.get('volume_ratio', 0)
    if 'amplitude' not in df.columns:
        df['amplitude'] = df.get('amplitude', 0)

    df = df[df['volume_ratio'] > 2.5]
    df = df[df['amplitude'] > 6.5]
    df = df[df['change_pct'] > 2.44]
    df = df.sort_values('volume_ratio', ascending=False)

    for _, row in df.head(500).iterrows():
        code = str(row['code'])
        if code not in name_map:
            continue

        kline = load_hist_kline(code, 60)
        if kline is None or len(kline) < 30:
            continue

        closes = kline['close'].to_list()
        if len(closes) < 20:
            continue

        ma5 = sum(closes[-5:]) / 5
        ma10 = sum(closes[-10:]) / 10
        ma20 = sum(closes[-20:]) / 20

        if ma5 > ma10 > ma20:
            kl = enrich_with_key_levels(code, float(row['price']))
            kl_score = kl.get('key_level_score', 50.0)
            breakout_bonus = 15 if kl.get('is_breakout', False) else 0

            signal = SignalStock(
                code=code,
                name=str(row['name']),
                price=float(row['price']),
                change_pct=float(row['change_pct']),
                volume=int(row['volume']),
                amount=float(row['amount']),
                turnover_rate=float(row.get('turnover_rate', 0) or 0),
                high=float(row['high']),
                low=float(row['low']),
                open=float(row['open']),
                pre_close=float(row['pre_close']),
                signal_type='强势信号',
                signal_score=85 + float(row.get('volume_ratio', 0) or 0) * 3 + (kl_score - 50) * 0.5 + breakout_bonus,
                reason=f"量比{row.get('volume_ratio', 0):.1f} 振幅{row.get('amplitude', 0):.1f}% 多头 关键位{int(kl_score)}",
                amplitude=float(row.get('amplitude', 0) or 0),
                volume_ratio=float(row.get('volume_ratio', 0) or 0),
            )
            results.append(signal)

    results.sort(key=lambda x: x.signal_score, reverse=True)
    return results[:30]


def print_results(result: ScanResult):
    now = datetime.now()
    is_trading = time(9, 30) <= now.time() <= time(15, 0)
    market_status = "🟢 盘中" if is_trading else "🔴 盘后"

    print(f"\n{'='*100}")
    print(f"  📊 实时盘面扫描 {market_status} - {result.scan_time}")
    print(f"  总股票数: {result.total_stocks}")
    print(f"{'='*100}")

    if result.limit_ups:
        print(f"\n  🔥 涨停板 ({len(result.limit_ups)} 只)")
        print(f"  {'代码':<8} {'名称':<10} {'涨幅':<8} {'连板':<6} {'首次封板':<10} {'行业':<16}")
        print(f"  {'-'*75}")
        for s in result.limit_ups[:15]:
            industry_str = s.industry[:16] if s.industry else ''
            print(f"  {s.code:<8} {s.name:<10} {s.change_pct:>6.2f}% {s.consecutive_limit:>6} {s.limit_time:<10} {industry_str:<16}")
    else:
        print(f"\n  🔥 涨停板: 暂无")

    if result.fast_rise:
        print(f"\n  ⬆️ 快速拉升 ({len(result.fast_rise)} 只)")
        print(f"  {'代码':<8}{'名称':<10}{'涨幅':<8}{'价格':<10}{'原因':<30}")
        print(f"  {'-'*80}")
        for s in result.fast_rise[:10]:
            print(f"  {s.code:<8}{s.name:<10}{s.change_pct:>6.2f}%{s.price:>10.2f}{s.reason[:30]:<30}")

    if result.volume_price_up:
        print(f"\n  📈 量价齐升 ({len(result.volume_price_up)} 只)")
        print(f"  {'代码':<8}{'名称':<10}{'涨幅':<8}{'换手率':<10}{'原因':<30}")
        print(f"  {'-'*80}")
        for s in result.volume_price_up[:10]:
            print(f"  {s.code:<8}{s.name:<10}{s.change_pct:>6.2f}%{s.turnover_rate:>9.1f}%{s.reason[:30]:<30}")

    if result.breakout_high:
        print(f"\n  🎯 突破新高 ({len(result.breakout_high)} 只)")
        print(f"  {'代码':<8}{'名称':<10}{'涨幅':<8}{'价格':<10}{'原因':<20}")
        print(f"  {'-'*70}")
        for s in result.breakout_high[:10]:
            print(f"  {s.code:<8}{s.name:<10}{s.change_pct:>6.2f}%{s.price:>10.2f}{s.reason[:20]:<20}")

    if result.reversal:
        print(f"\n  🔄 低位反转 ({len(result.reversal)} 只)")
        print(f"  {'代码':<8}{'名称':<10}{'涨幅':<8}{'换手率':<10}{'原因':<30}")
        print(f"  {'-'*70}")
        for s in result.reversal[:10]:
            print(f"  {s.code:<8}{s.name:<10}{s.change_pct:>6.2f}%{s.turnover_rate:>9.1f}%{s.reason[:30]:<30}")

    if result.strong_turnover:
        print(f"\n  💪 高换手活跃 ({len(result.strong_turnover)} 只)")
        print(f"  {'代码':<8}{'名称':<10}{'涨幅':<8}{'换手率':<10}{'成交额(亿)':<12}")
        print(f"  {'-'*60}")
        for s in result.strong_turnover[:10]:
            amount_yi = s.amount / 1e8 if s.amount else 0
            print(f"  {s.code:<8}{s.name:<10}{s.change_pct:>6.2f}%{s.turnover_rate:>9.1f}%{amount_yi:>11.1f}")

    if result.strong_signal:
        print(f"\n  ⭐ 强势信号 ({len(result.strong_signal)} 只)")
        print(f"  {'代码':<8}{'名称':<10}{'涨幅':<8}{'量比':<8}{'振幅':<10}{'原因':<20}")
        print(f"  {'-'*70}")
        for s in result.strong_signal[:10]:
            print(f"  {s.code:<8}{s.name:<10}{s.change_pct:>6.2f}%{s.volume_ratio:>7.1f}{s.amplitude:>9.1f}%{s.reason[:20]:<20}")

    print(f"\n{'='*100}")
    print(f"  ⚠️ 本扫描仅作为选股参考，不构成实盘买入建议！")

    if result.strong_signal:
        limit_up_codes = {s.code for s in result.limit_ups}
        non_limit_signals = [s for s in result.strong_signal if s.code not in limit_up_codes]

        print(f"\n{'='*100}")
        print(f"  🎯 推荐交易 (强势信号 - 量比>2.5 振幅>6.5% 涨幅>2.44% 均线多头)")
        print(f"  {'代码':<8} {'名称':<10} {'涨幅':<8} {'量比':<8} {'换手率':<10} {'综合评分':<10}")
        print(f"  {'-'*70}")
        for s in non_limit_signals[:5]:
            print(f"  {s.code:<8} {s.name:<10} {s.change_pct:>6.2f}% {s.volume_ratio:>7.1f} {s.turnover_rate:>9.1f}% {s.signal_score:>9.1f}")
        print(f"{'='*100}")

    print(f"{'='*100}")


def save_scan_result(result: ScanResult, args) -> Optional[str]:
    try:
        import os
        scan_dir = DATA_DIR / 'realtime_scan'
        scan_dir.mkdir(parents=True, exist_ok=True)

        today = datetime.now().strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"scan_{today}_{timestamp}.parquet"
        filepath = scan_dir / filename

        all_signals = []
        signal_groups = [
            (result.limit_ups, '涨停'),
            (result.fast_rise, '快速拉升'),
            (result.volume_price_up, '量价齐升'),
            (result.breakout_high, '突破新高'),
            (result.reversal, '低位反转'),
            (result.strong_turnover, '高换手'),
            (result.strong_signal, '强势信号'),
        ]

        for stocks, signal_type in signal_groups:
            for stock in stocks:
                all_signals.append({
                    'scan_time': f"{today} {result.scan_time}",
                    'signal_type': signal_type,
                    'code': stock.code,
                    'name': stock.name,
                    'price': stock.price,
                    'change_pct': stock.change_pct,
                    'volume': stock.volume,
                    'amount': stock.amount,
                    'turnover_rate': stock.turnover_rate,
                    'industry': stock.industry,
                    'consecutive_limit': stock.consecutive_limit,
                    'limit_time': stock.limit_time,
                })

        if all_signals:
            df = pd.DataFrame(all_signals)
            df.to_parquet(filepath, index=False)
            logger.info(f"扫描结果已保存: {filepath}")
            return str(filepath)
        else:
            logger.info("无信号，跳过保存")
            return None

    except Exception as e:
        logger.error(f"保存扫描结果失败: {e}")
        return None


def save_to_mysql(result: ScanResult):
    """将扫描结果保存到 MySQL daily_prediction 表"""
    import math

    def _to_sql_val(val):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        if isinstance(val, (int, float)):
            return float(val)
        return val

    try:
        import pymysql

        mysql_host = os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost"))
        mysql_port = int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306")))
        mysql_user = os.getenv("MYSQL_USER", os.getenv("DB_USER", "root"))

        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "")),
            database=os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "xcn_db")),
            charset="utf8mb4",
        )
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_prediction (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                scan_date DATE NOT NULL,
                code VARCHAR(10) NOT NULL,
                name VARCHAR(32) NOT NULL DEFAULT '',
                industry VARCHAR(64) NOT NULL DEFAULT '',
                board_type VARCHAR(16) NOT NULL DEFAULT '',
                category VARCHAR(32) NOT NULL,
                source VARCHAR(32) NOT NULL DEFAULT 'realtime_scan',
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

                -- 综合评分扩展字段 (source='comprehensive' 时使用)
                financial_score DOUBLE DEFAULT NULL,
                market_score DOUBLE DEFAULT NULL,
                announcement_score DOUBLE DEFAULT NULL,
                technical_score DOUBLE DEFAULT NULL,
                total_score DOUBLE DEFAULT NULL,

                -- 财务因子
                roe DOUBLE DEFAULT NULL,
                gross_margin DOUBLE DEFAULT NULL,
                revenue_growth DOUBLE DEFAULT NULL,
                debt_ratio DOUBLE DEFAULT NULL,

                -- 市场因子
                main_force_flow DOUBLE DEFAULT NULL,
                dragon_tiger_count INT DEFAULT NULL,
                northbound_holding DOUBLE DEFAULT NULL,

                -- 技术因子
                price_change_5d DOUBLE DEFAULT NULL,
                volume_ratio DOUBLE DEFAULT NULL,
                amplitude_ex DOUBLE DEFAULT NULL,
                price_change DOUBLE DEFAULT NULL,
                ma_bullish_strength DOUBLE DEFAULT NULL,

                -- 关键位
                is_breakout TINYINT DEFAULT 0,
                is_near_support TINYINT DEFAULT 0,
                high_20d DOUBLE DEFAULT NULL,
                low_20d DOUBLE DEFAULT NULL,

                -- 主力共振
                main_force_grade VARCHAR(8) DEFAULT NULL,
                main_force_signal_count INT DEFAULT 0,
                mf_s1_detected TINYINT DEFAULT 0,
                mf_s2_detected TINYINT DEFAULT 0,
                mf_s3_detected TINYINT DEFAULT 0,
                mf_s4_detected TINYINT DEFAULT 0,

                -- 元数据
                strategy_type VARCHAR(32) DEFAULT NULL,
                filter_reason VARCHAR(256) DEFAULT NULL,

                UNIQUE KEY uk_date_code_cat_source (scan_date, code, category, source),
                INDEX idx_scan_date (scan_date),
                INDEX idx_source (source),
                INDEX idx_category (category),
                INDEX idx_hot_score (hot_score),
                INDEX idx_total_score (total_score)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        insert_sql = """
            INSERT INTO daily_prediction (
                scan_date, code, name, industry, board_type, category,
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
                vol_ratio = VALUES(vol_ratio),
                amplitude = VALUES(amplitude),
                turnover_rate = VALUES(turnover_rate),
                consecutive_limit_days = VALUES(consecutive_limit_days),
                close_price = VALUES(close_price),
                updated_at = CURRENT_TIMESTAMP
        """

        scan_date = datetime.now().strftime('%Y-%m-%d')
        now = datetime.now()
        total_saved = 0

        signal_groups = [
            (result.limit_ups, '涨停'),
            (result.fast_rise, '快速拉升'),
            (result.volume_price_up, '量价齐升'),
            (result.breakout_high, '突破新高'),
            (result.reversal, '低位反转'),
            (result.strong_turnover, '高换手'),
            (result.strong_signal, '强势信号'),
        ]

        for stocks, cat_name in signal_groups:
            for stock in stocks:
                try:
                    cursor.execute(insert_sql, (
                        scan_date,
                        stock.code,
                        str(stock.name)[:32],
                        str(stock.industry)[:64],
                        str(stock.board_type)[:16],
                        cat_name,
                        'realtime_scan',
                        _to_sql_val(stock.signal_score),
                        _to_sql_val(stock.change_pct),
                        None,
                        None,
                        None,
                        _to_sql_val(stock.volume_ratio),
                        _to_sql_val(stock.amplitude),
                        _to_sql_val(stock.turnover_rate),
                        stock.consecutive_limit,
                        0,
                        _to_sql_val(stock.price),
                        scan_date,
                        now,
                    ))
                    total_saved += 1
                except Exception as e:
                    logger.warning(f"MySQL保存失败 {stock.code}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"已保存 {total_saved} 条记录到 daily_prediction 表 (scan_date={scan_date}, source=realtime_scan)")

    except Exception as e:
        logger.error(f"MySQL存储失败: {e}")


def parse_args():
    parser = argparse.ArgumentParser(description='实时盘面扫描 - 盘中监控市场机会')
    parser.add_argument('--limit', action='store_true', help='仅扫描涨停板')
    parser.add_argument('--fast', action='store_true', help='仅扫描快速拉升')
    parser.add_argument('--volume', action='store_true', help='仅扫描量价齐升')
    parser.add_argument('--breakout', action='store_true', help='仅扫描突破新高')
    parser.add_argument('--reversal', action='store_true', help='仅扫描低位反转')
    parser.add_argument('--turnover', action='store_true', help='仅扫描高换手')
    parser.add_argument('--strong', action='store_true', help='仅扫描强势信号(量比>2.5 振幅>6.5%% 涨幅>2.44%% 均线多头)')
    parser.add_argument('--top', type=int, default=30, help='每类显示数量')
    parser.add_argument('--no-hist', action='store_true', help='跳过历史K线检查(加速)')
    parser.add_argument('--source', type=str, default='akshare', choices=['akshare', 'tencent'], help='数据源: akshare(默认) 或 tencent(腾讯财经)')
    parser.add_argument('--interval', type=int, default=3, help='扫描间隔(分钟)，默认3分钟')
    parser.add_argument('--single', action='store_true', help='仅运行一次，不持续监控')
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 100)
    print("  📊 实时盘面扫描 - 盘中市场机会监控")
    print("  ⚠️ 仅作为选股参考，不做实盘买入")
    print("=" * 100)

    if args.single:
        run_single_scan(args)
    else:
        run_continuous_scan(args)


def run_single_scan(args):
    name_map, industry_map, valid_codes = load_stock_list()

    if args.source == 'tencent':
        quotes = load_realtime_quotes_tencent(name_map)
    else:
        quotes = load_realtime_quotes()

    if quotes.empty:
        logger.error("无法获取实时行情，退出")
        return

    limit_pool = load_limit_pool()

    result = ScanResult(
        scan_time=datetime.now().strftime("%H:%M:%S"),
        total_stocks=len(quotes),
    )

    all_scan = not any([args.limit, args.fast, args.volume, args.breakout, args.reversal, args.turnover])

    if args.limit or all_scan:
        result.limit_ups = scan_limit_ups(quotes, limit_pool)

    if args.fast or all_scan:
        result.fast_rise = scan_fast_rise(quotes)

    if args.volume or all_scan:
        result.volume_price_up = scan_volume_price_up(quotes)

    if args.breakout or all_scan:
        if not args.no_hist:
            result.breakout_high = scan_breakout_high(quotes, name_map)
        else:
            logger.info("跳过历史K线检查(使用--no-hist)")

    if args.reversal or all_scan:
        if not args.no_hist:
            result.reversal = scan_reversal(quotes, name_map)
        else:
            logger.info("跳过历史K线检查(使用--no-hist)")

    if args.turnover or all_scan:
        result.strong_turnover = scan_strong_turnover(quotes)

    if args.strong or all_scan:
        result.strong_signal = scan_strong_signal(quotes, name_map)

    print_results(result)

    total_signals = (
        len(result.limit_ups) + len(result.fast_rise) +
        len(result.volume_price_up) + len(result.breakout_high) +
        len(result.reversal) + len(result.strong_turnover) +
        len(result.strong_signal)
    )
    logger.info(f"扫描完成: 共发现 {total_signals} 个信号")

    save_scan_result(result, args)
    save_to_mysql(result)


def send_error_email(error_msg: str, subject: str = "实时扫描异常"):
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        import os

        smtp_host = os.environ.get('SMTP_HOST', '')
        smtp_port = int(os.environ.get('SMTP_PORT', 587))
        smtp_user = os.environ.get('SMTP_USER', '')
        smtp_pass = os.environ.get('SMTP_PASS', '')
        to_email = os.environ.get('ALERT_EMAIL', '')

        if not all([smtp_host, smtp_user, smtp_pass, to_email]):
            logger.warning("邮件配置不完整，跳过发送邮件")
            return

        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = to_email
        msg['Subject'] = f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {subject}"

        body = f"""
实时扫描监控异常

时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
错误信息: {error_msg}

请及时检查系统状态！
"""
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        logger.info("异常邮件已发送")
    except Exception as e:
        logger.error(f"发送邮件失败: {e}")


def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    current_time = now.time()
    return time(9, 30) <= current_time <= time(15, 0)


def run_continuous_scan(args):
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error("需要安装 apscheduler: pip install apscheduler")
        return

    scan_count = [0]
    error_count = [0]
    max_errors = 3

    def job_scan():
        try:
            if not is_market_open():
                now = datetime.now()
                if now.weekday() >= 5:
                    logger.info("周末不执行扫描")
                else:
                    current_time = now.time()
                    if current_time < time(9, 30):
                        logger.info("未到开盘时间 (9:30)，等待下次扫描")
                    else:
                        logger.info("已收盘，不再执行扫描")
                return

            scan_count[0] += 1
            print(f"\n{'='*80}")
            print(f"  🔄 第 {scan_count[0]} 次扫描 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}")

            name_map, industry_map, valid_codes = load_stock_list()

            if args.source == 'tencent':
                quotes = load_realtime_quotes_tencent(name_map)
            else:
                quotes = load_realtime_quotes()

            if quotes.empty:
                logger.warning("无法获取实时行情，跳过本次扫描")
                return

            limit_pool = load_limit_pool()

            result = ScanResult(
                scan_time=datetime.now().strftime("%H:%M:%S"),
                total_stocks=len(quotes),
            )

            all_scan = not any([args.limit, args.fast, args.volume, args.breakout, args.reversal, args.turnover])

            if args.limit or all_scan:
                result.limit_ups = scan_limit_ups(quotes, limit_pool)

            if args.fast or all_scan:
                result.fast_rise = scan_fast_rise(quotes)

            if args.volume or all_scan:
                result.volume_price_up = scan_volume_price_up(quotes)

            if args.breakout or all_scan:
                if not args.no_hist:
                    result.breakout_high = scan_breakout_high(quotes, name_map)
            if args.reversal or all_scan:
                if not args.no_hist:
                    result.reversal = scan_reversal(quotes, name_map)
            if args.turnover or all_scan:
                result.strong_turnover = scan_strong_turnover(quotes)
            if args.strong or all_scan:
                result.strong_signal = scan_strong_signal(quotes, name_map)

            print_results(result)

            total_signals = (
                len(result.limit_ups) + len(result.fast_rise) +
                len(result.volume_price_up) + len(result.breakout_high) +
                len(result.reversal) + len(result.strong_turnover) +
                len(result.strong_signal)
            )
            logger.info(f"第 {scan_count[0]} 次扫描完成: 共发现 {total_signals} 个信号")

            save_scan_result(result, args)
            save_to_mysql(result)

            error_count[0] = 0

        except Exception as e:
            error_count[0] += 1
            error_msg = f"扫描异常 ({error_count[0]}/{max_errors}): {str(e)}"
            logger.error(error_msg)

            if error_count[0] >= max_errors:
                send_error_email(error_msg, "实时扫描连续失败")
                error_count[0] = 0

    print(f"\n⏱️ 启动连续扫描模式: 每 {args.interval} 分钟扫描一次")
    print(f"   交易时间: 9:30-15:00 (周一至周五)")
    print(f"   异常邮件: {'已配置' if os.environ.get('SMTP_HOST') else '未配置'}")
    print("   按 Ctrl+C 停止扫描\n")

    scheduler = BlockingScheduler()

    job_scan()

    scheduler.add_job(job_scan, 'interval', minutes=args.interval, id='scan_job')
    scheduler.start()


if __name__ == '__main__':
    main()
