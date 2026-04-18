#!/usr/bin/env python3
"""
使用Baostock获取完整的基本面数据 - 修复优化版本
参考文档: https://www.baostock.com/mainContent?file=stockKData.md

修复内容:
1. 添加指数退避重试机制
2. 分级异常处理+详细日志
3. 数据完整性检查
4. 内存效率优化（懒加载）
5. 参数配置化
6. 增量更新策略
7. 数据校验机制
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import time
import logging
import json
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from functools import wraps

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / 'logs' / 'fetch_baostock.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 确保日志目录存在
(PROJECT_ROOT / 'logs').mkdir(exist_ok=True)


@dataclass
class Config:
    """配置类"""
    # 并发配置
    max_workers: int = 4
    batch_size: int = 50
    
    # 重试配置
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    
    # 请求频率控制
    request_delay: float = 0.02
    batch_delay: float = 0.5
    
    # 数据配置
    kline_days: int = 365 * 3
    fundamental_years: int = 3
    min_kline_rows: int = 100  # 最少K线数据行数
    
    # 数据质量阈值
    max_pe: float = 1000.0
    max_pb: float = 100.0
    max_roe: float = 200.0


# 全局配置
config = Config()


def retry_on_error(max_retries: int = None, base_delay: float = None):
    """
    指数退避重试装饰器
    
    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟秒数
    """
    max_retries = max_retries or config.max_retries
    base_delay = base_delay or config.retry_base_delay
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        # 指数退避: 1s, 2s, 4s, 8s...
                        delay = min(base_delay * (2 ** attempt), config.retry_max_delay)
                        logger.warning(f"{func.__name__} 第{attempt + 1}次尝试失败: {e}, {delay}s后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"{func.__name__} 所有{max_retries + 1}次尝试均失败")
            
            raise last_exception
        return wrapper
    return decorator


def convert_code(code: str) -> str:
    """转换代码格式为baostock格式"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz.{code}"
    return f"sz.{code}"


def validate_kline_data(df: pd.DataFrame, code: str) -> Tuple[bool, str]:
    """
    验证K线数据完整性
    
    Returns:
        (是否有效, 错误信息)
    """
    if len(df) < config.min_kline_rows:
        return False, f"数据行数不足: {len(df)} < {config.min_kline_rows}"
    
    # 检查必要字段
    required_cols = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"缺少字段: {missing_cols}"
    
    # 检查价格合理性
    if (df['close'] <= 0).any():
        return False, "存在无效收盘价"
    
    # 检查OHLC逻辑
    invalid_ohlc = (
        (df['high'] < df['low']) |
        (df['high'] < df['open']) |
        (df['high'] < df['close']) |
        (df['low'] > df['open']) |
        (df['low'] > df['close'])
    )
    if invalid_ohlc.any():
        n_invalid = invalid_ohlc.sum()
        logger.warning(f"{code} 存在{n_invalid}条OHLC逻辑异常数据")
    
    return True, "OK"


def save_with_verification(df: pd.DataFrame, output_file: Path) -> bool:
    """
    保存数据并验证完整性
    
    Returns:
        是否成功
    """
    try:
        # 保存
        pl.from_pandas(df).write_parquet(output_file)
        
        # 验证
        df_verify = pl.read_parquet(output_file)
        if len(df_verify) != len(df):
            logger.error(f"数据验证失败: 保存{len(df)}行, 读取{len(df_verify)}行")
            return False
        
        return True
    except Exception as e:
        logger.error(f"保存或验证失败: {e}")
        return False


@retry_on_error(max_retries=3)
def fetch_single_kline_with_retry(bs, code: str, start_date: str, end_date: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    获取单只股票K线数据（带重试）
    
    Returns:
        (是否成功, DataFrame, 错误信息)
    """
    try:
        bs_code = convert_code(code)
        
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2"
        )
        
        if rs.error_code != '0':
            return False, pd.DataFrame(), f"API错误: {rs.error_msg}"
        
        data_list = []
        row_count = 0
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
            row_count += 1
        
        if not data_list:
            return False, pd.DataFrame(), "无数据返回"
        
        df = pd.DataFrame(data_list)
        
        # 验证数据完整性
        is_valid, msg = validate_kline_data(df, code)
        if not is_valid:
            return False, df, f"数据验证失败: {msg}"
        
        return True, df, "OK"
        
    except Exception as e:
        logger.exception(f"获取{code}K线数据异常")
        raise


def get_incremental_date_range(code: str, days: int) -> Tuple[str, str]:
    """
    获取增量更新的日期范围
    
    Returns:
        (start_date, end_date)
    """
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    # 检查本地已有数据
    kline_file = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"
    if kline_file.exists():
        try:
            df_existing = pl.read_parquet(kline_file)
            if len(df_existing) > 0:
                last_date = df_existing['trade_date'].max()
                # 从最后一天的下一天开始
                last_date = datetime.strptime(last_date, '%Y-%m-%d')
                start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                logger.debug(f"{code} 增量更新: {start_date} 至 {end_date}")
                return start_date, end_date
        except Exception as e:
            logger.warning(f"读取{code}历史数据失败: {e}")
    
    # 全量更新
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    return start_date, end_date


def fetch_kline_data_enhanced(bs, codes: List[str], days: int = None) -> int:
    """
    增强版K线数据采集（带重试、验证、增量更新）
    """
    days = days or config.kline_days
    
    print("\n" + "=" * 80)
    print(f"获取K线历史行情数据 (最近{days}天, 增量更新)")
    print("=" * 80)
    
    kline_dir = PROJECT_ROOT / "data" / "kline"
    kline_dir.mkdir(exist_ok=True)
    
    success_count = 0
    failed_codes = []
    skipped_count = 0
    
    for i, code in enumerate(codes):
        try:
            # 获取增量日期范围
            start_date, end_date = get_incremental_date_range(code, days)
            
            # 如果已经是最新数据，跳过
            if start_date > end_date:
                skipped_count += 1
                continue
            
            # 获取数据（带重试）
            success, df_new, msg = fetch_single_kline_with_retry(bs, code, start_date, end_date)
            
            if not success:
                failed_codes.append((code, msg))
                continue
            
            # 合并现有数据（增量模式）
            output_file = kline_dir / f"{code}.parquet"
            if output_file.exists():
                try:
                    df_existing = pl.read_parquet(output_file).to_pandas()
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=['trade_date'], keep='last')
                    df_combined = df_combined.sort_values('trade_date')
                    df_new = df_combined
                except Exception as e:
                    logger.warning(f"合并{code}数据失败，使用新数据: {e}")
            
            # 保存并验证
            if save_with_verification(df_new, output_file):
                success_count += 1
            else:
                failed_codes.append((code, "保存验证失败"))
            
            # 进度报告
            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只, 跳过 {skipped_count} 只")
            
            time.sleep(config.request_delay)
            
        except Exception as e:
            logger.exception(f"处理{code}时发生未预期错误")
            failed_codes.append((code, str(e)))
    
    print(f"\nK线数据获取完成: {success_count}/{len(codes)} 只成功, {skipped_count} 只已是最新")
    if failed_codes:
        print(f"失败: {len(failed_codes)} 只")
        # 记录失败详情到日志
        for code, reason in failed_codes[:10]:  # 只显示前10个
            logger.error(f"  {code}: {reason}")
    
    return success_count


@retry_on_error(max_retries=3)
def fetch_single_valuation_with_retry(bs, code: str, start_date: str, end_date: str) -> Tuple[bool, pd.DataFrame, str]:
    """获取单只股票估值数据（带重试）"""
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
        
        if rs.error_code != '0':
            return False, pd.DataFrame(), f"API错误: {rs.error_msg}"
        
        data_list = []
        while rs.next():
            row = rs.get_row_data()
            pe = float(row[2]) if row[2] and row[2] != '' else None
            pb = float(row[3]) if row[3] and row[3] != '' else None
            ps = float(row[4]) if row[4] and row[4] != '' else None
            pcf = float(row[5]) if row[5] and row[5] != '' else None
            turnover = float(row[6]) if row[6] and row[6] != '' else None
            
            # 过滤异常值
            if pe and 0 < pe < config.max_pe:
                data_list.append({
                    'trade_date': row[0],
                    'code': code,
                    'pe_ttm': pe,
                    'pb': pb if pb and 0 < pb < config.max_pb else None,
                    'ps_ttm': ps if ps and 0 < ps < config.max_pe else None,
                    'pcf': pcf if pcf and abs(pcf) < config.max_pe else None,
                    'turnover': turnover,
                })
        
        if not data_list:
            return False, pd.DataFrame(), "无有效数据"
        
        return True, pd.DataFrame(data_list), "OK"
        
    except Exception as e:
        logger.exception(f"获取{code}估值数据异常")
        raise


def fetch_valuation_data_enhanced(bs, codes: List[str], days: int = None) -> int:
    """增强版估值数据采集"""
    days = days or config.kline_days
    
    print("\n" + "=" * 80)
    print(f"获取估值数据历史 (最近{days}天, 增量更新)")
    print("=" * 80)
    
    valuation_dir = PROJECT_ROOT / "data" / "fundamental" / "valuation_daily"
    valuation_dir.mkdir(exist_ok=True, parents=True)
    
    success_count = 0
    failed_codes = []
    
    for i, code in enumerate(codes):
        try:
            # 增量日期
            start_date, end_date = get_incremental_date_range(code, days)
            if start_date > end_date:
                continue
            
            # 获取数据
            success, df_new, msg = fetch_single_valuation_with_retry(bs, code, start_date, end_date)
            
            if not success:
                failed_codes.append((code, msg))
                continue
            
            # 合并
            output_file = valuation_dir / f"{code}.parquet"
            if output_file.exists():
                try:
                    df_existing = pl.read_parquet(output_file).to_pandas()
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=['trade_date'], keep='last')
                    df_combined = df_combined.sort_values('trade_date')
                    df_new = df_combined
                except Exception as e:
                    logger.warning(f"合并{code}估值数据失败: {e}")
            
            # 保存
            if save_with_verification(df_new, output_file):
                success_count += 1
            else:
                failed_codes.append((code, "保存失败"))
            
            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")
            
            time.sleep(config.request_delay)
            
        except Exception as e:
            logger.exception(f"处理{code}估值数据时错误")
            failed_codes.append((code, str(e)))
    
    print(f"\n估值数据获取完成: {success_count}/{len(codes)} 只")
    if failed_codes:
        print(f"失败: {len(failed_codes)} 只")
    
    return success_count


@retry_on_error(max_retries=3)
def fetch_fundamental_data_with_retry(bs, code: str, data_type: str, years: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    获取基本面数据（带重试）
    
    Args:
        data_type: 'profit', 'growth', 'operation', 'balance', 'dupont'
    """
    try:
        bs_code = convert_code(code)
        data_list = []
        
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1
        
        for year in range(current_year - years + 1, current_year + 1):
            for quarter in range(1, 5):
                if year == current_year and quarter > current_quarter:
                    continue
                
                # 根据类型调用不同API
                if data_type == 'profit':
                    rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
                elif data_type == 'growth':
                    rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)
                elif data_type == 'operation':
                    rs = bs.query_operation_data(code=bs_code, year=year, quarter=quarter)
                elif data_type == 'balance':
                    rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)
                elif data_type == 'dupont':
                    rs = bs.query_dupont_data(code=bs_code, year=year, quarter=quarter)
                else:
                    return False, pd.DataFrame(), f"未知数据类型: {data_type}"
                
                if rs.error_code == '0' and rs.next():
                    row = rs.get_row_data()
                    record = {'code': code, 'year': year, 'quarter': quarter}
                    
                    # 根据类型解析字段
                    if data_type == 'profit':
                        record['roe'] = float(row[4]) if len(row) > 4 and row[4] else None
                        record['roa'] = float(row[5]) if len(row) > 5 and row[5] else None
                        record['gross_margin'] = float(row[6]) if len(row) > 6 and row[6] else None
                        record['net_margin'] = float(row[7]) if len(row) > 7 and row[7] else None
                    elif data_type == 'growth':
                        record['revenue_growth'] = float(row[3]) if len(row) > 3 and row[3] else None
                        record['profit_growth'] = float(row[4]) if len(row) > 4 and row[4] else None
                    elif data_type == 'operation':
                        record['inventory_turnover'] = float(row[3]) if len(row) > 3 and row[3] else None
                        record['ar_turnover'] = float(row[4]) if len(row) > 4 and row[4] else None
                    elif data_type == 'balance':
                        record['current_ratio'] = float(row[3]) if len(row) > 3 and row[3] else None
                        record['quick_ratio'] = float(row[4]) if len(row) > 4 and row[4] else None
                    elif data_type == 'dupont':
                        record['dupont_roe'] = float(row[3]) if len(row) > 3 and row[3] else None
                    
                    data_list.append(record)
                
                time.sleep(0.01)
        
        if not data_list:
            return False, pd.DataFrame(), "无数据"
        
        return True, pd.DataFrame(data_list), "OK"
        
    except Exception as e:
        logger.exception(f"获取{code} {data_type}数据异常")
        raise


def fetch_all_fundamental_data_enhanced(bs, codes: List[str], years: int = None) -> Dict[str, int]:
    """批量获取所有基本面数据"""
    years = years or config.fundamental_years
    
    data_types = ['profit', 'growth', 'operation', 'balance', 'dupont']
    results = {}
    
    for data_type in data_types:
        print(f"\n{'=' * 80}")
        print(f"获取{data_type}数据历史 (最近{years}年)")
        print("=" * 80)
        
        output_dir = PROJECT_ROOT / "data" / "fundamental" / f"{data_type}_quarterly"
        output_dir.mkdir(exist_ok=True, parents=True)
        
        success_count = 0
        failed_codes = []
        
        for i, code in enumerate(codes):
            try:
                success, df_new, msg = fetch_fundamental_data_with_retry(bs, code, data_type, years)
                
                if not success:
                    failed_codes.append((code, msg))
                    continue
                
                # 合并
                output_file = output_dir / f"{code}.parquet"
                if output_file.exists():
                    try:
                        df_existing = pl.read_parquet(output_file).to_pandas()
                        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                        df_combined = df_combined.drop_duplicates(subset=['year', 'quarter'], keep='last')
                        df_combined = df_combined.sort_values(['year', 'quarter'])
                        df_new = df_combined
                    except Exception as e:
                        logger.warning(f"合并{code} {data_type}数据失败: {e}")
                
                if save_with_verification(df_new, output_file):
                    success_count += 1
                else:
                    failed_codes.append((code, "保存失败"))
                
                if (i + 1) % 100 == 0:
                    print(f"  已处理 {i + 1}/{len(codes)} 只, 成功 {success_count} 只")
                
                time.sleep(config.request_delay)
                
            except Exception as e:
                logger.exception(f"处理{code} {data_type}数据时错误")
                failed_codes.append((code, str(e)))
        
        results[data_type] = success_count
        print(f"\n{data_type}数据获取完成: {success_count}/{len(codes)} 只")
    
    return results


@retry_on_error(max_retries=3)
def fetch_industry_data_enhanced(bs) -> int:
    """获取行业分类数据（带重试）"""
    print("\n" + "=" * 80)
    print("获取行业分类数据")
    print("=" * 80)
    
    try:
        rs = bs.query_stock_industry()
        
        if rs.error_code != '0':
            logger.error(f"获取行业数据失败: {rs.error_msg}")
            return 0
        
        data_list = []
        while rs.next():
            row = rs.get_row_data()
            code = row[1].split('.')[-1] if '.' in row[1] else row[1]
            data_list.append({
                'code': code,
                'industry': row[3] if len(row) > 3 else '',
                'industry_classification': row[4] if len(row) > 4 else '',
            })
        
        if data_list:
            df = pd.DataFrame(data_list)
            output_file = PROJECT_ROOT / "data" / "fundamental" / "industry_data.parquet"
            if save_with_verification(df, output_file):
                print(f"行业数据已保存: {len(data_list)} 条")
                return len(data_list)
        
        return 0
        
    except Exception as e:
        logger.exception("获取行业数据异常")
        raise


def merge_all_data_enhanced():
    """增强版数据合并（内存优化）"""
    print("\n" + "=" * 80)
    print("合并所有数据到股票列表")
    print("=" * 80)
    
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    df_stocks = pl.read_parquet(stock_list_file)
    print(f"原始股票列表: {len(df_stocks)} 只")
    
    # 估值数据 - 使用懒加载
    valuation_dir = PROJECT_ROOT / "data" / "fundamental" / "valuation_daily"
    if valuation_dir.exists():
        valuation_latest = []
        parquet_files = list(valuation_dir.glob("*.parquet"))
        
        # 分批处理，避免内存溢出
        batch_size = 500
        for i in range(0, len(parquet_files), batch_size):
            batch_files = parquet_files[i:i+batch_size]
            for f in batch_files:
                try:
                    # 使用懒加载只读取需要的列
                    df_val = pl.scan_parquet(f).select(['trade_date', 'code', 'pe_ttm', 'pb']).collect()
                    if len(df_val) > 0:
                        latest = df_val.sort('trade_date').tail(1)
                        valuation_latest.append({
                            'code': latest['code'][0],
                            'pe_ttm': latest['pe_ttm'][0],
                            'pb': latest['pb'][0],
                        })
                except Exception as e:
                    logger.debug(f"读取{f}失败: {e}")
                    continue
        
        if valuation_latest:
            df_val_latest = pl.DataFrame(valuation_latest)
            df_stocks = df_stocks.join(df_val_latest, on='code', how='left')
            pe_count = df_stocks.filter(pl.col('pe_ttm').is_not_null()).shape[0]
            print(f"合并估值数据: {pe_count}/{len(df_stocks)} 只")
    
    # 盈利数据
    profit_dir = PROJECT_ROOT / "data" / "fundamental" / "profit_quarterly"
    if profit_dir.exists():
        profit_latest = []
        parquet_files = list(profit_dir.glob("*.parquet"))
        
        for i in range(0, len(parquet_files), batch_size):
            batch_files = parquet_files[i:i+batch_size]
            for f in batch_files:
                try:
                    df_profit = pl.scan_parquet(f).select(['year', 'quarter', 'code', 'roe']).collect()
                    if len(df_profit) > 0:
                        latest = df_profit.sort(['year', 'quarter']).tail(1)
                        profit_latest.append({
                            'code': latest['code'][0],
                            'roe': latest['roe'][0],
                        })
                except:
                    continue
        
        if profit_latest:
            df_profit_latest = pl.DataFrame(profit_latest)
            df_stocks = df_stocks.join(df_profit_latest, on='code', how='left')
            roe_count = df_stocks.filter(pl.col('roe').is_not_null()).shape[0]
            print(f"合并盈利数据: {roe_count}/{len(df_stocks)} 只")
    
    # 保存
    df_stocks.write_parquet(stock_list_file)
    print(f"\n已更新股票列表")
    
    return df_stocks


def get_stock_list_from_baostock(bs) -> List[str]:
    """从Baostock获取股票列表"""
    print("\n" + "=" * 80)
    print("从Baostock获取所有股票列表")
    print("=" * 80)
    
    for i in range(10):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        print(f"  尝试日期: {date}")
        
        try:
            rs = bs.query_all_stock(day=date)
            
            stock_list = []
            while rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
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
        except Exception as e:
            logger.warning(f"获取{date}股票列表失败: {e}")
            continue
    
    if len(stock_list) == 0:
        logger.error("无法获取股票列表")
        return []
    
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(exist_ok=True)
    
    df = pd.DataFrame(stock_list)
    output_file = output_dir / "stock_list.parquet"
    
    if save_with_verification(df, output_file):
        print(f"股票列表已保存: {output_file}")
    
    return [s['code'] for s in stock_list]


def main():
    """主函数"""
    print("=" * 80)
    print("使用Baostock获取股票数据 - 修复优化版本")
    print(f"配置: 重试{config.max_retries}次, 增量更新, 数据验证")
    print("=" * 80)
    
    try:
        import baostock as bs
    except ImportError:
        logger.info("安装 baostock...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "baostock", "-q"])
        import baostock as bs
    
    # 登录
    print("\n登录Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"登录失败: {lg.error_msg}")
        return
    print("登录成功!")
    
    try:
        # 获取股票列表
        codes = get_stock_list_from_baostock(bs)
        if not codes:
            return
        
        print(f"\n总共 {len(codes)} 只股票需要处理")
        
        # K线数据（增量+重试+验证）
        fetch_kline_data_enhanced(bs, codes)
        
        # 估值数据
        fetch_valuation_data_enhanced(bs, codes)
        
        # 基本面数据
        fetch_all_fundamental_data_enhanced(bs, codes)
        
        # 行业数据
        fetch_industry_data_enhanced(bs)
        
        # 合并数据
        merge_all_data_enhanced()
        
        print("\n" + "=" * 80)
        print("所有数据采集完成!")
        print("=" * 80)
        
    except Exception as e:
        logger.exception("主程序异常")
    finally:
        bs.logout()
        print("\n已退出Baostock")


if __name__ == "__main__":
    main()
