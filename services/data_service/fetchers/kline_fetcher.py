#!/usr/bin/env python3
"""
K线数据获取器 - 纯异步版本

架构: asyncio 单线程 + Semaphore 控制并发
- Baostock 不是线程安全的，必须单线程串行化 IO
- DataSourceManager 内置 fallback: Baostock -> Tencent -> AKShare
- 重试5次、请求延迟、断点续传、自动去重、数据验证
- 无参数默认: 检查当前数据 -> 采集30天增量 -> 质量检查 -> 邮件报告
"""
import sys
import asyncio
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import time
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

from .unified_fetcher import get_unified_fetcher
from core.logger import setup_logger
from core.delisting_guard import get_delisting_guard
from services.data_service.config.kline_config import get_kline_config

logger = setup_logger("kline_fetcher", log_file="system/kline_fetcher.log")

_yaml = get_kline_config()
_kline_cfg = _yaml.get("kline", {})
_concurrency_cfg = _yaml.get("concurrency", {})
_retry_cfg = _yaml.get("retry", {})
_request_cfg = _yaml.get("request", {})
_quality_cfg = _yaml.get("quality", {})
_optimization_cfg = _yaml.get("optimization", {})


@dataclass
class Config:
    max_concurrency: int = _concurrency_cfg.get("max_concurrency", 32)
    max_retries: int = _retry_cfg.get("max_retries", 3)
    retry_base_delay: float = _retry_cfg.get("base_delay", 0.5)
    retry_max_delay: float = _retry_cfg.get("max_delay", 10.0)
    retry_backoff: float = _retry_cfg.get("backoff", 2.0)
    request_delay: float = _request_cfg.get("delay", 0.02)
    kline_days: int = _kline_cfg.get("days", 30)
    full_days: int = _kline_cfg.get("full_days", 3650)
    min_kline_rows: int = _quality_cfg.get("min_rows", 50)
    skip_fresh_days: int = _optimization_cfg.get("skip_fresh_days", 1)
    batch_precheck: bool = _optimization_cfg.get("batch_precheck", True)
    # 双源配置
    enabled: bool = _yaml.get("dual_source", {}).get("enabled", True)


config = Config()


def _get_date_col(df: pd.DataFrame) -> str:
    return 'trade_date' if 'trade_date' in df.columns else 'date'


def validate_kline_data(df: pd.DataFrame, code: str, is_incremental: bool = False) -> Tuple[bool, str]:
    min_rows = 1 if is_incremental else config.min_kline_rows
    if len(df) < min_rows:
        return False, f"数据行数不足: {len(df)} < {min_rows}"

    required_cols = ['open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"缺少字段: {missing_cols}"

    date_col = _get_date_col(df)
    if date_col not in df.columns:
        return False, "缺少日期字段"

    if (df['close'] <= 0).any():
        return False, "存在无效收盘价(<=0)"
    if (df['open'] <= 0).any():
        return False, "存在无效开盘价(<=0)"
    if (df['volume'] < 0).any():
        return False, "存在无效成交量(<0)"
    if (df['high'] < df['low']).any():
        return False, "存在最高价<最低价异常"

    try:
        from services.data_service.quality.gx_validator import KlineDataQualitySuite
        validator = KlineDataQualitySuite.create_validator()
        result = validator.validate(df, suite_name=f"kline_{code}")
        if not result.success and result.success_rate < 0.95:
            return False, f"数据质量不达标: 成功率 {result.success_rate:.1%}"
        logger.info(f"{code} 验证通过: 成功率 {result.success_rate:.1%}")
    except Exception:
        pass

    return True, "OK"


def save_with_verification(df: pd.DataFrame, output_file: Path) -> bool:
    try:
        output_file.parent.mkdir(exist_ok=True)
        df.to_parquet(output_file, index=False)
        df_verify = pd.read_parquet(output_file)
        if len(df_verify) != len(df):
            logger.error(f"数据验证失败: 保存{len(df)}行, 读取{len(df_verify)}行")
            return False
        return True
    except Exception as e:
        logger.error(f"保存或验证失败: {e}")
        return False


def get_incremental_date_range(code: str, days: int, data_dir: Path) -> Tuple[str, str, bool]:
    end_date = datetime.now().strftime('%Y-%m-%d')
    kline_file = data_dir / f"{code}.parquet"

    if kline_file.exists():
        try:
            df_existing = pd.read_parquet(kline_file)
            date_col = _get_date_col(df_existing)
            if len(df_existing) > 0 and date_col in df_existing.columns:
                last_date = pd.to_datetime(df_existing[date_col]).max()
                if not pd.isna(last_date):
                    start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                    if start_date >= end_date:
                        return start_date, end_date, True
                    return start_date, end_date, True
        except Exception as e:
            logger.warning(f"读取{code}历史数据失败: {e}")

    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    return start_date, end_date, False


def check_data_quality(kline_dir: Path) -> Dict:
    """检查数据完整性和新鲜度"""
    kline_path = Path(kline_dir)
    if not kline_path.exists():
        return {'total': 0, 'fresh': 0, 'stale': 0, 'error': 0, 'details': {}}

    files = list(kline_path.glob("*.parquet"))
    if not files:
        return {'total': 0, 'fresh': 0, 'stale': 0, 'error': 0, 'details': {}}

    today = datetime.now()
    threshold_30d = today - timedelta(days=30)
    fresh_count = 0
    stale_count = 0
    error_count = 0
    details = {}

    for f in files:
        code = f.stem
        try:
            df = pd.read_parquet(f)
            if df.empty:
                error_count += 1
                details[code] = {'status': 'error', 'reason': '空文件'}
                continue
            date_col = _get_date_col(df)
            if date_col not in df.columns:
                error_count += 1
                details[code] = {'status': 'error', 'reason': '缺少日期字段'}
                continue
            last_date = pd.to_datetime(df[date_col]).max()
            if pd.isna(last_date):
                error_count += 1
                details[code] = {'status': 'error', 'reason': '日期为空'}
                continue
            if last_date >= threshold_30d:
                fresh_count += 1
                details[code] = {'status': 'fresh', 'last_date': last_date.strftime('%Y-%m-%d'), 'rows': len(df)}
            else:
                stale_count += 1
                details[code] = {'status': 'stale', 'last_date': last_date.strftime('%Y-%m-%d'), 'rows': len(df)}
        except Exception as e:
            error_count += 1
            details[code] = {'status': 'error', 'reason': str(e)}

    return {
        'total': len(files),
        'fresh': fresh_count,
        'stale': stale_count,
        'error': error_count,
        'fresh_pct': f"{fresh_count/len(files)*100:.1f}%" if files else "0%",
        'details': details
    }


async def fetch_single(code: str, fetcher, kline_path: Path, days: int,
                       semaphore: asyncio.Semaphore) -> Tuple[str, bool, int, str, str]:
    """采集单只股票（纯异步，信号量控制并发）"""
    async with semaphore:
        start_date, end_date, is_incremental = get_incremental_date_range(code, days, kline_path)

        if start_date > end_date:
            return code, True, 0, 'skipped', ''

        df_new = None
        last_error = ''

        for attempt in range(config.max_retries):
            try:
                await asyncio.sleep(config.request_delay)
                df_new = await fetcher.fetch_kline(code, start_date, end_date)
                if df_new is not None and not df_new.empty:
                    break
                last_error = 'no_data'
            except Exception as e:
                last_error = str(e)
                if attempt < config.max_retries - 1:
                    delay = min(config.retry_base_delay * (config.retry_backoff ** attempt), config.retry_max_delay)
                    logger.debug(f"{code} 第{attempt+1}次重试, 延迟{delay:.1f}s: {last_error}")
                    await asyncio.sleep(delay)

        if df_new is None or df_new.empty:
            return code, False, 0, f'no_data_after_{config.max_retries}_retries', last_error

        is_valid, msg = validate_kline_data(df_new, code, is_incremental=is_incremental)
        if not is_valid:
            return code, False, 0, f'validation_failed: {msg}', ''

        output_file = kline_path / f"{code}.parquet"
        if is_incremental and output_file.exists():
            try:
                df_existing = pd.read_parquet(output_file)
                date_col = _get_date_col(df_existing)
                new_date_col = _get_date_col(df_new)
                if new_date_col != date_col:
                    df_new = df_new.rename(columns={new_date_col: date_col})
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=[date_col], keep='last')
                df_combined = df_combined.sort_values(date_col).reset_index(drop=True)
                df_new = df_combined
            except Exception as e:
                logger.warning(f"合并{code}数据失败，使用新数据: {e}")

        if save_with_verification(df_new, output_file):
            return code, True, len(df_new), 'success', ''
        else:
            return code, False, 0, 'save_failed', ''


def batch_precheck_fresh(codes: List[str], kline_dir: Path, skip_days: int = 1) -> Tuple[List[str], List[str]]:
    """
    批量预检查数据新鲜度，快速跳过已最新的股票
    
    优化：只读取parquet文件的metadata获取最新日期，避免完整读取
    """
    from datetime import date as date_type
    today = date_type.today()
    threshold = today - timedelta(days=skip_days)
    
    need_update = []
    already_fresh = []
    
    kline_path = Path(kline_dir)
    
    for code in codes:
        parquet_file = kline_path / f"{code}.parquet"
        if not parquet_file.exists():
            need_update.append(code)
            continue
        
        try:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(parquet_file)
            schema = pf.schema_arrow
            
            date_col = None
            for field in schema:
                if field.name in ('date', 'trade_date'):
                    date_col = field.name
                    break
            
            if not date_col:
                need_update.append(code)
                continue
            
            df_meta = pf.read_row_group(0, columns=[date_col]).to_pandas()
            last_row_idx = pf.metadata.num_rows - 1
            row_group_idx = last_row_idx // pf.metadata.row_group(0).num_rows
            rg_count = pf.metadata.num_row_groups
            if row_group_idx < rg_count:
                df_last = pf.read_row_group(row_group_idx, columns=[date_col]).to_pandas()
                all_dates = pd.concat([df_meta[date_col], df_last[date_col]]).dropna().unique()
                latest_str = str(all_dates[-1])[:10]
            else:
                latest_str = str(df_meta[date_col].iloc[-1])[:10]
            
            try:
                latest_date = datetime.strptime(latest_str, '%Y-%m-%d').date() if latest_str and latest_str not in ('nan', 'NaT', '') else None
            except (ValueError, TypeError):
                latest_date = None
            
            if latest_date and latest_date >= threshold:
                already_fresh.append(code)
            else:
                need_update.append(code)
                
        except Exception:
            need_update.append(code)
    
    return need_update, already_fresh


async def fetch_kline_data_parallel(
    codes: List[str],
    kline_dir: Path,
    days: int = None,
    filter_delisted: bool = True,
    skip_precheck: bool = False
) -> Dict:
    """
    纯异步高并发采集K线数据

    架构: asyncio 单线程事件循环 + Semaphore
    - Baostock 不是线程安全的，禁止使用 ThreadPoolExecutor
    - DataSourceManager 内置 fallback: Baostock -> Tencent -> AKShare
    """
    days = days or config.kline_days

    if filter_delisted:
        delisting_guard = get_delisting_guard()
        original_count = len(codes)
        codes = [code for code in codes if not delisting_guard.is_delisted_by_code(code)]
        filtered_count = original_count - len(codes)
        if filtered_count > 0:
            logger.info(f"已过滤 {filtered_count} 只退市股票，剩余 {len(codes)} 只")

    if not skip_precheck and config.batch_precheck and len(codes) > 100:
        logger.info("执行批量预检查，跳过已最新数据的股票...")
        codes_to_skip, codes = batch_precheck_fresh(codes, kline_dir, config.skip_fresh_days)
        logger.info(f"预检查完成: {len(codes_to_skip)}只股票已是最新，{len(codes)}只需要更新")
    else:
        codes_to_skip = []

    logger.info(f"开始异步采集K线数据 (最近{days}天, 并发: {config.max_concurrency}, 重试: {config.max_retries})")

    kline_path = Path(kline_dir)
    kline_path.mkdir(exist_ok=True)

    fetcher = await get_unified_fetcher()
    semaphore = asyncio.Semaphore(config.max_concurrency)
    start_time = time.time()

    success_count = 0
    failed_count = 0
    skipped_count = 0
    total_rows = 0
    done_count = 0
    failed_codes: List[Dict] = []

    async def submit_task(code: str):
        nonlocal done_count, success_count, failed_count, skipped_count, total_rows
        code_r, success, rows, status, error = await fetch_single(
            code, fetcher, kline_path, days, semaphore
        )

        done_count += 1
        if success:
            if status == 'skipped':
                skipped_count += 1
            else:
                success_count += 1
                total_rows += rows
        else:
            failed_count += 1
            failed_codes.append({'code': code_r, 'status': status, 'error': error[:80] if error else ''})
            if done_count % 50 == 0 or failed_count <= 10:
                logger.warning(f"{code_r} 失败: {status}")

        if done_count % 20 == 0 or done_count == len(codes):
            pct = done_count / len(codes) * 100
            bar_len = 20
            filled = int(bar_len * done_count / len(codes))
            bar = '\u2588' * filled + '\u2591' * (bar_len - filled)
            eta = ""
            if done_count < len(codes):
                elapsed = time.time() - start_time
                rate = done_count / elapsed if elapsed > 0 else 0
                remaining = len(codes) - done_count
                eta = f" ETA {remaining / rate:.0f}s" if rate > 0 else ""
            logger.info(f"[{bar}] {pct:5.1f}% {done_count}/{len(codes)} "
                        f"\u2713{success_count} \u2298{skipped_count} \u2717{failed_count}{eta}")

        return code_r, success, rows, status

    tasks = [submit_task(code) for code in codes]
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.time() - start_time
    logger.info(f"K线采集完成: \u2713{success_count} \u2298{skipped_count} \u2717{failed_count} "
                f"(预跳过{len(codes_to_skip)}) | {total_rows}行 | 耗时{elapsed:.0f}s")

    return {
        'success': success_count,
        'skipped': skipped_count,
        'precheck_skipped': len(codes_to_skip),
        'failed': failed_count,
        'total_rows': total_rows,
        'elapsed_seconds': round(elapsed, 1),
        'failed_codes': failed_codes
    }


def fetch_kline_data_parallel_sync(
    codes: List[str],
    kline_dir: Path,
    days: int = None,
    filter_delisted: bool = True
) -> Dict:
    """同步入口"""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            fetch_kline_data_parallel(codes, kline_dir, days, filter_delisted)
        )
    finally:
        loop.close()


async def fetch_kline_for_stock_async(code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    fetcher = await get_unified_fetcher()
    return await fetcher.fetch_kline(code, start_date, end_date)


def fetch_kline_for_stock(code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(fetch_kline_for_stock_async(code, start_date, end_date))
    finally:
        loop.close()


fetch_kline_batch_microservice = fetch_kline_data_parallel_sync
fetch_kline_data_parallel_microservice = fetch_kline_data_parallel_sync


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="K线数据采集CLI（纯异步版）")
    parser.add_argument("--codes", type=str, default="all", help="股票代码，逗号分隔，默认all")
    parser.add_argument("--days", type=int, default=None, help="采集最近N天（默认30天增量）")
    parser.add_argument("--years", type=int, default=None, help="采集最近N年")
    parser.add_argument("--output-dir", type=str, default="data/kline", help="输出目录")
    parser.add_argument("--no-filter-delisted", action="store_true", help="不过滤退市股票")
    parser.add_argument("--quality-only", action="store_true", help="仅检查数据质量，不采集")
    parser.add_argument("--max-retries", type=int, default=config.max_retries, help="最大重试次数")
    parser.add_argument("--task-id", type=str, default=None, help="任务ID（用于日志追踪）")
    parser.add_argument("--dual-source", action="store_true", default=config.enabled if hasattr(config, 'enabled') else False,
                        help="启用双源采集模式(Baostock历史+腾讯近期)")
    parser.add_argument("--single-source", action="store_true", help="强制使用单源模式(兼容旧逻辑)")
    # 断点续传参数
    parser.add_argument("--no-resume", action="store_true", help="禁用断点续传，从头开始")
    parser.add_argument("--clear-checkpoint", action="store_true", help="清除进度文件后开始")
    parser.add_argument("--status", action="store_true", help="查看当前采集进度状态")
    # 快速检查参数
    parser.add_argument("--quick-check-only", action="store_true", help="仅执行快速检查，不实际采集")

    args = parser.parse_args()

    if args.years is not None:
        days = args.years * 365
    elif args.days is not None:
        days = args.days
    else:
        days = config.kline_days

    # 查看状态
    if args.status:
        from .dual_source_fetcher import CheckpointManager
        checkpoint = CheckpointManager()
        summary = checkpoint.get_summary()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        sys.exit(0)

    # 清除进度
    if args.clear_checkpoint:
        from .dual_source_fetcher import CheckpointManager
        checkpoint = CheckpointManager()
        checkpoint.clear()
        print("进度文件已清除")
        sys.exit(0)

    if args.codes == "all":
        stock_list_path = Path("data/stock_list.parquet")
        if stock_list_path.exists():
            df = pd.read_parquet(stock_list_path)
            col = 'code' if 'code' in df.columns else 'ts_code'
            codes = df[col].astype(str).str.zfill(6).tolist()
        else:
            print("ERROR: data/stock_list.parquet not found")
            sys.exit(1)
    else:
        codes = [c.strip().zfill(6) for c in args.codes.split(",")]

    kline_dir = Path(args.output_dir)

    if args.quality_only:
        print("检查数据质量...")
        quality = check_data_quality(kline_dir)
        print(json.dumps({k: v for k, v in quality.items() if k != 'details'}, indent=2, ensure_ascii=False))
        sys.exit(0)

    # 判断使用双源还是单源模式
    use_dual_source = args.dual_source and not args.single_source

    if use_dual_source:
        print(f"=== 双源采集模式 ===")
        print(f"历史数据源: Baostock (分割日期之前)")
        print(f"近期数据源: 腾讯 (分割日期之后, 快5-10倍)")
        print(f"股票来源: Redis缓存")
        print(f"开始采集 {len(codes)} 只股票...")

        from .dual_source_fetcher import run_dual_source_fetch

        result = run_dual_source_fetch(
            codes=codes,
            kline_dir=str(kline_dir),
            days=days,
            filter_delisted=not args.no_filter_delisted,
            resume=not args.no_resume
        )

        # 转换结果格式以兼容后续逻辑
        result_formatted = {
            'success': result.get('success', 0),
            'skipped': result.get('skipped', 0) + result.get('cache_hits', 0),
            'failed': result.get('failed', 0),
            'total_rows': 0,
            'elapsed_seconds': result.get('elapsed_seconds', 0),
            'mode': 'dual_source',
            'baostock_stats': result.get('baostock_stats', {}),
            'tencent_stats': result.get('tencent_stats', {}),
            'split_date': result.get('split_date', '')
        }
        result = result_formatted
    else:
        print(f"开始采集 {len(codes)} 只股票，最近 {days} 天")
        result = fetch_kline_data_parallel_sync(
            codes=codes,
            kline_dir=kline_dir,
            days=days,
            filter_delisted=not args.no_filter_delisted
        )

    quality = check_data_quality(kline_dir)
    result['quality'] = {k: v for k, v in quality.items() if k != 'details'}

    print(json.dumps(result, indent=2, ensure_ascii=False))

    try:
        from services.email_sender import send_report_email

        mode = '全量' if days >= 365 else '增量'
        subject = f"[K线采集报告] {mode} {days}天 | \u2713{result['success']} \u2717{result['failed']}"
        failed_list = ""
        if result['failed_codes']:
            top_failures = result['failed_codes'][:20]
            failed_list = "\n\n失败股票(Top20):\n" + "\n".join(
                f"  {f['code']}: {f['status']}" for f in top_failures
            )
            if len(result['failed_codes']) > 20:
                failed_list += f"\n  ... 共{len(result['failed_codes'])}只"

        body = (
            f"K线数据采集报告\n"
            f"{'='*40}\n"
            f"采集模式: {mode}\n"
            f"采集范围: 最近 {days} 天\n"
            f"股票总数: {len(codes)}\n"
            f"成功: {result['success']}\n"
            f"跳过(已是最新): {result['skipped']}\n"
            f"失败: {result['failed']}\n"
            f"累计行数: {result['total_rows']:,}\n"
            f"耗时: {result['elapsed_seconds']:.0f}秒\n"
            f"{'='*40}\n"
            f"数据质量:\n"
            f"  总文件: {quality['total']}\n"
            f"  新鲜(<30天): {quality['fresh']} ({quality['fresh_pct']})\n"
            f"  过时(>30天): {quality['stale']}\n"
            f"  异常文件: {quality['error']}\n"
            f"{'='*40}\n"
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            f"{failed_list}"
        )
        ok = send_report_email(subject, body)
        print("邮件报告已发送" if ok else "邮件发送失败（不影响主流程）")
    except Exception as e:
        print(f"邮件发送失败（不影响主流程）: {e}")

    sys.exit(0)
