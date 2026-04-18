#!/usr/bin/env python3
"""
K线数据采集任务 V2 - 微服务版本

核心约定（重要！）：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. 交易日盘中（9:30-15:00）禁止采集当日K线数据
2. 当日数据必须在收盘后（15:30+）采集
3. 历史数据可在任何时间采集（包括盘中、周末）
4. 强制指定 --date 参数可采集历史数据

允许场景：
  ✅ 交易日 15:30 后 → 采集当日数据
  ✅ 非交易日任何时间 → 采集历史数据
  ✅ --date 历史日期 → 采集指定历史数据

禁止场景：
  ❌ 交易日 9:30-15:00 采集当日数据 → 强制退出
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

特性：
- 集成主备数据源，自动故障转移
- 自动区分当日/历史数据，应用不同采集规则
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import asyncio
import polars as pl
from pathlib import Path
from datetime import datetime, time
import json
import argparse
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from core.trading_calendar import check_market_status
from core.market_guardian import enforce_market_closed
from core.logger import setup_logger
from services.data_service.datasource import get_datasource_manager
from services.data_service.fetchers.unified_fetcher import UnifiedFetcher

logger = setup_logger("data_collect_v2", log_file="system/data_collect_v2.log")


class DataCollectorV2:
    """
    数据采集器 V2
    
    特性：
    1. 调用微服务架构
    2. 主备数据源自动切换
    3. 统一数据格式
    4. 断点续传
    """

    def __init__(self, force_date=None, retry_mode=False):
        self.project_root = Path(__file__).parent.parent.parent
        self.kline_dir = self.project_root / "data" / "kline"
        self.force_date = force_date
        self.retry_mode = retry_mode
        self.ds_manager = None
        self.fetcher = None
        
        # 确保目录存在
        self.kline_dir.mkdir(parents=True, exist_ok=True)

    async def initialize(self):
        """初始化数据源"""
        self.ds_manager = get_datasource_manager()
        await self.ds_manager.initialize()
        
        self.fetcher = UnifiedFetcher()
        await self.fetcher.initialize()
        
        logger.info("✅ 数据采集器 V2 初始化完成")

    async def run(self):
        """执行采集"""
        await self.initialize()
        
        logger.info("=" * 60)
        logger.info("开始采集K线数据 (微服务版本)")
        logger.info("=" * 60)

        # K线数据采集时间检查
        # 核心约定：交易日盘中禁止采集当日数据，历史数据随时可采
        if self.force_date:
            # 强制模式：采集指定历史日期，不受时间限制
            logger.info(f"强制重新采集历史数据: {self.force_date}")
        else:
            # 正常模式：检查是否允许采集当日数据
            # 交易日盘中会强制退出！
            today = datetime.now().strftime('%Y-%m-%d')
            enforce_market_closed(target_date=today)

        # 创建锁文件
        if not self.retry_mode:
            self._create_lock()

        # 断点续传模式检查
        if self.retry_mode:
            logger.info("断点续传模式：只采集缺失最新日期数据的股票")
            if self._check_retry_conflict():
                logger.error("❌ 检测到与初始采集冲突，退出")
                return

        # 获取股票列表
        codes = await self._get_stock_codes()
        
        if self.retry_mode:
            codes = self._filter_missing_codes(codes)
            logger.info(f"断点续传：需要补充的股票数量: {len(codes)}")
        
        logger.info(f"共 {len(codes)} 只股票待采集")

        # 执行采集
        stats = await self._collect_batch(codes)
        
        logger.info("=" * 60)
        logger.info(f"采集完成统计:")
        logger.info(f"  ✅ 新增: {stats['success']}")
        logger.info(f"  🔄 更新: {stats['updated']}")
        logger.info(f"  ⏭️  跳过: {stats['skipped']}")
        logger.info(f"  ❌ 失败: {stats['failed']}")
        logger.info("=" * 60)
        
        # 清理锁文件
        self._remove_lock()

    def _check_market_closed(self) -> bool:
        """检查市场是否已收盘"""
        market_status = check_market_status()

        if not market_status['is_trading_day']:
            logger.info(f"非交易日（周末或节假日），跳过数据采集")
            return False

        if market_status['is_after_market_close']:
            return True

        current_time = datetime.now().time()
        if current_time >= time(15, 30):
            return True

        logger.info(f"交易日盘中（{datetime.now().strftime('%H:%M')}），需等待收盘后采集")
        return False

    def _create_lock(self):
        """创建锁文件"""
        lock_file = self.project_root / "data" / ".data_fetch.lock"
        with open(lock_file, 'w') as f:
            json.dump({
                'pid': os.getpid(),
                'start_time': datetime.now().isoformat(),
                'version': 'v2'
            }, f)
        logger.info(f"锁文件已创建: {lock_file}")

    def _remove_lock(self):
        """移除锁文件"""
        lock_file = self.project_root / "data" / ".data_fetch.lock"
        if lock_file.exists():
            lock_file.unlink()
            logger.info(f"锁文件已移除: {lock_file}")

    def _check_retry_conflict(self) -> bool:
        """检查是否与初始采集冲突"""
        lock_file = self.project_root / "data" / ".data_fetch.lock"

        if not lock_file.exists():
            return False

        try:
            with open(lock_file, 'r') as f:
                data = json.load(f)
                pid = data.get('pid')

            import psutil
            if pid and psutil.pid_exists(pid):
                logger.warning(f"⚠️ 检测到初始采集仍在运行 (PID: {pid})，重试任务退出")
                return True

            logger.info("发现残留锁文件，清理后继续")
            lock_file.unlink()
        except:
            if lock_file.exists():
                lock_file.unlink()

        return False

    async def _get_stock_codes(self) -> List[str]:
        """获取股票代码列表"""
        # 优先从微服务获取
        try:
            df = await self.fetcher.fetch_stock_list()
            if not df.empty:
                codes = df['code'].astype(str).tolist()
                logger.info(f"从微服务获取到 {len(codes)} 只股票")
                return codes
        except Exception as e:
            logger.warning(f"从微服务获取股票列表失败: {e}")
        
        # 回退到本地文件
        codes = []
        if self.kline_dir.exists():
            for f in sorted(self.kline_dir.glob("*.parquet")):
                codes.append(f.stem)
        
        logger.info(f"从本地文件获取到 {len(codes)} 只股票")
        return codes

    def _filter_missing_codes(self, all_codes: List[str]) -> List[str]:
        """过滤缺失最新日期数据的股票"""
        missing_codes = []
        
        latest_date = self._get_latest_trade_date()
        if not latest_date:
            logger.warning("无法获取最新交易日期，返回全部股票")
            return all_codes
        
        logger.info(f"检查缺失最新日期 {latest_date} 数据的股票...")
        
        for code in all_codes:
            parquet_file = self.kline_dir / f"{code}.parquet"
            if not parquet_file.exists():
                missing_codes.append(code)
                continue
            
            try:
                df = pl.read_parquet(parquet_file)
                dates = df["trade_date"].to_list()
                if latest_date not in dates:
                    missing_codes.append(code)
            except:
                missing_codes.append(code)
        
        return missing_codes
    
    def _get_latest_trade_date(self) -> Optional[str]:
        """获取最新交易日期"""
        from datetime import timedelta
        
        today = datetime.now()
        
        for i in range(7):
            check_date = today - timedelta(days=i)
            weekday = check_date.weekday()
            
            if weekday < 5:  # 周一到周五
                return check_date.strftime("%Y-%m-%d")
        
        return None

    async def _collect_batch(self, codes: List[str]) -> Dict[str, int]:
        """
        批量采集
        
        使用异步并发，同时利用主备数据源
        """
        stats = {
            'success': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0
        }
        stats_lock = Lock()

        async def fetch_with_stats(code: str):
            """带统计的采集函数"""
            result = await self._fetch_single(code)
            with stats_lock:
                stats[result] = stats.get(result, 0) + 1
            
            # 定期输出进度
            total_processed = sum(stats.values())
            if total_processed % 100 == 0:
                logger.info(f"进度: {total_processed}/{len(codes)} | "
                          f"✅{stats['success']} 🔄{stats['updated']} "
                          f"⏭️{stats['skipped']} ❌{stats['failed']}")
            
            return result

        # 使用信号量限制并发数
        semaphore = asyncio.Semaphore(10)
        
        async def fetch_with_semaphore(code: str):
            async with semaphore:
                return await fetch_with_stats(code)

        logger.info(f"启动并发采集: 最大10并发")
        
        # 执行所有任务
        tasks = [fetch_with_semaphore(code) for code in codes]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        return stats

    def _validate_kline_data(self, df: pl.DataFrame, code: str) -> tuple[bool, str]:
        """
        验证K线数据合理性
        
        Args:
            df: K线数据DataFrame
            code: 股票代码
        Returns:
            (是否有效, 错误信息)
        """
        try:
            if df.is_empty():
                return False, "数据为空"
            
            # 检查必要字段
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                return False, f"缺少字段: {missing_cols}"
            
            # 获取最新一行数据
            latest = df.tail(1).to_dicts()[0]
            
            open_price = float(latest.get('open', 0) or 0)
            high = float(latest.get('high', 0) or 0)
            low = float(latest.get('low', 0) or 0)
            close = float(latest.get('close', 0) or 0)
            volume = float(latest.get('volume', 0) or 0)
            
            # 检查价格合理性
            if not (0 < open_price <= 5000):
                return False, f"开盘价异常: {open_price}"
            if not (0 < high <= 5000):
                return False, f"最高价异常: {high}"
            if not (0 < low <= 5000):
                return False, f"最低价异常: {low}"
            if not (0 < close <= 5000):
                return False, f"收盘价异常: {close}"
            
            # 检查价格关系
            if high < low:
                return False, f"最高价{high} < 最低价{low}"
            if close > high or close < low:
                return False, f"收盘价{close}超出高低价范围[{low}, {high}]"
            
            # 检查成交量合理性 (单位: 股, 正常不超过50亿股)
            if volume <= 0:
                return False, f"成交量异常: {volume}"
            if volume > 5000000000:  # 50亿股
                return False, f"成交量过大: {volume}股"
            
            # 检查涨跌幅 (如果存在preclose或pct_chg)
            preclose = latest.get('preclose') or latest.get('pre_close')
            pct_chg = latest.get('pct_chg') or latest.get('pctChg')
            
            if pct_chg is not None:
                pct_chg_val = float(pct_chg)
                if abs(pct_chg_val) > 25:  # 科创板/创业板涨跌幅限制20%，留点余量
                    return False, f"涨跌幅异常: {pct_chg_val}%"
            elif preclose is not None and float(preclose) > 0:
                preclose_val = float(preclose)
                change_pct = abs((close - preclose_val) / preclose_val * 100)
                if change_pct > 25:
                    return False, f"计算涨跌幅异常: {change_pct:.2f}%"
            
            # 检查成交额合理性 (如果存在amount字段)
            if 'amount' in latest and latest['amount'] is not None:
                amount = float(latest['amount'])
                # amount单位可能是元或万元，需要判断
                if amount > 100000000000:  # 1000亿元（如果单位是元）
                    return False, f"成交额过大: {amount}"
            
            return True, "验证通过"
            
        except Exception as e:
            return False, f"验证异常: {e}"

    async def _fetch_single(self, code: str) -> str:
        """
        采集单只股票
        
        使用微服务的UnifiedFetcher，自动主备切换
        """
        try:
            # 计算日期范围
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = self._get_start_date(code)
            
            # 使用UnifiedFetcher获取K线（自动主备切换）
            df = await self.fetcher.fetch_kline(code, start_date, end_date)
            
            if df.empty:
                logger.debug(f"{code} 无数据返回")
                return 'failed'
            
            # 验证数据合理性
            is_valid, error_msg = self._validate_kline_data(df, code)
            if not is_valid:
                logger.warning(f"{code} 数据验证失败: {error_msg}")
                return 'failed'
            
            # 更新Parquet文件
            return self._update_parquet(code, df)
            
        except Exception as e:
            logger.error(f"{code} 采集异常: {e}", exc_info=True)
            return 'failed'

    def _get_start_date(self, code: str) -> str:
        """获取该股票的起始日期"""
        parquet_file = self.kline_dir / f"{code}.parquet"
        
        if parquet_file.exists() and not self.force_date:
            try:
                df = pl.read_parquet(parquet_file)
                if not df.is_empty():
                    last_date = df["trade_date"].max()
                    # 从最后日期+1天开始
                    from datetime import timedelta
                    last_dt = datetime.strptime(str(last_date), '%Y-%m-%d')
                    start_dt = last_dt + timedelta(days=1)
                    return start_dt.strftime('%Y-%m-%d')
            except:
                pass
        
        # 默认返回3年前
        from datetime import timedelta
        return (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')

    def _update_parquet(self, code: str, new_df: pl.DataFrame) -> str:
        """
        更新Parquet文件
        
        Args:
            code: 股票代码
            new_df: 新数据
        Returns:
            'success'/'updated'/'skipped'/'failed'
        """
        parquet_file = self.kline_dir / f"{code}.parquet"

        # 标准化列名 - 支持多种数据源格式
        column_mapping = {
            # 日期
            'date': 'trade_date',
            # 价格
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
            # 成交量和成交额
            'volume': 'volume',
            'amount': 'amount',
            # 昨收 (多种可能的字段名)
            'preclose': 'preclose',
            'pre_close': 'preclose',
            'preClose': 'preclose',
            # 涨跌幅 (多种可能的字段名)
            'pct_chg': 'pct_chg',
            'pctChg': 'pct_chg',
            'change_pct': 'pct_chg',
            'pct_change': 'pct_chg',
            # 换手率 (多种可能的字段名)
            'turn': 'turnover',
            'turnover': 'turnover',
            'turnover_rate': 'turnover',
        }
        
        # 重命名列
        for old_col, new_col in column_mapping.items():
            if old_col in new_df.columns and new_col not in new_df.columns:
                new_df = new_df.rename({old_col: new_col})
        
        # 确保code列存在
        if 'code' not in new_df.columns:
            new_df = new_df.with_columns(pl.lit(code).alias('code'))
        
        # 计算preclose（如果不存在但有pct_chg和close）
        if 'preclose' not in new_df.columns and 'pct_chg' in new_df.columns and 'close' in new_df.columns:
            # preclose = close / (1 + pct_chg/100)
            new_df = new_df.with_columns(
                (pl.col('close') / (1 + pl.col('pct_chg') / 100)).alias('preclose')
            )
        
        # 计算pct_chg（如果不存在但有preclose和close）
        if 'pct_chg' not in new_df.columns and 'preclose' in new_df.columns and 'close' in new_df.columns:
            # pct_chg = (close - preclose) / preclose * 100
            new_df = new_df.with_columns(
                ((pl.col('close') - pl.col('preclose')) / pl.col('preclose') * 100).alias('pct_chg')
            )

        if parquet_file.exists():
            try:
                existing = pl.read_parquet(parquet_file)
                existing_dates = set(existing["trade_date"].to_list())
                new_dates = set(new_df["trade_date"].to_list())

                if self.force_date:
                    # 强制模式：替换指定日期
                    # 首先验证新数据质量
                    is_valid, error_msg = self._validate_kline_data(new_df, code)
                    if not is_valid:
                        logger.warning(f"{code} 强制模式数据验证失败，放弃更新: {error_msg}")
                        return 'failed'
                    
                    if self.force_date in existing_dates:
                        existing_filtered = existing.filter(
                            pl.col("trade_date") != self.force_date
                        )
                        merged = pl.concat([existing_filtered, new_df], how="diagonal")
                        merged = merged.sort("trade_date")
                        merged.write_parquet(parquet_file)
                        logger.info(f"{code} 强制更新 {self.force_date} 数据成功")
                        return 'updated'
                    else:
                        merged = pl.concat([existing, new_df], how="diagonal")
                        merged = merged.sort("trade_date")
                        merged.write_parquet(parquet_file)
                        logger.info(f"{code} 新增 {self.force_date} 数据成功")
                        return 'success'
                else:
                    # 正常模式：合并新数据
                    already_has = new_dates.intersection(existing_dates)
                    
                    if len(already_has) == len(new_dates):
                        return 'skipped'

                    new_to_add = new_df.filter(
                        ~pl.col("trade_date").is_in(existing_dates)
                    )
                    
                    if len(new_to_add) > 0:
                        merged = pl.concat([existing, new_to_add], how="diagonal")
                        merged = merged.sort("trade_date")
                        merged.write_parquet(parquet_file)
                        logger.info(f"{code} 新增 {len(new_to_add)} 条数据")
                        return 'updated'
                    
                    return 'skipped'
                    
            except Exception as e:
                logger.error(f"更新 {code} Parquet失败: {e}", exc_info=True)
                logger.error(f"  - 现有数据列: {existing.columns if 'existing' in locals() else 'N/A'}")
                logger.error(f"  - 新数据列: {new_df.columns}")
                return 'failed'
        else:
            try:
                # 验证数据后再创建新文件
                is_valid, error_msg = self._validate_kline_data(new_df, code)
                if not is_valid:
                    logger.warning(f"{code} 新数据验证失败，不创建文件: {error_msg}")
                    return 'failed'
                
                new_df.write_parquet(parquet_file)
                logger.info(f"{code} 创建新文件，包含 {len(new_df)} 条数据")
                return 'success'
            except Exception as e:
                logger.error(f"创建 {code} Parquet失败: {e}", exc_info=True)
                return 'failed'

    def get_datasource_status(self) -> Dict:
        """获取数据源状态"""
        if self.ds_manager:
            return self.ds_manager.get_status()
        return {}


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据采集 V2 - 微服务版本')
    parser.add_argument('--date', type=str, default=None, help='强制重新采集指定日期 (YYYY-MM-DD)')
    parser.add_argument('--retry', action='store_true', help='断点续传模式：只采集缺失的股票')
    parser.add_argument('--status', action='store_true', help='查看数据源状态')
    args = parser.parse_args()

    collector = DataCollectorV2(force_date=args.date, retry_mode=args.retry)
    
    if args.status:
        await collector.initialize()
        status = collector.get_datasource_status()
        print("\n" + "=" * 60)
        print("数据源状态")
        print("=" * 60)
        print(f"当前主数据源: {status.get('primary', 'N/A')}")
        print("\n各数据源状态:")
        for name, info in status.get('sources', {}).items():
            status_icon = "✅" if info.get('available') else "❌"
            cb_icon = "🔴" if info.get('circuit_breaker') else "🟢"
            print(f"  {status_icon} {name:12} | "
                  f"延迟: {info.get('latency_ms', 0):.0f}ms | "
                  f"失败: {info.get('failures', 0)} | "
                  f"熔断: {cb_icon}")
        print("=" * 60)
        return

    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
