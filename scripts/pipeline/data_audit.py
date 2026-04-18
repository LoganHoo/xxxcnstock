"""数据质检任务 - 17:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import polars as pl
from pathlib import Path
from datetime import datetime

from core.data_version_manager import get_version_manager


class DataAuditor:
    """数据质检器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.kline_dir = self.project_root / "data" / "kline"
        self.version_manager = get_version_manager()
        self.logger = self._setup_logger()

    def _check_system_resources(self) -> tuple:
        """检查系统资源（磁盘空间、内存）"""
        warnings = []
        critical = []

        try:
            import shutil
            usage = shutil.disk_usage(self.project_root / "data")
            percent = usage.used / usage.total * 100
            if percent > 95:
                warnings.append(f"磁盘空间不足: 已用 {percent:.1f}%")
                self.logger.warning(f"磁盘空间警告: 已用 {percent:.1f}%")
            else:
                self.logger.info(f"磁盘空间检查通过: 已用 {percent:.1f}%")
        except Exception as e:
            self.logger.warning(f"磁盘空间检查失败: {e}")

        try:
            import psutil
            mem = psutil.virtual_memory()
            if mem.percent > 95:
                critical.append(f"内存不足: 已用 {mem.percent:.1f}%")
                self.logger.warning(f"内存警告: 已用 {mem.percent:.1f}%")
            else:
                self.logger.info(f"内存检查通过: 已用 {mem.percent:.1f}%")
        except Exception as e:
            self.logger.warning(f"内存检查失败: {e}")

        return warnings, critical

    def _setup_logger(self):
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def run(self) -> bool:
        """执行质检"""
        self.logger.info("开始数据质检...")

        issues = []

        # 0. 检查系统资源
        resource_warnings, resource_critical = self._check_system_resources()
        issues.extend(resource_critical)

        # 1. 检查K线文件数量
        kline_files = list(self.kline_dir.glob("*.parquet"))
        if len(kline_files) < 4000:
            issues.append(f"K线文件不足: {len(kline_files)}个")

        # 2. 检查最新日期数据量
        latest_issues, latest_date, stock_count = self._check_latest_data()
        issues.extend(latest_issues)

        # 3. 检查涨跌停数据
        limit_issues = self._check_limit_data()
        issues.extend(limit_issues)

        # 4. 记录结果
        if issues:
            for issue in issues:
                self.logger.warning(f"⚠️ {issue}")
            self.logger.warning(f"数据质检发现 {len(issues)} 个问题")
            # 质检未通过也锁定版本（只是数据不完整）
            if latest_date and stock_count:
                self.version_manager.lock_version(latest_date, stock_count, quality_passed=False)
            return False
        else:
            self.logger.info("✅ 数据质检通过")
            # 质检通过后锁定数据版本
            if latest_date and stock_count:
                self.version_manager.lock_version(latest_date, stock_count, quality_passed=True)
                self.logger.info("✅ 数据版本已锁定，后续任务将使用锁定数据")
            return True

    def _check_latest_data(self) -> tuple:
        """检查最新日期数据量
        
        Returns:
            tuple: (issues, latest_date, stock_count)
        """
        issues = []
        latest_date = None
        stock_count = 0

        try:
            kline_files = list(self.kline_dir.glob("*.parquet"))
            if len(kline_files) < 200:
                issues.append(f"K线文件过少: {len(kline_files)}个")
                return issues, latest_date, stock_count

            dfs = []
            for f in kline_files:
                try:
                    df = pl.read_parquet(f)
                    if len(df) > 0:
                        # 确保所有DataFrame有相同的列
                        required_cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
                        available_cols = [c for c in required_cols if c in df.columns]
                        if len(available_cols) >= 6:  # 至少包含核心字段
                            df = df.select(available_cols)
                            df = df.with_columns([
                                pl.col('volume').cast(pl.Float64),
                                pl.col('open').cast(pl.Float64),
                                pl.col('close').cast(pl.Float64),
                                pl.col('high').cast(pl.Float64),
                                pl.col('low').cast(pl.Float64),
                            ])
                            df_unique = df.unique(subset=['trade_date'])
                            dfs.append(df_unique)
                except Exception as e:
                    self.logger.debug(f"读取文件失败 {f.name}: {e}")
                    pass

            if not dfs:
                issues.append("无法读取任何K线文件")
                return issues, latest_date, stock_count

            # 使用how="diagonal"处理不同列数的DataFrame
            data = pl.concat(dfs, how="diagonal")

            latest_date = data["trade_date"].max()
            day_data = data.filter(pl.col("trade_date") == latest_date)
            stock_count = len(day_data)

            self.logger.info(f"最新日期: {latest_date}, 采样数据量: {len(day_data)}只")

            if len(day_data) < 4000:
                issues.append(f"最新日期({latest_date})数据不足: {len(day_data)}只")
        except Exception as e:
            issues.append(f"检查最新数据失败: {e}")

        return issues, latest_date, stock_count

    def _check_limit_data(self) -> list:
        """检查涨跌停数据是否合理

        涨跌停判断逻辑：
        1. 获取每个股票的前日收盘价
        2. 计算当日涨跌幅 = (收盘价 - 前日收盘价) / 前日收盘价
        3. 判断是否达到涨停板（根据股票代码判断板类型：主板10%，创业板/科创板20%，北交所30%）
        """
        issues = []

        try:
            kline_files = list(self.kline_dir.glob("*.parquet"))
            if len(kline_files) < 100:
                issues.append("K线文件不足，无法检查涨跌停")
                return issues

            all_data = []
            for f in kline_files:
                try:
                    df = pl.read_parquet(f)
                    if len(df) >= 2:
                        # 确保所有DataFrame有相同的列
                        required_cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
                        available_cols = [c for c in required_cols if c in df.columns]
                        if len(available_cols) >= 6:
                            df = df.select(available_cols)
                            df = df.with_columns([
                                pl.col('volume').cast(pl.Float64),
                                pl.col('open').cast(pl.Float64),
                                pl.col('close').cast(pl.Float64),
                                pl.col('high').cast(pl.Float64),
                                pl.col('low').cast(pl.Float64),
                            ])
                            df_unique = df.unique(subset=['trade_date'])
                            if len(df_unique) >= 2:
                                all_data.append(df_unique)
                except Exception as e:
                    self.logger.debug(f"读取文件失败 {f.name}: {e}")
                    pass

            if not all_data:
                issues.append("无法读取有效K线数据")
                return issues

            # 使用how="diagonal"处理不同列数的DataFrame
            data = pl.concat(all_data, how="diagonal")

            latest_date = data["trade_date"].max()

            def get_limit_rate(code):
                code_str = str(code)
                if 'ST' in code_str or '*ST' in code_str or 'S' in code_str:
                    return 0.05
                if code_str.startswith('300') or code_str.startswith('301') or code_str.startswith('688'):
                    return 0.20
                elif code_str.startswith('8') or code_str.startswith('4') or code_str.startswith('43'):
                    return 0.30
                else:
                    return 0.10

            prev_date_candidates = data.filter(pl.col("trade_date") < latest_date)["trade_date"].unique().sort(descending=True)
            prev_date = prev_date_candidates[0] if len(prev_date_candidates) > 0 else None

            if prev_date is None:
                issues.append("无法获取前一交易日数据")
                return issues

            audit_date = latest_date
            audit_prev_date = prev_date

            if latest_date in ['2026-04-02', '2026-04-03', '2026-04-04', '2026-04-05', '2026-04-06']:
                if prev_date is not None:
                    self.logger.info(f"假期数据检测: 使用前一交易日 {prev_date} 进行涨跌停检查")
                    audit_date = prev_date
                    prev_date_candidates2 = data.filter(pl.col("trade_date") < audit_date)["trade_date"].unique().sort(descending=True)
                    audit_prev_date = prev_date_candidates2[0] if len(prev_date_candidates2) > 0 else None

            latest_data = data.filter(pl.col("trade_date") == audit_date)
            prev_data = data.filter(pl.col("trade_date") == audit_prev_date).select(["code", "close"]).rename({"close": "prev_close"}) if audit_prev_date else None

            if prev_data is None:
                issues.append(f"无法获取 {audit_date} 的前一交易日数据")
                return issues

            merged = latest_data.join(prev_data, on="code", how="left")

            merged = merged.with_columns([
                pl.col('code').map_elements(get_limit_rate, return_dtype=pl.Float64).alias('limit_rate')
            ])

            merged = merged.with_columns([
                (pl.col('prev_close').is_not_null()).alias('has_prev_close')
            ])

            merged = merged.with_columns([
                (((pl.col('close') - pl.col('prev_close')) / pl.col('prev_close') * 100).round(2) >= pl.col('limit_rate') * 100).cast(pl.Int64).alias('is_limit_up')
            ])
            merged = merged.with_columns([
                (((pl.col('close') - pl.col('prev_close')) / pl.col('prev_close') * 100).round(2) <= -pl.col('limit_rate') * 100).cast(pl.Int64).alias('is_limit_down')
            ])

            near_limit_threshold = 0.98
            merged = merged.with_columns([
                (((pl.col('close') - pl.col('prev_close')) / pl.col('prev_close') * 100).round(2) >= pl.col('limit_rate') * 100 * near_limit_threshold).cast(pl.Int64).alias('is_near_limit_up')
            ])

            limit_up_count = int(merged["is_limit_up"].sum())
            limit_down_count = int(merged["is_limit_down"].sum())
            near_limit_up_count = int(merged["is_near_limit_up"].sum())
            valid_stocks = int(merged["has_prev_close"].sum())

            self.logger.info(f"涨跌停检查 [{audit_date}]: 有效股票{valid_stocks}只, 涨停{limit_up_count}只(含准涨停{near_limit_up_count}只), 跌停{limit_down_count}只")

            if limit_up_count == 0 and valid_stocks > 100:
                issues.append(f"涨停数据异常: 0只（市场可能未收盘或数据不完整）")

            if limit_down_count == 0 and valid_stocks > 100:
                issues.append(f"跌停数据异常: 0只（市场可能未收盘或数据不完整）")

            # 涨停数据偏少只作为警告，不作为错误
            if limit_up_count > 0 and limit_up_count < 5:
                self.logger.warning(f"⚠️ 涨停数据偏少: 仅{limit_up_count}只（可能为盘中数据）")

        except Exception as e:
            import traceback
            issues.append(f"检查涨跌停数据失败: {e}")
            self.logger.error(traceback.format_exc())

        return issues


if __name__ == "__main__":
    auditor = DataAuditor()
    result = auditor.run()

    if result:
        try:
            import json as json_module
            dq_path = Path("data/dq_close.json")
            if dq_path.exists():
                with open(dq_path, 'r', encoding='utf-8') as f:
                    dq_data = json_module.load(f)

                subject = f"【数据质检】{dq_data.get('date', datetime.now().strftime('%Y-%m-%d'))} 质检通过"
                content = f"""数据质检报告
========================================
日期: {dq_data.get('date', 'N/A')}
状态: ✅ 通过

完整率: {dq_data.get('completeness', {}).get('completeness_rate', 0) * 100:.2f}%
股票总数: {dq_data.get('completeness', {}).get('total_stocks', 0)}
有效股票: {dq_data.get('completeness', {}).get('valid_stocks', 0)}
无效股票: {dq_data.get('completeness', {}).get('invalid_stocks', 0)}

新鲜度: {dq_data.get('freshness', {}).get('latest_update', 'N/A')}
生成时间: {dq_data.get('generated_at', 'N/A')}

========================================
数据版本已锁定，可进行后续分析任务。
"""
                try:
                    from scripts.email_sender import EmailSender
                    sender = EmailSender()
                    sender.send_email(
                        to_emails=["287363@qq.com"],
                        subject=subject,
                        content=content
                    )
                    print("✅ 质检报告已发送邮件")
                except Exception as e:
                    print(f"⚠️ 质检报告邮件发送失败: {e}")
        except Exception as e:
            print(f"⚠️ 发送质检报告失败: {e}")

    sys.exit(0 if result else 1)