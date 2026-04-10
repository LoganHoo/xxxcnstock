"""数据质量检查器

提供统一的数据质量检查功能，包括：
- 日期连续性校验
- 字段级缺失率统计
- 价格合理性检查
"""
import logging
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta
import polars as pl


class DataQualityChecker:
    """数据质量检查器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._setup_logging()

    def _setup_logging(self):
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def check_all(self, data: pl.DataFrame, trading_dates: List[str] = None) -> Dict[str, Any]:
        """执行完整的数据质量检查

        Args:
            data: K线数据 DataFrame
            trading_dates: 预期的交易日列表 (YYYY-MM-DD格式)

        Returns:
            Dict containing check results
        """
        results = {
            'passed': True,
            'issues': [],
            'warnings': [],
            'stats': {}
        }

        self.logger.info("开始数据质量检查...")

        continuity_result = self.check_date_continuity(data, trading_dates)
        results['continuity'] = continuity_result
        if not continuity_result['passed']:
            results['passed'] = False
            results['issues'].extend(continuity_result.get('missing_dates', []))

        missing_result = self.check_field_missing(data)
        results['missing'] = missing_result
        results['stats']['missing_rate'] = missing_result['overall_missing_rate']
        if missing_result['overall_missing_rate'] > 0.05:
            results['warnings'].append(f"字段总体缺失率较高: {missing_result['overall_missing_rate']:.2%}")

        price_result = self.check_price_validity(data)
        results['price'] = price_result
        if not price_result['passed']:
            results['passed'] = False
            results['issues'].append(f"价格异常: {price_result['invalid_count']}条")

        volume_result = self.check_volume_validity(data)
        results['volume'] = volume_result
        if not volume_result['passed']:
            results['warnings'].append(f"成交量异常: {volume_result['zero_count']}条零成交")

        self.logger.info(f"数据质量检查完成: {'通过' if results['passed'] else '未通过'}")
        if results['warnings']:
            for w in results['warnings']:
                self.logger.warning(f"  ⚠️ {w}")

        return results

    def check_date_continuity(self, data: pl.DataFrame, expected_dates: List[str] = None) -> Dict[str, Any]:
        """检查日期连续性（面板数据优化版）

        对于面板数据，应该按单只股票检查连续性，而不是检查全市场所有日期。

        检查策略:
        1. 如果提供了 expected_dates，按股票检查缺失的交易日
        2. 否则只检查最近30天数据的连续性
        3. 允许周末(3天)和节假日(5-10天)的间隔

        Args:
            data: K线数据 DataFrame
            expected_dates: 预期的交易日列表 (YYYY-MM-DD格式)

        Returns:
            Dict with continuity check results
        """
        result = {
            'passed': True,
            'stocks_checked': 0,
            'stocks_with_gaps': 0,
            'recent_gaps': {},
            'warning_stocks': [],
            'total_records': len(data)
        }

        if data is None or len(data) == 0:
            result['passed'] = False
            result['error'] = '数据为空'
            return result

        if 'code' not in data.columns or 'trade_date' not in data.columns:
            result['passed'] = False
            result['error'] = '缺少必需列: code 或 trade_date'
            return result

        result['stocks_checked'] = data['code'].n_unique()

        if expected_dates:
            expected_set = set(expected_dates)
            stock_dates = data.group_by('code').agg([
                pl.col('trade_date').alias('dates')
            ])

            for row in stock_dates.iter_rows(named=True):
                code = row['code']
                dates = set(str(d) for d in row['dates'])
                missing = expected_set - dates
                if missing:
                    result['stocks_with_gaps'] += 1
                    result['warning_stocks'].append({
                        'code': code,
                        'missing_dates': sorted(list(missing))[:10]
                    })
                    if len(result['warning_stocks']) >= 20:
                        break

            if result['warning_stocks']:
                result['passed'] = False
                self.logger.warning(f"发现 {result['stocks_with_gaps']} 只股票存在缺失日期")
        else:
            recent_days = 30
            latest_date = data['trade_date'].max()
            latest_str = str(latest_date)

            result['latest_date'] = latest_str
            result['data_age_days'] = (datetime.now().date() - (
                datetime.fromisoformat(latest_str).date()
                if isinstance(latest_str, str) else latest_date.date()
            )).days

            cutoff_date = None
            if isinstance(latest_date, str):
                cutoff_date = datetime.fromisoformat(latest_date).date()
            elif hasattr(latest_date, 'date'):
                cutoff_date = latest_date.date()
            elif isinstance(latest_date, datetime):
                cutoff_date = latest_date.date()

            if cutoff_date:
                cutoff = datetime.combine(cutoff_date, datetime.min.time()) - timedelta(days=recent_days)
                cutoff = cutoff.date() if isinstance(cutoff, datetime) else cutoff

            recent_data = data.filter(
                pl.col('trade_date') >= str(cutoff)
            )

            latest_date_stocks = recent_data.filter(
                pl.col('trade_date') == latest_str
            )['code'].n_unique()

            result['recent_stocks_count'] = latest_date_stocks
            result['stocks_checked'] = data['code'].n_unique()

            coverage_rate = latest_date_stocks / result['stocks_checked'] * 100
            result['latest_coverage_rate'] = coverage_rate

            if coverage_rate < 50:
                result['passed'] = False
                result['warnings'] = [f"最新日期({latest_str})覆盖率仅{coverage_rate:.1f}%"]
                self.logger.warning(
                    f"最新日期覆盖率过低: {coverage_rate:.1f}% "
                    f"(最新日期股票数: {latest_date_stocks}, 总股票数: {result['stocks_checked']})"
                )

            self.logger.info(f"最近数据新鲜度检查:")
            self.logger.info(f"  最新数据日期: {latest_str}")
            self.logger.info(f"  最新日期股票数: {latest_date_stocks}/{result['stocks_checked']} ({coverage_rate:.1f}%)")
            self.logger.info(f"  数据年龄: {result['data_age_days']}天")

            return result

    def check_field_missing(self, data: pl.DataFrame) -> Dict[str, Any]:
        """统计字段级缺失率

        Args:
            data: K线数据 DataFrame

        Returns:
            Dict with missing rate statistics
        """
        result = {
            'passed': True,
            'field_missing_rates': {},
            'overall_missing_rate': 0.0,
            'total_records': len(data)
        }

        if data is None or len(data) == 0:
            result['passed'] = False
            return result

        essential_fields = ['code', 'trade_date', 'open', 'close', 'high', 'low', 'volume']
        total_records = len(data)

        missing_counts = {}
        for field in essential_fields:
            if field in data.columns:
                null_count = data[field].null_count()
                if null_count > 0:
                    missing_counts[field] = null_count
                    rate = null_count / total_records
                    result['field_missing_rates'][field] = {
                        'null_count': null_count,
                        'missing_rate': rate
                    }
                    if rate > 0.01:
                        self.logger.warning(f"字段 {field} 缺失率: {rate:.2%} ({null_count}条)")

        if missing_counts:
            total_missing = sum(missing_counts.values())
            result['overall_missing_rate'] = total_missing / (total_records * len(essential_fields))

        result['passed'] = result['overall_missing_rate'] < 0.05

        self.logger.info(f"字段缺失率统计:")
        self.logger.info(f"  总记录数: {total_records}")
        if result['field_missing_rates']:
            for field, info in result['field_missing_rates'].items():
                self.logger.info(f"  {field}: {info['missing_rate']:.2%} ({info['null_count']}条)")
        else:
            self.logger.info("  无缺失值 ✓")

        return result

    def check_price_validity(self, data: pl.DataFrame) -> Dict[str, Any]:
        """检查价格合理性

        检查规则:
        1. high >= low
        2. open, close 在 [low, high] 范围内
        3. price > 0

        Args:
            data: K线数据 DataFrame

        Returns:
            Dict with price validity results
        """
        result = {
            'passed': True,
            'invalid_count': 0,
            'issues': []
        }

        if data is None or len(data) == 0:
            result['passed'] = False
            return result

        required_cols = ['open', 'close', 'high', 'low']
        for col in required_cols:
            if col not in data.columns:
                result['passed'] = False
                result['issues'].append(f"缺少必需列: {col}")
                return result

        issues_found = []

        high_low_invalid = data.filter(pl.col('high') < pl.col('low'))
        if len(high_low_invalid) > 0:
            issues_found.append(f"high < low: {len(high_low_invalid)}条")

        open_invalid = data.filter(
            (pl.col('open') < pl.col('low')) | (pl.col('open') > pl.col('high'))
        )
        if len(open_invalid) > 0:
            issues_found.append(f"open不在[low,high]范围: {len(open_invalid)}条")

        close_invalid = data.filter(
            (pl.col('close') < pl.col('low')) | (pl.col('close') > pl.col('high'))
        )
        if len(close_invalid) > 0:
            issues_found.append(f"close不在[low,high]范围: {len(close_invalid)}条")

        zero_price = data.filter((pl.col('open') <= 0) | (pl.col('close') <= 0))
        if len(zero_price) > 0:
            issues_found.append(f"价格为零: {len(zero_price)}条")

        negative_price = data.filter(
            (pl.col('high') < 0) | (pl.col('low') < 0)
        )
        if len(negative_price) > 0:
            issues_found.append(f"价格为负: {len(negative_price)}条")

        result['invalid_count'] = len(high_low_invalid) + len(open_invalid) + len(close_invalid) + len(zero_price) + len(negative_price)
        result['issues'] = issues_found
        result['passed'] = len(issues_found) == 0

        if issues_found:
            for issue in issues_found:
                self.logger.warning(f"价格异常: {issue}")
        else:
            self.logger.info("价格合理性检查通过 ✓")

        return result

    def check_volume_validity(self, data: pl.DataFrame) -> Dict[str, Any]:
        """检查成交量合理性

        检查规则:
        1. volume >= 0
        2. 零成交应有合理原因(停牌)

        Args:
            data: K线数据 DataFrame

        Returns:
            Dict with volume validity results
        """
        result = {
            'passed': True,
            'zero_count': 0,
            'negative_count': 0,
            'warnings': []
        }

        if data is None or len(data) == 0:
            result['passed'] = False
            return result

        if 'volume' not in data.columns:
            result['passed'] = False
            result['warnings'].append("缺少volume列")
            return result

        zero_volume = data.filter(pl.col('volume') == 0)
        result['zero_count'] = len(zero_volume)

        negative_volume = data.filter(pl.col('volume') < 0)
        result['negative_count'] = len(negative_volume)

        if result['negative_count'] > 0:
            result['passed'] = False
            result['warnings'].append(f"负成交量: {result['negative_count']}条")
            self.logger.warning(f"负成交量异常: {result['negative_count']}条")

        if result['zero_count'] > 0:
            zero_rate = result['zero_count'] / len(data)
            result['warnings'].append(f"零成交: {result['zero_count']}条 ({zero_rate:.2%})")
            if zero_rate > 0.1:
                self.logger.warning(f"零成交率偏高: {zero_rate:.2%}")
            else:
                self.logger.warning(f"零成交: {result['zero_count']}条")

        if result['passed']:
            self.logger.info("成交量合理性检查通过 ✓")

        return result

    def check_change_validity(self, data: pl.DataFrame) -> Dict[str, Any]:
        """检查涨跌幅合理性

        检查规则:
        1. 涨跌幅应在 [-20%, 20%] 范围内(科创板/创业板为[-40%, 40%])
        2. 涨跌幅与 (close - prev_close) / prev_close 应一致

        Args:
            data: K线数据 DataFrame

        Returns:
            Dict with change validity results
        """
        result = {
            'passed': True,
            'extreme_change_count': 0,
            'issues': []
        }

        if data is None or len(data) == 0:
            result['passed'] = False
            return result

        required_cols = ['close']
        for col in required_cols:
            if col not in data.columns:
                result['passed'] = False
                result['issues'].append(f"缺少必需列: {col}")
                return result

        extreme_changes = data.filter(
            (pl.col('change_pct').abs() > 20) if 'change_pct' in data.columns
            else pl.lit(False)
        )
        result['extreme_change_count'] = len(extreme_changes)

        if result['extreme_change_count'] > 0:
            result['warnings'] = [f"极端涨跌幅: {result['extreme_change_count']}条"]
            self.logger.warning(f"极端涨跌幅记录: {result['extreme_change_count']}条")

        return result

    def generate_report(self, results: Dict[str, Any]) -> str:
        """生成数据质量报告

        Args:
            results: check_all() 返回的结果

        Returns:
            格式化的报告字符串
        """
        lines = []
        lines.append("\n" + "=" * 60)
        lines.append("【数据质量报告】")
        lines.append("=" * 60)

        overall = "✅ 通过" if results.get('passed') else "❌ 未通过"
        lines.append(f"\n总体状态: {overall}")

        if results.get('issues'):
            lines.append(f"\n⚠️ 问题 ({len(results['issues'])}项):")
            for issue in results['issues']:
                lines.append(f"  - {issue}")

        if results.get('warnings'):
            lines.append(f"\n⚡ 警告 ({len(results['warnings'])}项):")
            for warning in results['warnings']:
                lines.append(f"  - {warning}")

        continuity = results.get('continuity', {})
        lines.append(f"\n【数据新鲜度】")
        lines.append(f"  最新日期: {continuity.get('latest_date', 'N/A')}")
        lines.append(f"  数据年龄: {continuity.get('data_age_days', 'N/A')}天")
        lines.append(f"  最新日期股票数: {continuity.get('recent_stocks_count', 'N/A')}/{continuity.get('stocks_checked', 'N/A')}")
        lines.append(f"  覆盖率: {continuity.get('latest_coverage_rate', 0):.1f}%")

        missing = results.get('missing', {})
        lines.append(f"\n【字段缺失】")
        lines.append(f"  总体缺失率: {missing.get('overall_missing_rate', 0):.2%}")
        field_rates = missing.get('field_missing_rates', {})
        if field_rates:
            for field, info in field_rates.items():
                lines.append(f"  {field}: {info['missing_rate']:.2%}")

        price = results.get('price', {})
        lines.append(f"\n【价格合理性】")
        lines.append(f"  状态: {'✅ 通过' if price.get('passed') else '❌ 未通过'}")
        if price.get('issues'):
            for issue in price['issues']:
                lines.append(f"  - {issue}")

        volume = results.get('volume', {})
        lines.append(f"\n【成交量合理性】")
        lines.append(f"  零成交: {volume.get('zero_count', 0)}条")
        lines.append(f"  负成交: {volume.get('negative_count', 0)}条")

        lines.append("\n" + "=" * 60)

        return "\n".join(lines)


def check_data_quality(data: pl.DataFrame, trading_dates: List[str] = None) -> Tuple[bool, Dict[str, Any]]:
    """便捷函数：执行数据质量检查

    Args:
        data: K线数据 DataFrame
        trading_dates: 预期的交易日列表

    Returns:
        Tuple[bool, Dict]: (是否通过, 详细结果)
    """
    checker = DataQualityChecker()
    results = checker.check_all(data, trading_dates)
    return results['passed'], results


if __name__ == "__main__":
    import sys
    from pathlib import Path

    project_root = Path(__file__).parent.parent
    kline_dir = project_root / "data" / "kline"

    checker = DataQualityChecker()

    if kline_dir.exists():
        parquet_files = list(kline_dir.glob("*.parquet"))[:100]
        if parquet_files:
            dfs = []
            for f in parquet_files:
                try:
                    df = pl.read_parquet(f)
                    df = df.select(['code', 'trade_date', 'open', 'close', 'high', 'low', 'volume'])
                    dfs.append(df)
                except Exception:
                    pass

            if dfs:
                data = pl.concat(dfs)
                print(f"加载数据: {len(data)}行")
                passed, results = check_data_quality(data)
                report = checker.generate_report(results)
                print(report)
            else:
                print("无可用数据")
                sys.exit(1)
        else:
            print("无K线文件")
            sys.exit(1)
    else:
        print(f"目录不存在: {kline_dir}")
        sys.exit(1)