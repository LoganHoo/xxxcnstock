#!/usr/bin/env python3
"""
复盘报告推送 - 使用 BaseReporter 重构
【15:30执行】
"""
import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.base_reporter import BaseReporter
from core.paths import ReportPaths
from core.data_quality_metrics import DataQualityMetricsCalculator
from services.notify_service.templates import get_template
from services.report_db_service import ReportDBService


class ReviewReportGenerator(BaseReporter):
    """复盘报告推送器"""

    @property
    def report_type(self) -> str:
        return "review_report"

    @property
    def required_data_sources(self) -> List[str]:
        return []

    @property
    def optional_data_sources(self) -> List[str]:
        return ['dq_report', 'market_review', 'picks_review', 'quality_metrics']

    def generate_and_save_market_review(self) -> dict:
        """
        生成并保存市场复盘数据到 market_review.json
        从增强分析数据中提取市场信息
        """
        try:
            self.logger.info("生成市场复盘数据...")
            import json
            import polars as pl
            from pathlib import Path

            project_root = Path(__file__).parent.parent
            kline_dir = project_root / "data" / "kline"

            # 读取所有K线数据
            all_data = []
            for f in kline_dir.glob("*.parquet"):
                try:
                    df = pl.read_parquet(f)
                    if len(df) > 0:
                        # 统一列类型
                        df = df.with_columns([
                            pl.col('code').cast(pl.Utf8),
                            pl.col('trade_date').cast(pl.Utf8),
                            pl.col('open').cast(pl.Float64),
                            pl.col('close').cast(pl.Float64),
                            pl.col('high').cast(pl.Float64),
                            pl.col('low').cast(pl.Float64),
                            pl.col('volume').cast(pl.Float64),
                        ])
                        all_data.append(df)
                except Exception as e:
                    pass

            if not all_data:
                self.logger.warning("没有K线数据，无法生成市场复盘")
                return None

            data = pl.concat(all_data)
            latest_date = data["trade_date"].max()
            today = datetime.now().strftime('%Y-%m-%d')

            # 获取最新日期的数据
            latest_data = data.filter(pl.col("trade_date") == latest_date)

            # 计算涨跌统计
            prev_date_candidates = data.filter(pl.col("trade_date") < latest_date)["trade_date"].unique().sort(descending=True)
            prev_date = prev_date_candidates[0] if len(prev_date_candidates) > 0 else None

            if prev_date is None:
                self.logger.warning("无法获取前一交易日数据")
                return None

            prev_data = data.filter(pl.col("trade_date") == prev_date).select(["code", "close"]).rename({"close": "prev_close"})
            merged = latest_data.join(prev_data, on="code", how="left")

            # 计算涨跌幅
            merged = merged.with_columns([
                ((pl.col("close") - pl.col("prev_close")) / pl.col("prev_close") * 100).alias("change_pct")
            ])

            # 统计涨跌
            rising = int((merged["change_pct"] > 0).sum())
            falling = int((merged["change_pct"] < 0).sum())
            flat = int((merged["change_pct"] == 0).sum())

            # 统计涨跌停
            def get_limit_rate(code):
                code_str = str(code)
                if code_str.startswith('300') or code_str.startswith('301') or code_str.startswith('688'):
                    return 20.0
                elif code_str.startswith('8') or code_str.startswith('4') or code_str.startswith('43'):
                    return 30.0
                else:
                    return 10.0

            merged = merged.with_columns([
                pl.col("code").map_elements(get_limit_rate, return_dtype=pl.Float64).alias("limit_rate")
            ])

            limit_up = int((merged["change_pct"] >= merged["limit_rate"] - 0.5).sum())
            limit_down = int((merged["change_pct"] <= -merged["limit_rate"] + 0.5).sum())

            # 计算成交额
            total_volume = float(merged["volume"].sum() / 100000000)  # 转换为亿

            # 判断市场状态
            if rising > falling * 1.5:
                market_status = 'strong'
            elif falling > rising * 1.5:
                market_status = 'weak'
            else:
                market_status = 'oscillating'

            # 生成市场复盘数据
            market_review = {
                'date': today,
                'summary': {
                    'rising_count': rising,
                    'falling_count': falling,
                    'flat_count': flat,
                    'limit_up_count': limit_up,
                    'limit_down_count': limit_down,
                    'total_volume': round(total_volume, 2)
                },
                'market_status': market_status,
                'cvd': {
                    'signal': 'neutral',
                    'cvd_cumsum': 0,
                    'cvd_trend': 'unknown'
                },
                'key_levels': {
                    'index_close': round(float(merged.filter(pl.col("code") == "000001")["close"].mean()), 2) if len(merged.filter(pl.col("code") == "000001")) > 0 else 'N/A'
                },
                'top_sectors': []
            }

            # 保存到文件
            market_review_path = ReportPaths.market_review()
            with open(market_review_path, 'w', encoding='utf-8') as f:
                json.dump(market_review, f, ensure_ascii=False, indent=2)

            self.logger.info(f"市场复盘数据已生成: {market_review_path}")
            return market_review

        except Exception as e:
            self.logger.error(f"生成市场复盘数据失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None

    def load_data(self) -> Dict[str, Any]:
        """加载复盘报告所需数据"""
        # 加载数据质量报告
        dq_report = self._load_dq_report()

        # 加载市场复盘数据，如果不存在则生成
        market_review = self._load_market_review()
        if not market_review:
            self.logger.info("市场复盘数据不存在，尝试生成...")
            market_review = self.generate_and_save_market_review()

        # 加载选股复盘数据
        picks_review = self._load_yesterday_picks()

        return {
            'dq_report': dq_report,
            'market_review': market_review,
            'picks_review': picks_review
        }

    # 保持向后兼容的方法名
    def load_dq_report(self) -> dict:
        return self._load_dq_report()

    def _load_dq_report(self) -> dict:
        """加载数据质量报告"""
        dq_path = ReportPaths.dq_close()
        if dq_path and dq_path.exists():
            try:
                with open(dq_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载数据质量报告失败: {e}")
        return None

    def load_market_review(self) -> dict:
        return self._load_market_review()

    def _load_market_review(self) -> dict:
        """加载市场复盘数据"""
        review_path = ReportPaths.market_review()
        if review_path.exists():
            try:
                with open(review_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载市场复盘数据失败: {e}")
        return None

    def load_yesterday_picks(self) -> dict:
        return self._load_yesterday_picks()

    def _load_yesterday_picks(self) -> dict:
        """加载昨日选股数据"""
        picks_path = ReportPaths.daily_picks()
        if picks_path.exists():
            try:
                with open(picks_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载选股数据失败: {e}")
        return None

    def _load_quality_metrics(self) -> dict:
        """加载数据质量标准化指标"""
        try:
            calculator = DataQualityMetricsCalculator(project_root)
            metrics = calculator.calculate_metrics()
            # 保存指标
            calculator.save_metrics(metrics)
            return metrics.__dict__
        except Exception as e:
            self.logger.warning(f"计算数据质量指标失败: {e}")
            return None

    def _load_drawdown_analysis(self) -> dict:
        """加载回撤分析数据"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            drawdown_file = project_root / "data" / "audit" / f"{today}_drawdown_analysis.json"
            if drawdown_file.exists():
                with open(drawdown_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
        except Exception as e:
            self.logger.warning(f"加载回撤分析数据失败: {e}")
            return None

    def _load_audit_result_for_report(self) -> dict:
        """加载审计结果用于报告生成"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            audit_file = project_root / "data" / "audit" / f"{today}_audit_result.json"
            if audit_file.exists():
                with open(audit_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
        except Exception as e:
            self.logger.warning(f"加载审计结果失败: {e}")
            return None

    def _generate_quality_metrics_section(self, quality_metrics: dict) -> str:
        """生成数据质量指标章节"""
        lines = []
        lines.append("\n一、数据质量标准化指标")
        lines.append("-" * 50)

        if not quality_metrics:
            lines.append("  ⚠️ 数据质量指标暂不可用")
            return "\n".join(lines)

        # 综合评分
        overall_score = quality_metrics.get('overall_score', 0)
        quality_level = quality_metrics.get('quality_level', 'unknown')

        level_map = {
            'excellent': '优秀 ⭐⭐⭐⭐⭐',
            'good': '良好 ⭐⭐⭐⭐',
            'fair': '一般 ⭐⭐⭐',
            'poor': '较差 ⭐⭐',
            'unknown': '未知'
        }

        lines.append(f"  【综合评分】{overall_score:.1f}/100 - {level_map.get(quality_level, quality_level)}")
        lines.append("")

        # 详细指标
        lines.append("  【详细指标】")
        lines.append(f"    ● 采集率: {quality_metrics.get('collection_rate', 0):.1f}%")
        lines.append(f"    ● 完整性: {quality_metrics.get('completeness_rate', 0):.1f}%")
        lines.append(f"    ● 新鲜度: {quality_metrics.get('freshness_score', 0):.1f}/100")
        lines.append(f"    ● 一致性: {quality_metrics.get('consistency_score', 0):.1f}/100")
        lines.append("")

        # 数据统计
        lines.append("  【数据统计】")
        lines.append(f"    ● 应采集股票: {quality_metrics.get('total_stocks', 0)}只")
        lines.append(f"    ● 实际采集: {quality_metrics.get('collected_stocks', 0)}只")
        lines.append(f"    ● 有效数据: {quality_metrics.get('valid_stocks', 0)}只")
        lines.append(f"    ● 无效数据: {quality_metrics.get('invalid_stocks', 0)}只")

        # 质量提示
        if overall_score >= 95:
            lines.append("  ✅ 数据质量优秀，分析结果可信度高")
        elif overall_score >= 85:
            lines.append("  ✅ 数据质量良好，分析结果可靠")
        elif overall_score >= 70:
            lines.append("  ⚠️ 数据质量一般，分析结果仅供参考")
        else:
            lines.append("  ❌ 数据质量较差，建议检查数据采集流程")

        return "\n".join(lines)

    def _generate_market_review_from_enhanced(self) -> dict:
        """从增强分析数据生成市场复盘"""
        enhanced_path = ReportPaths.enhanced_analysis()
        if enhanced_path.exists():
            try:
                with open(enhanced_path, 'r', encoding='utf-8') as f:
                    enhanced = json.load(f)

                market_review = {
                    'summary': {},
                    'cvd': {},
                    'key_levels': {},
                    'top_sectors': []
                }

                if 'market_overview' in enhanced:
                    overview = enhanced['market_overview']
                    market_review['summary'] = {
                        'rising_count': overview.get('rising_count', 0),
                        'falling_count': overview.get('falling_count', 0),
                        'limit_up_count': overview.get('limit_up_count', 0),
                        'limit_down_count': overview.get('limit_down_count', 0),
                        'total_volume': overview.get('total_volume', 0) / 10000,
                        'market_status': overview.get('market_status', 'unknown')
                    }

                if 'market_data' in enhanced:
                    market_data = enhanced['market_data']
                    if 'indices' in market_data:
                        for idx in market_data['indices']:
                            if idx.get('name') == '上证指数':
                                market_review['key_levels'] = {
                                    'index_close': idx.get('close', 'N/A'),
                                    'high_60': idx.get('high_60', 'N/A'),
                                    'low_60': idx.get('low_60', 'N/A'),
                                    'ma5': idx.get('ma5', 'N/A'),
                                    'ma20': idx.get('ma20', 'N/A')
                                }
                                break

                if 'cvd_analysis' in enhanced:
                    cvd = enhanced['cvd_analysis']
                    market_review['cvd'] = {
                        'signal': cvd.get('signal', 'neutral'),
                        'cvd_cumsum': cvd.get('cvd_cumsum', 0),
                        'cvd_trend': cvd.get('cvd_trend', 'N/A')
                    }

                if 'sector_analysis' in enhanced:
                    sectors = enhanced['sector_analysis']
                    if isinstance(sectors, list):
                        market_review['top_sectors'] = sectors[:5]

                return market_review
            except Exception as e:
                self.logger.warning(f"从增强数据生成市场复盘失败: {e}")
        return None

    def _generate_drawdown_section(self, drawdown_analysis: dict) -> str:
        """生成回撤分析章节"""
        lines = []
        lines.append("\n七、回撤之最分析（避免幸存者偏差）")
        lines.append("-" * 50)

        if not drawdown_analysis:
            lines.append("  ⚠️ 回撤分析数据暂不可用")
            return "\n".join(lines)

        summary = drawdown_analysis.get('summary', {})
        lines.append(f"  【整体表现】")
        lines.append(f"    ● 选股总数: {summary.get('total_picks', 0)}只")
        lines.append(f"    ● 平均收益: {summary.get('avg_return', 0):.2f}%")
        lines.append(f"    ● 胜率: {summary.get('win_rate', 0):.1f}%")
        lines.append(f"    ● 最大收益: +{summary.get('max_return', 0):.2f}%")
        lines.append(f"    ● 最大回撤: {summary.get('min_return', 0):.2f}%")
        lines.append("")

        # 回撤股票
        drawdowns = drawdown_analysis.get('drawdowns', [])
        if drawdowns:
            lines.append(f"  【回撤最大股票】")
            for i, stock in enumerate(drawdowns[:3], 1):
                code = stock.get('code', 'N/A')
                name = stock.get('name', 'N/A')
                return_pct = stock.get('return_pct', 0)
                reason = stock.get('reason', '未知')
                lines.append(f"    {i}. {code} {name}: {return_pct:.2f}%")
                lines.append(f"       原因: {reason}")
            lines.append("")

        # 风险分析
        risk = drawdown_analysis.get('risk_analysis', {})
        lines.append(f"  【风险归因】")
        lines.append(f"    ● 平均回撤: {risk.get('average_drawdown', 0):.2f}%")
        lines.append(f"    ● 系统性风险占比: {risk.get('systematic_ratio', 0):.0%}")
        lines.append(f"    ● 个股特有风险占比: {risk.get('idiosyncratic_ratio', 0):.0%}")
        lines.append(f"    ● 结论: {risk.get('conclusion', '未知')}")
        lines.append("")

        # 因子分析
        factor = drawdown_analysis.get('factor_analysis', {})
        if factor:
            lines.append(f"  【因子失效分析】")
            failing = factor.get('failing_factors', [])
            effective = factor.get('effective_factors', [])
            if failing:
                lines.append(f"    ● 失效因子: {', '.join(failing)}")
            if effective:
                lines.append(f"    ● 有效因子: {', '.join(effective)}")
            recommendation = factor.get('recommendation', '')
            if recommendation:
                lines.append(f"    ● 建议: {recommendation}")

        return "\n".join(lines)

    def _generate_picks_review_section(self, picks_review: dict) -> str:
        """生成选股复盘章节"""
        lines = []
        lines.append("\n八、昨日选股回顾")
        lines.append("-" * 50)

        if not picks_review:
            lines.append("  ⚠️ 昨日选股数据暂不可用")
            return "\n".join(lines)

        filters = picks_review.get('filters', {})
        s_grade = filters.get('s_grade', {})
        a_grade = filters.get('a_grade', {})

        s_stocks = s_grade.get('stocks', [])
        a_stocks = a_grade.get('stocks', [])

        if s_stocks:
            lines.append("  【S级股票】")
            for i, stock in enumerate(s_stocks[:3], 1):
                code = stock.get('code', 'N/A')
                name = stock.get('name', 'N/A')
                reasons = stock.get('reasons', '')
                score = stock.get('score', 0)

                # 获取价格信息（适配两种数据结构）
                prev_close = stock.get('prev_close', 0)
                # 优先使用 curr_close，否则使用 price
                curr_close = stock.get('curr_close', 0) or stock.get('price', 0)
                change_pct = stock.get('change_pct', 0)

                lines.append(f"  {i}. {code} {name}")
                if reasons:
                    lines.append(f"     理由: {reasons}")

                # 显示价格对比
                if prev_close > 0 and curr_close > 0:
                    change_sign = '+' if change_pct >= 0 else ''
                    lines.append(f"     昨日收盘: ¥{prev_close:.2f} → 今日收盘: ¥{curr_close:.2f} ({change_sign}{change_pct:.2f}%)")
                else:
                    lines.append(f"     评分: {score}")

        if a_stocks:
            lines.append("  【A级股票】")
            for i, stock in enumerate(a_stocks[:3], 1):
                code = stock.get('code', 'N/A')
                name = stock.get('name', 'N/A')
                score = stock.get('score', 0)

                # 获取价格信息（适配两种数据结构）
                prev_close = stock.get('prev_close', 0)
                # 优先使用 curr_close，否则使用 price
                curr_close = stock.get('curr_close', 0) or stock.get('price', 0)
                change_pct = stock.get('change_pct', 0)

                lines.append(f"  {i}. {code} {name}")

                # 显示价格对比
                if prev_close > 0 and curr_close > 0:
                    change_sign = '+' if change_pct >= 0 else ''
                    lines.append(f"     昨日收盘: ¥{prev_close:.2f} → 今日收盘: ¥{curr_close:.2f} ({change_sign}{change_pct:.2f}%)")
                else:
                    lines.append(f"     评分: {score}")

        if not s_stocks and not a_stocks:
            lines.append("  ⚠️ 昨日无选股推荐")

        return "\n".join(lines)

    def generate(self, data: Dict[str, Any]) -> str:
        """生成复盘报告"""
        dq_report = data.get('dq_report')
        market_review = data.get('market_review')
        picks_review = data.get('picks_review')
        quality_metrics = data.get('quality_metrics')

        # 如果 market_review 缺失，尝试从增强数据生成
        if not market_review:
            market_review = self._generate_market_review_from_enhanced()

        # 如果 quality_metrics 缺失，尝试计算
        if not quality_metrics:
            quality_metrics = self._load_quality_metrics()

        # 生成文本报告
        lines = []
        lines.append("=" * 70)
        lines.append("【复盘快报】A股市场今日总结")
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("=" * 70)

        # 数据质量标准化指标（新的标准化指标）
        quality_section = self._generate_quality_metrics_section(quality_metrics)
        lines.append(quality_section)

        # 数据质量详细报告（使用audit_result数据，与第一章保持一致）
        lines.append("\n二、数据质量详细报告")
        lines.append("-" * 50)

        # 优先使用audit_result数据（与第一章一致）
        audit_result = self._load_audit_result_for_report()
        if audit_result:
            checks = audit_result.get('checks', {})
            completeness = checks.get('completeness', {})
            freshness = checks.get('freshness', {})

            total = audit_result.get('total_stocks', 0)
            collected = audit_result.get('collected_stocks', 0)
            valid = completeness.get('valid', 0)
            invalid = completeness.get('invalid', 0)

            collection_rate = (collected / total * 100) if total > 0 else 0
            completeness_rate = (valid / total * 100) if total > 0 else 0

            lines.append(f"  ● 采集完整度: {collection_rate:.1f}% ({collected}/{total}只)")
            lines.append(f"  ● 有效数据: {valid}只")
            lines.append(f"  ● 无效数据: {invalid}只")

            latest_date = freshness.get('latest_date', '未知')
            lines.append(f"  ● 最新更新: {latest_date}")

            if completeness_rate < 95:
                lines.append("  ⚠️ 数据完整度偏低，分析结果仅供参考")
            else:
                lines.append("  ✅ 数据质量良好")
        elif dq_report:
            # 兼容旧版数据
            completeness = dq_report.get('completeness', {})
            total = completeness.get('total', 0)
            valid = completeness.get('valid', 0)
            invalid = completeness.get('invalid', 0)
            completeness_rate = completeness.get('completeness_rate', 0)

            lines.append(f"  ● 采集完整度: {completeness_rate:.1f}% ({valid}/{total}只)")
            lines.append(f"  ● 有效数据: {valid}只")
            lines.append(f"  ● 无效数据: {invalid}只")

            latest = dq_report.get('latest_date', '未知')
            lines.append(f"  ● 最新更新: {latest}")

            if completeness_rate < 95:
                lines.append("  ⚠️ 数据完整度偏低，分析结果仅供参考")
        else:
            lines.append("  ⚠️ 数据质量报告暂不可用")

        # 市场概况部分
        lines.append("\n三、今日市场概况")
        lines.append("-" * 50)
        if market_review:
            summary = market_review.get('summary', {})
            lines.append(f"  ● 上涨股票: {summary.get('rising_count', 'N/A')}只")
            lines.append(f"  ● 下跌股票: {summary.get('falling_count', 'N/A')}只")
            lines.append(f"  ● 涨停股票: {summary.get('limit_up_count', 'N/A')}只")
            lines.append(f"  ● 跌停股票: {summary.get('limit_down_count', 'N/A')}只")
            lines.append(f"  ● 成交额: {summary.get('total_volume', 'N/A')}亿")

            market_status = market_review.get('market_status', 'unknown')
            status_map = {
                'strong': '强势上涨',
                'weak': '弱势下跌',
                'oscillating': '震荡整理',
                'unknown': '状态未知'
            }
            lines.append(f"  ● 市场状态: {status_map.get(market_status, market_status)}")
        else:
            lines.append("  ⚠️ 复盘数据暂不可用")

        # 资金流向部分
        lines.append("\n四、资金流向")
        lines.append("-" * 50)
        if market_review:
            cvd_data = market_review.get('cvd', {})
            cvd_signal = cvd_data.get('signal', 'neutral')
            signal_map = {
                'buy_dominant': '主力净流入（买方占优）',
                'sell_dominant': '主力净流出（卖方占优）',
                'neutral': '多空平衡'
            }
            lines.append(f"  ● CVD信号: {signal_map.get(cvd_signal, cvd_signal)}")
            lines.append(f"  ● CVD累计: {cvd_data.get('cvd_cumsum', 'N/A')}")
            lines.append(f"  ● CVD趋势: {cvd_data.get('cvd_trend', 'N/A')}")
        else:
            lines.append("  ⚠️ 资金流向数据暂不可用")

        # 关键位分析
        lines.append("\n五、关键位分析")
        lines.append("-" * 50)
        if market_review:
            levels = market_review.get('key_levels', {})
            lines.append(f"  ● 上证指数: {levels.get('index_close', 'N/A')}")
            lines.append(f"  ● 60日高点: {levels.get('high_60', 'N/A')}")
            lines.append(f"  ● 60日低点: {levels.get('low_60', 'N/A')}")
            lines.append(f"  ● MA5: {levels.get('ma5', 'N/A')}")
            lines.append(f"  ● MA20: {levels.get('ma20', 'N/A')}")
        else:
            lines.append("  ⚠️ 关键位数据暂不可用")

        # 热点板块部分
        lines.append("\n六、热点板块")
        lines.append("-" * 50)
        if market_review:
            top_sectors = market_review.get('top_sectors', [])
            if top_sectors:
                for i, sector in enumerate(top_sectors[:5], 1):
                    name = sector.get('name', 'N/A')
                    change = sector.get('change', 0)
                    change_sign = '+' if change > 0 else ''
                    lines.append(f"  {i}. {name}: {change_sign}{change:.2f}%")
            else:
                lines.append("  暂无板块数据")
        else:
            lines.append("  ⚠️ 板块数据暂不可用")

        # 加载回撤分析数据
        drawdown_analysis = self._load_drawdown_analysis()

        # 回撤分析部分（避免幸存者偏差）
        drawdown_section = self._generate_drawdown_section(drawdown_analysis)
        lines.append(drawdown_section)

        # 选股回顾部分
        picks_section = self._generate_picks_review_section(picks_review)
        lines.append(picks_section)

        # 结尾
        lines.append("\n" + "=" * 70)
        lines.append("【风险提示】以上分析仅供参考，不构成投资建议")
        lines.append("=" * 70)

        return "\n".join(lines)

    # 保持向后兼容的方法名
    def generate_text_report(self, dq_report=None, market_review=None, picks_review=None) -> str:
        """生成文本报告（兼容旧接口）"""
        data = {
            'dq_report': dq_report,
            'market_review': market_review,
            'picks_review': picks_review
        }
        return self.generate(data)

    def _send(self, content: str) -> bool:
        """发送报告并保存到数据库

        邮件发送失败不视为任务失败，只要报告生成成功即可
        """
        success = super()._send(content)

        # 无论邮件是否成功，都尝试保存到数据库
        try:
            db_service = ReportDBService()
            today = datetime.now().strftime('%Y-%m-%d')
            subject = f"【复盘快报】A股今日总结 {today}"

            db_service.save_report(
                report_type='review',
                report_date=today,
                subject=subject,
                text_content=content
            )
            self.logger.info("报告已保存到数据库")

            # 保存TXT文件
            txt_path = db_service.save_txt_file('review', today, content)
            self.logger.info(f"TXT已保存: {txt_path}")

        except Exception as e:
            self.logger.warning(f"保存到数据库失败: {e}")

        # 邮件发送失败不导致任务失败，只要报告内容生成成功即可
        if not success:
            self.logger.warning("邮件发送失败，但报告已保存到本地")
            return True  # 报告生成成功即视为任务成功

        return success

    def run(self) -> bool:
        """执行复盘报告推送（兼容旧接口）"""
        return super().run()


def main():
    """主函数"""
    generator = ReviewReportGenerator()
    success = generator.run()

    result = generator.get_last_result()
    if result:
        print(f"\n执行结果: {result.status.value}")
        if result.error_message:
            print(f"错误: {result.error_message}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
