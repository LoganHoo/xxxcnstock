"""
增强版明日股票推荐报告生成器

功能特性:
1. 交易日计算：如果是周五，自动计算下一个交易日（周一）
2. 数据质量说明：完整性、新鲜度、原始数据质量
3. 数据路径追踪：展示数据来源和存储位置
4. 增强的报告格式：文本、HTML、JSON

================================================================================
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
from scripts.tomorrow_picks import (
    ConfigManager, DataLoader, FilterEngine, StockRecommender,
    TextReporter, HTMLReporter, JSONReporter
)

logger = logging.getLogger(__name__)


class TradingCalendar:
    """交易日历工具类"""
    
    # 2024-2025年中国A股休市安排（主要节假日）
    HOLIDAYS_2024_2025 = [
        # 2024年
        "2024-01-01",  # 元旦
        "2024-02-09", "2024-02-10", "2024-02-11", "2024-02-12", "2024-02-13", "2024-02-14", "2024-02-15", "2024-02-16",  # 春节
        "2024-04-04", "2024-04-05", "2024-04-06",  # 清明节
        "2024-05-01", "2024-05-02", "2024-05-03", "2024-05-04", "2024-05-05",  # 劳动节
        "2024-06-10",  # 端午节
        "2024-09-15", "2024-09-16", "2024-09-17",  # 中秋节
        "2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04", "2024-10-05", "2024-10-06", "2024-10-07",  # 国庆节
        # 2025年
        "2025-01-01",  # 元旦
        "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31", "2025-02-01", "2025-02-02", "2025-02-03", "2025-02-04",  # 春节
        "2025-04-04", "2025-04-05", "2025-04-06",  # 清明节
        "2025-05-01", "2025-05-02", "2025-05-03", "2025-05-04", "2025-05-05",  # 劳动节
        "2025-05-31", "2025-06-01", "2025-06-02",  # 端午节
        "2025-10-01", "2025-10-02", "2025-10-03", "2025-10-04", "2025-10-05", "2025-10-06", "2025-10-07", "2025-10-08",  # 国庆节
    ]
    
    @classmethod
    def is_trading_day(cls, date: datetime) -> bool:
        """判断是否为交易日"""
        # 周末
        if date.weekday() >= 5:  # 5=周六, 6=周日
            return False
        
        # 节假日
        date_str = date.strftime('%Y-%m-%d')
        if date_str in cls.HOLIDAYS_2024_2025:
            return False
        
        return True
    
    @classmethod
    def get_next_trading_day(cls, date: datetime) -> datetime:
        """获取下一个交易日"""
        next_day = date + timedelta(days=1)
        while not cls.is_trading_day(next_day):
            next_day += timedelta(days=1)
        return next_day
    
    @classmethod
    def get_report_target_date(cls, base_date: datetime = None) -> Tuple[datetime, str, str]:
        """
        获取报告目标日期
        
        Returns:
            (target_date, date_description, trading_day_info)
        """
        if base_date is None:
            base_date = datetime.now()
        
        # 如果是周五，下一个交易日是下周一
        if base_date.weekday() == 4:  # 周五
            target_date = cls.get_next_trading_day(base_date)
            date_description = f"下周一 ({target_date.strftime('%Y-%m-%d')})"
            trading_day_info = f"今日周五，推荐股票目标交易日为下周一 ({target_date.strftime('%Y-%m-%d')})"
        else:
            target_date = cls.get_next_trading_day(base_date)
            weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
            date_description = f"明日 ({weekday_names[target_date.weekday()]} {target_date.strftime('%Y-%m-%d')})"
            trading_day_info = f"推荐股票目标交易日为明日 ({target_date.strftime('%Y-%m-%d')})"
        
        return target_date, date_description, trading_day_info


class DataQualityChecker:
    """数据质量检查器"""
    
    def __init__(self, data_loader: DataLoader):
        self.data_loader = data_loader
        self.quality_report = {}
    
    def check_data_completeness(self, df: pl.DataFrame) -> Dict:
        """检查数据完整性"""
        total_records = len(df)
        
        # 检查必要字段
        required_fields = ['code', 'name', 'close', 'change_pct']
        missing_fields = [f for f in required_fields if f not in df.columns]
        
        # 检查空值
        null_counts = {}
        for col in df.columns:
            null_count = df[col].null_count()
            if null_count > 0:
                null_counts[col] = null_count
        
        # 计算完整率
        if total_records > 0:
            completeness_rate = (total_records - sum(null_counts.values()) / max(len(df.columns), 1)) / total_records * 100
        else:
            completeness_rate = 0
        
        return {
            'total_records': total_records,
            'missing_fields': missing_fields,
            'null_counts': null_counts,
            'completeness_rate': round(completeness_rate, 2),
            'status': '良好' if completeness_rate > 95 and not missing_fields else '需关注'
        }
    
    def check_data_freshness(self, df: pl.DataFrame) -> Dict:
        """检查数据新鲜度"""
        now = datetime.now()
        
        # 获取最新数据日期
        if 'trade_date' in df.columns:
            latest_date_str = df['trade_date'].max()
            try:
                latest_date = datetime.strptime(str(latest_date_str), '%Y-%m-%d')
            except:
                latest_date = now
        elif 'update_time' in df.columns:
            latest_time_str = df['update_time'].max()
            try:
                latest_date = datetime.strptime(str(latest_time_str)[:10], '%Y-%m-%d')
            except:
                latest_date = now
        else:
            latest_date = now
        
        # 计算数据延迟
        delay_days = (now - latest_date).days
        
        # 判断新鲜度
        if delay_days == 0:
            freshness = '最新'
            status = '优秀'
        elif delay_days == 1:
            freshness = '较新'
            status = '良好'
        elif delay_days <= 3:
            freshness = '一般'
            status = '可接受'
        else:
            freshness = '滞后'
            status = '需更新'
        
        return {
            'latest_date': latest_date.strftime('%Y-%m-%d'),
            'delay_days': delay_days,
            'freshness': freshness,
            'status': status
        }
    
    def check_raw_data_quality(self) -> Dict:
        """检查原始数据质量"""
        quality_info = {
            'kline_data': {},
            'index_data': {},
            'stock_list': {},
            'key_levels': {},
            'cvd_data': {}
        }
        
        # K线数据
        kline_dir = Path(self.data_loader.kline_dir) if self.data_loader.kline_dir else None
        if kline_dir and kline_dir.exists():
            parquet_files = list(kline_dir.glob('*.parquet'))
            quality_info['kline_data'] = {
                'path': str(kline_dir),
                'file_count': len(parquet_files),
                'status': '正常' if len(parquet_files) > 4000 else '需关注'
            }
        else:
            quality_info['kline_data'] = {
                'path': str(kline_dir) if kline_dir else 'N/A',
                'file_count': 0,
                'status': '缺失'
            }
        
        # 指数数据
        index_path = Path('data/index/000001.parquet')
        if index_path.exists():
            try:
                index_df = pl.read_parquet(index_path)
                quality_info['index_data'] = {
                    'path': str(index_path),
                    'record_count': len(index_df),
                    'status': '正常'
                }
            except:
                quality_info['index_data'] = {
                    'path': str(index_path),
                    'record_count': 0,
                    'status': '异常'
                }
        else:
            quality_info['index_data'] = {
                'path': str(index_path),
                'record_count': 0,
                'status': '缺失'
            }
        
        # 股票列表
        if self.data_loader.stock_list_path:
            stock_list_path = Path(self.data_loader.stock_list_path)
            if stock_list_path.exists():
                try:
                    stock_list_df = pl.read_parquet(stock_list_path)
                    quality_info['stock_list'] = {
                        'path': str(stock_list_path),
                        'record_count': len(stock_list_df),
                        'status': '正常'
                    }
                except:
                    quality_info['stock_list'] = {
                        'path': str(stock_list_path),
                        'record_count': 0,
                        'status': '异常'
                    }
            else:
                quality_info['stock_list'] = {
                    'path': str(stock_list_path),
                    'record_count': 0,
                    'status': '缺失'
                }
        
        # 关键位数据
        if self.data_loader.key_levels_path:
            key_levels_path = Path(self.data_loader.key_levels_path)
            if key_levels_path.exists():
                try:
                    key_levels_df = pl.read_parquet(key_levels_path)
                    quality_info['key_levels'] = {
                        'path': str(key_levels_path),
                        'record_count': len(key_levels_df),
                        'status': '正常'
                    }
                except:
                    quality_info['key_levels'] = {
                        'path': str(key_levels_path),
                        'record_count': 0,
                        'status': '异常'
                    }
            else:
                quality_info['key_levels'] = {
                    'path': str(key_levels_path),
                    'record_count': 0,
                    'status': '缺失'
                }
        
        # CVD数据
        if self.data_loader.cvd_path:
            cvd_path = Path(self.data_loader.cvd_path)
            if cvd_path.exists():
                try:
                    cvd_df = pl.read_parquet(cvd_path)
                    quality_info['cvd_data'] = {
                        'path': str(cvd_path),
                        'record_count': len(cvd_df),
                        'status': '正常'
                    }
                except:
                    quality_info['cvd_data'] = {
                        'path': str(cvd_path),
                        'record_count': 0,
                        'status': '异常'
                    }
            else:
                quality_info['cvd_data'] = {
                    'path': str(cvd_path),
                    'record_count': 0,
                    'status': '缺失'
                }
        
        return quality_info
    
    def generate_quality_report(self, df: pl.DataFrame) -> Dict:
        """生成完整的数据质量报告"""
        self.quality_report = {
            'completeness': self.check_data_completeness(df),
            'freshness': self.check_data_freshness(df),
            'raw_data': self.check_raw_data_quality(),
            'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return self.quality_report


class EnhancedTextReporter(TextReporter):
    """增强版文本报告生成器"""
    
    def __init__(self, data_loader: DataLoader = None, quality_checker: DataQualityChecker = None):
        super().__init__(data_loader)
        self.quality_checker = quality_checker
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager,
                 trading_info: Dict = None,
                 quality_report: Dict = None) -> str:
        lines = []
        
        # 报告标题
        lines.append("=" * 80)
        lines.append("明日股票推荐报告")
        lines.append("=" * 80)
        
        # 报告时间信息
        now = datetime.now()
        lines.append(f"\n📅 报告生成时间: {now.strftime('%Y年%m月%d日 %H:%M:%S')}")
        
        # 交易日信息
        if trading_info:
            lines.append(f"📊 目标交易日: {trading_info.get('date_description', '明日')}")
            lines.append(f"⏰ 交易提示: {trading_info.get('trading_day_info', '')}")
        
        lines.append("")
        
        # 数据质量说明
        if quality_report:
            lines.append("-" * 80)
            lines.append("【数据质量说明】")
            lines.append("-" * 80)
            
            # 完整性
            completeness = quality_report.get('completeness', {})
            lines.append(f"\n📊 数据完整性:")
            lines.append(f"   • 总记录数: {completeness.get('total_records', 0):,} 条")
            lines.append(f"   • 完整率: {completeness.get('completeness_rate', 0)}%")
            lines.append(f"   • 状态: {completeness.get('status', '未知')}")
            if completeness.get('missing_fields'):
                lines.append(f"   • 缺失字段: {', '.join(completeness['missing_fields'])}")
            
            # 新鲜度
            freshness = quality_report.get('freshness', {})
            lines.append(f"\n🕐 数据新鲜度:")
            lines.append(f"   • 最新数据日期: {freshness.get('latest_date', '未知')}")
            lines.append(f"   • 数据延迟: {freshness.get('delay_days', 0)} 天")
            lines.append(f"   • 新鲜度: {freshness.get('freshness', '未知')} ({freshness.get('status', '未知')})")
            
            # 原始数据质量
            raw_data = quality_report.get('raw_data', {})
            lines.append(f"\n📁 原始数据质量:")
            for data_type, info in raw_data.items():
                lines.append(f"   • {data_type}:")
                lines.append(f"     - 路径: {info.get('path', 'N/A')}")
                lines.append(f"     - 记录/文件数: {info.get('record_count', 0) if 'record_count' in info else info.get('file_count', 0)}")
                lines.append(f"     - 状态: {info.get('status', '未知')}")
            
            lines.append("")
        
        # 调用父类生成股票推荐内容
        stock_lines = super().generate(filter_results, stats, config_manager).split('\n')
        
        # 过滤掉父类的标题，因为我们已经添加了增强版标题
        skip_until = 0
        for i, line in enumerate(stock_lines):
            if '【' in line and '】' in line:
                skip_until = i
                break
        
        lines.extend(stock_lines[skip_until:])
        
        return "\n".join(lines)


class EnhancedHTMLReporter(HTMLReporter):
    """增强版HTML报告生成器"""
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager,
                 trading_info: Dict = None,
                 quality_report: Dict = None) -> str:
        html_parts = [
            '<!DOCTYPE html>',
            '<html lang="zh-CN">',
            '<head>',
            '    <meta charset="UTF-8">',
            '    <meta name="viewport" content="width=device-width, initial-scale=1.0">',
            '    <title>明日股票推荐报告</title>',
            '    <style>',
            '        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 20px; background-color: #f5f5f5; }',
            '        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }',
            '        h1 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }',
            '        h2 { color: #555; margin-top: 30px; }',
            '        .header-info { background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 20px 0; }',
            '        .header-info p { margin: 5px 0; }',
            '        .quality-section { background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0; }',
            '        .quality-item { margin: 10px 0; padding: 10px; background: white; border-radius: 3px; }',
            '        .quality-label { font-weight: bold; color: #666; }',
            '        .quality-value { color: #333; }',
            '        .status-good { color: #28a745; }',
            '        .status-warning { color: #ffc107; }',
            '        .status-error { color: #dc3545; }',
            '        .data-path { font-family: monospace; font-size: 0.9em; color: #666; }',
            '        .section { margin: 20px 0; padding: 15px; border-radius: 5px; }',
            '        .s-grade { background-color: #d4edda; border-left: 4px solid #28a745; }',
            '        .a-grade { background-color: #fff3cd; border-left: 4px solid #ffc107; }',
            '        .bullish { background-color: #cce5ff; border-left: 4px solid #007bff; }',
            '        .stock { padding: 8px; margin: 5px 0; background: white; border-radius: 3px; }',
            '        .code { font-weight: bold; color: #007bff; }',
            '        .name { margin-left: 10px; }',
            '        .price { margin-left: 10px; color: #28a745; }',
            '        .change { margin-left: 10px; }',
            '        .positive { color: #28a745; }',
            '        .negative { color: #dc3545; }',
            '        .score { margin-left: 10px; font-weight: bold; }',
            '        .stats { background-color: #e9ecef; padding: 15px; border-radius: 5px; margin-top: 20px; }',
            '        .warning { background-color: #fff3cd; padding: 10px; border-radius: 5px; margin-top: 20px; }',
            '    </style>',
            '</head>',
            '<body>',
            '    <div class="container">',
            f'        <h1>📈 明日股票推荐报告</h1>',
        ]
        
        # 头部信息
        now = datetime.now()
        html_parts.append('        <div class="header-info">')
        html_parts.append(f'            <p><strong>📅 报告生成时间:</strong> {now.strftime("%Y年%m月%d日 %H:%M:%S")}</p>')
        
        if trading_info:
            html_parts.append(f'            <p><strong>📊 目标交易日:</strong> {trading_info.get("date_description", "明日")}</p>')
            html_parts.append(f'            <p><strong>⏰ 交易提示:</strong> {trading_info.get("trading_day_info", "")}</p>')
        
        html_parts.append('        </div>')
        
        # 数据质量说明
        if quality_report:
            html_parts.append('        <div class="quality-section">')
            html_parts.append('            <h2>📊 数据质量说明</h2>')
            
            # 完整性
            completeness = quality_report.get('completeness', {})
            status_class = 'status-good' if completeness.get('status') == '良好' else 'status-warning'
            html_parts.append('            <div class="quality-item">')
            html_parts.append('                <span class="quality-label">数据完整性:</span>')
            html_parts.append(f'                <span class="quality-value">总记录 {completeness.get("total_records", 0):,} 条，完整率 {completeness.get("completeness_rate", 0)}%</span>')
            html_parts.append(f'                <span class="{status_class}">[{completeness.get("status", "未知")}]</span>')
            html_parts.append('            </div>')
            
            # 新鲜度
            freshness = quality_report.get('freshness', {})
            freshness_class = 'status-good' if freshness.get('status') == '优秀' else ('status-warning' if freshness.get('status') == '良好' else 'status-error')
            html_parts.append('            <div class="quality-item">')
            html_parts.append('                <span class="quality-label">数据新鲜度:</span>')
            html_parts.append(f'                <span class="quality-value">最新数据 {freshness.get("latest_date", "未知")}，延迟 {freshness.get("delay_days", 0)} 天</span>')
            html_parts.append(f'                <span class="{freshness_class}">[{freshness.get("freshness", "未知")} - {freshness.get("status", "未知")}]</span>')
            html_parts.append('            </div>')
            
            # 原始数据
            html_parts.append('            <div class="quality-item">')
            html_parts.append('                <span class="quality-label">原始数据质量:</span>')
            html_parts.append('                <ul>')
            
            raw_data = quality_report.get('raw_data', {})
            for data_type, info in raw_data.items():
                status_class = 'status-good' if info.get('status') == '正常' else ('status-warning' if info.get('status') == '需关注' else 'status-error')
                count = info.get('record_count', 0) if 'record_count' in info else info.get('file_count', 0)
                html_parts.append(f'                    <li>{data_type}: <span class="data-path">{info.get("path", "N/A")}</span> - {count} 条/文件 <span class="{status_class}">[{info.get("status", "未知")}]</span></li>')
            
            html_parts.append('                </ul>')
            html_parts.append('            </div>')
            html_parts.append('        </div>')
        
        # 股票推荐内容
        for filter_name, df in filter_results.items():
            if len(df) == 0:
                continue
            
            config = config_manager.get_filter_config(filter_name)
            css_class = filter_name.replace('_', '-')
            
            html_parts.append(f'        <div class="section {css_class}">')
            html_parts.append(f'            <h2>{config["description"]}</h2>')
            
            for row in df.iter_rows(named=True):
                change = f"+{row['change_pct']}" if row['change_pct'] >= 0 else str(row['change_pct'])
                change_class = 'positive' if row['change_pct'] >= 0 else 'negative'
                
                html_parts.append('            <div class="stock">')
                html_parts.append(f'                <span class="code">{row["code"]}</span>')
                html_parts.append(f'                <span class="name">{row["name"]}</span>')
                html_parts.append(f'                <span class="price">{row["price"]:.2f}元</span>')
                html_parts.append(f'                <span class="change {change_class}">{change}%</span>')
                html_parts.append(f'                <span class="score">评分{row["enhanced_score"]:.0f}</span>')
                html_parts.append('            </div>')
            
            html_parts.append('        </div>')
        
        # 统计摘要
        html_parts.append('        <div class="stats">')
        html_parts.append('            <h2>📊 统计摘要</h2>')
        html_parts.append(f'            <p>✅ S级: {stats["s_grade_count"]} 只 (强烈推荐)</p>')
        html_parts.append(f'            <p>✅ A级: {stats["a_grade_count"]} 只 (建议关注)</p>')
        html_parts.append(f'            <p>📈 多头排列: {stats["bullish_count"]} 只</p>')
        html_parts.append(f'            <p>⬆️  今日上涨: {stats["rising_count"]} 只</p>')
        html_parts.append('        </div>')
        
        # 风险提示
        html_parts.append('        <div class="warning">')
        html_parts.append('            <h3>⚠️ 风险提示</h3>')
        html_parts.append('            <p>以上分析基于技术指标，仅供参考，不构成投资建议。</p>')
        html_parts.append('            <p>股市有风险，投资需谨慎。</p>')
        html_parts.append('        </div>')
        
        html_parts.append('    </div>')
        html_parts.append('</body>')
        html_parts.append('</html>')
        
        return '\n'.join(html_parts)


class EnhancedStockRecommender(StockRecommender):
    """增强版股票推荐系统"""
    
    def run(self):
        """执行增强版推荐流程"""
        try:
            self.logger.info("="*80)
            self.logger.info("开始增强版股票推荐流程")
            self.logger.info("="*80)
            
            analysis_time = datetime.now()
            self.logger.info(f"📅 分析时间: {analysis_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 1. 计算交易日信息
            trading_calendar = TradingCalendar()
            target_date, date_description, trading_day_info = trading_calendar.get_report_target_date(analysis_time)
            trading_info = {
                'target_date': target_date.strftime('%Y-%m-%d'),
                'date_description': date_description,
                'trading_day_info': trading_day_info
            }
            self.logger.info(f"📊 {trading_day_info}")
            
            # 2. 加载数据
            data_path = self.config_manager.get_data_path()
            self.logger.info(f"📁 数据路径: {data_path}")
            
            self.data_loader.check_and_update_data()
            df = self.data_loader.load_data()
            
            # 3. 数据质量检查
            self.logger.info("执行数据质量检查...")
            quality_checker = DataQualityChecker(self.data_loader)
            quality_report = quality_checker.generate_quality_report(df)
            
            completeness = quality_report['completeness']
            self.logger.info(f"数据完整性: {completeness['completeness_rate']}% ({completeness['status']})")
            
            freshness = quality_report['freshness']
            self.logger.info(f"数据新鲜度: {freshness['freshness']}，延迟 {freshness['delay_days']} 天")
            
            # 4. 合并关键位和CVD数据
            key_levels = self.data_loader.load_key_levels()
            if key_levels is not None:
                df = self.data_loader.merge_key_levels(df, key_levels)
            
            cvd_data = self.data_loader.load_cvd()
            if cvd_data is not None:
                df = self.data_loader.merge_cvd(df, cvd_data)
            
            # 5. 过滤停牌股票
            df = self._filter_suspended_stocks(df)
            
            # 6. 应用筛选器
            filter_results = self.filter_engine.apply_all_filters(df)
            
            # 7. 计算统计信息
            stats = self.calculate_stats(df)
            
            # 8. 生成增强版报告
            reports = {}
            
            # 文本报告
            text_reporter = EnhancedTextReporter(self.data_loader, quality_checker)
            reports['text'] = text_reporter.generate(
                filter_results, stats, self.config_manager,
                trading_info=trading_info,
                quality_report=quality_report
            )
            
            # HTML报告
            html_reporter = EnhancedHTMLReporter()
            reports['html'] = html_reporter.generate(
                filter_results, stats, self.config_manager,
                trading_info=trading_info,
                quality_report=quality_report
            )
            
            # JSON报告
            json_reporter = JSONReporter()
            reports['json'] = json_reporter.generate(filter_results, stats, self.config_manager)
            
            # 9. 保存报告
            if self.config_manager.config['recommendation']['output']['save_to_file']:
                self.save_reports(reports)
            
            # 10. 保存到MySQL
            if self.mysql_storage:
                pick_date = analysis_time.strftime('%Y-%m-%d')
                self.mysql_storage.save_picks(filter_results, stats, pick_date)
            
            # 11. 发送邮件
            email_config = self.config_manager.get_email_config()
            if email_config.get('enabled', False):
                subject = f"{email_config['subject_prefix']} - {date_description}"
                self.email_notifier.send_report(
                    subject=subject,
                    content=reports.get('text', ''),
                    html_content=reports.get('html')
                )
            
            # 12. 输出到控制台
            if 'text' in reports:
                print(reports['text'])
            
            self.logger.info("="*80)
            self.logger.info("增强版股票推荐流程完成")
            self.logger.info("="*80)
            
        except Exception as e:
            self.logger.error(f"推荐流程失败: {e}")
            raise


def main():
    """主函数"""
    PROJECT_ROOT = Path(__file__).parent.parent
    CONFIG_FILE = PROJECT_ROOT / "config" / "xcn_comm.yaml"
    
    recommender = EnhancedStockRecommender(str(CONFIG_FILE))
    recommender.run()


if __name__ == '__main__':
    main()
