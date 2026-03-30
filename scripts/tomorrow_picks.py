"""
明日推荐股票
使用 Polars + DuckDB 优化性能
支持文本、HTML、JSON输出和邮件通知

================================================================================
策略说明文档
================================================================================

一、关键位识别方法 (Key Levels)
----------------------------------------
基于 Fabio Valentini 策略，通过以下三种市场结构识别关键位：

1. 供需区域 (Supply and Demand Zones)
   - 定义：价格在过去曾出现剧烈反应或转向的区域
   - 识别方法：寻找大实体K线（body_ratio > 0.7）且涨跌幅 > 3% 的区域
   - 意义：代表大资金买入或卖出的痕迹，后续价格可能再次在此区域反应

2. 箱体结构 (Box Structures)
   - 定义：市场在一段时间内的窄幅区间波动（盘整区）
   - 识别方法：20日价格波动率（std/mean）< 5%
   - 意义：代表多空双方暂时平衡，一旦被打破往往伴随大行情

3. 大额订单位置
   - 定义：成交量显著放大的价格区域
   - 识别方法：成交量 > 20日均量 × 1.5
   - 意义：大资金建仓或出货位置，是重要支撑/压力位

关键位字段说明：
  - support_strong: 强支撑位（60日最低价10%分位数）
  - resistance_strong: 强压力位（60日最高价90%分位数）
  - demand_zone: 需求区域（大阳线最低价）
  - supply_zone: 供给区域（大阴线最高价）
  - is_consolidating: 是否处于箱体震荡（1=是，0=否）

二、CVD 指标 (Cumulative Volume Delta)
----------------------------------------
CVD 被称为"市场压力的探测器"，用于衡量买卖力量的累积差异。

1. 基础数据 (Delta)
   - 定义：主动买入量与主动卖出量的差异
   - 估算方法（无逐笔数据时）：
     * K线实体法：delta = volume × (close - open) / (high - low)
     * 收盘位置法：delta = volume × (2 × (close - low) / (high - low) - 1)
     * 综合法：取两种方法的平均值

2. 累积计算
   - 公式：CVD(n) = Σ(delta, i=1 to n)
   - 滚动窗口：60日累积

3. 背离检测
   - 顶背离：价格上涨 + CVD下跌 → 卖压累积，可能反转下行
   - 底背离：价格下跌 + CVD上涨 → 买压累积，可能反转上行

CVD 字段说明：
  - cvd_60d: 60日累积成交量差
  - cvd_signal: 买卖信号（buy_dominant/sell_dominant/neutral）
  - cvd_trend: 趋势状态（strong_buy/weak_buy/strong_sell/weak_sell）
  - divergence_5d/10d: 5日/10日背离信号

三、综合评分逻辑
----------------------------------------
基于第一性原理的多维度评分：

1. 趋势判断 (20%) - 多空排列、均线趋势
2. 动量指标 (15%) - 3日/5日/10日/20日涨跌幅
3. 成交量分析 (15%) - 量价关系、放量缩量
4. 技术指标 (20%) - MACD/RSI/KDJ/布林带
5. 位置分析 (15%) - 支撑压力位、上涨空间
6. 波动分析 (15%) - ATR、振幅、稳定性

四、使用方法
----------------------------------------
1. 运行预计算：
   python scripts/calculate_key_levels_cvd.py

2. 运行推荐系统：
   python scripts/tomorrow_picks.py

3. 输出文件：
   - data/key_levels_latest.parquet: 关键位数据
   - data/cvd_latest.parquet: CVD指标数据
   - reports/daily_picks_*.txt/html/json: 推荐报告

================================================================================
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
import duckdb
import yaml
import json
import logging
import os
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from services.data_validator import DataValidator
from services.email_sender import EmailService
from core.freshness_check_decorator import check_data_freshness

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from dotenv import load_dotenv
    MYSQL_AVAILABLE = True
    load_dotenv()
except ImportError:
    MYSQL_AVAILABLE = False


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path).resolve()
        self.project_root = self.config_path.parent.parent
        self.config = self.load_config()
        self.validate_config()
        self.logger = logging.getLogger(__name__)
    
    def load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"配置文件加载失败: {e}")
    
    def validate_config(self):
        """验证配置"""
        if 'recommendation' not in self.config:
            raise ValueError("配置文件缺少 recommendation 部分")
        
        if 'filters' not in self.config['recommendation']:
            raise ValueError("配置文件缺少 filters 部分")
    
    def get_filter_config(self, filter_name: str) -> dict:
        """获取筛选器配置"""
        return self.config['recommendation']['filters'].get(filter_name, {})
    
    def get_data_path(self) -> str:
        """获取数据路径（返回绝对路径）"""
        relative_path = self.config['data_paths']['enhanced_scores_full']
        return str(self.project_root / relative_path)
    
    def get_output_formats(self) -> List[str]:
        """获取输出格式列表"""
        return self.config['recommendation']['output']['formats']
    
    def get_output_dir(self) -> str:
        """获取输出目录（返回绝对路径）"""
        relative_path = self.config['recommendation']['output']['output_dir']
        return str(self.project_root / relative_path)
    
    def get_output_prefix(self) -> str:
        """获取输出文件前缀"""
        return self.config['recommendation']['output']['filename_prefix']
    
    def get_email_config(self) -> dict:
        """获取邮件配置"""
        return self.config['recommendation']['email']
    
    def get_kline_dir(self) -> str:
        """获取K线数据目录（返回绝对路径）"""
        relative_path = self.config['data_paths']['kline_dir']
        return str(self.project_root / relative_path)
    
    def get_stock_list_path(self) -> str:
        """获取股票列表路径（返回绝对路径）"""
        return str(self.project_root / 'data/stock_list.parquet')
    
    def get_key_levels_path(self) -> str:
        """获取关键位数据路径（返回绝对路径）"""
        return str(self.project_root / 'data/key_levels_latest.parquet')
    
    def get_cvd_path(self) -> str:
        """获取CVD数据路径（返回绝对路径）"""
        return str(self.project_root / 'data/cvd_latest.parquet')


class DataLoader:
    """数据加载器"""
    
    def __init__(self, data_path: str, kline_dir: str = None, stock_list_path: str = None, 
                 key_levels_path: str = None, cvd_path: str = None):
        self.data_path = data_path
        self.kline_dir = kline_dir
        self.stock_list_path = stock_list_path
        self.key_levels_path = key_levels_path
        self.cvd_path = cvd_path
        self.logger = logging.getLogger(__name__)
    
    def get_effective_date(self) -> tuple:
        """
        根据当前时间确定有效数据日期
        规则：15点之前使用昨天数据，15点之后使用今天数据
        
        Returns:
            tuple: (effective_date_str, is_before_cutoff)
                   effective_date_str: 有效日期字符串 (YYYY-MM-DD)
                   is_before_cutoff: 是否在15点分界线之前
        """
        from datetime import timedelta
        
        now = datetime.now()
        current_hour = now.hour
        
        cutoff_hour = 15
        
        if current_hour < cutoff_hour:
            effective_date = now - timedelta(days=1)
            is_before_cutoff = True
        else:
            effective_date = now
            is_before_cutoff = False
        
        effective_date_str = effective_date.strftime('%Y-%m-%d')
        
        self.logger.info(f"⏰ 当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"📅 有效数据日期: {effective_date_str} ({'15点前-使用昨日数据' if is_before_cutoff else '15点后-使用今日数据'})")
        
        return effective_date_str, is_before_cutoff
    
    def check_and_update_data(self) -> bool:
        """检查数据是否需要更新，如果需要则运行预计算"""
        import subprocess
        
        effective_date, _ = self.get_effective_date()
        
        need_update = False
        
        if not Path(self.data_path).exists():
            self.logger.info("预计算数据文件不存在，需要更新")
            need_update = True
        else:
            try:
                df = pl.read_parquet(self.data_path)
                
                if 'trade_date' in df.columns:
                    latest_date = str(df['trade_date'].max())
                    if latest_date < effective_date:
                        self.logger.info(f"数据日期 {latest_date} 早于有效日期 {effective_date}，需要更新")
                        need_update = True
                elif len(df) == 0:
                    self.logger.info("数据文件为空，需要更新")
                    need_update = True
                    
            except Exception as e:
                self.logger.warning(f"读取数据文件失败: {e}，需要更新")
                need_update = True
        
        if need_update:
            self.logger.info("正在运行预计算脚本...")
            script_path = Path(__file__).parent / "precompute_enhanced_scores.py"
            
            if script_path.exists():
                try:
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        capture_output=True,
                        text=True,
                        timeout=300
                    )
                    
                    if result.returncode == 0:
                        self.logger.info("预计算完成")
                        return True
                    else:
                        self.logger.error(f"预计算失败: {result.stderr}")
                        return False
                except subprocess.TimeoutExpired:
                    self.logger.error("预计算超时")
                    return False
                except Exception as e:
                    self.logger.error(f"运行预计算脚本失败: {e}")
                    return False
            else:
                self.logger.warning(f"预计算脚本不存在: {script_path}")
                return False
        
        return True
    
    def load_data(self) -> pl.DataFrame:
        """使用Polars加载数据"""
        try:
            df = pl.read_parquet(self.data_path)
            self.validate_data(df)
            self.logger.info(f"数据加载成功: {len(df)} 条记录")
            
            effective_date, _ = self.get_effective_date()
            
            if 'trade_date' in df.columns:
                df = self._filter_by_date(df, effective_date, 'trade_date')
            elif 'update_time' in df.columns:
                df = self._filter_by_datetime(df, effective_date, 'update_time')
            
            return df
        except FileNotFoundError:
            self.logger.warning(f"预计算数据文件不存在: {self.data_path}")
            if self.kline_dir:
                self.logger.info("尝试从K线数据重新计算...")
                return self.load_from_kline()
            raise
        except Exception as e:
            self.logger.error(f"数据加载失败: {e}")
            raise
    
    def _filter_by_date(self, df: pl.DataFrame, effective_date: str, date_col: str) -> pl.DataFrame:
        """根据日期列过滤数据"""
        try:
            df = df.filter(pl.col(date_col) <= effective_date)
            self.logger.info(f"按日期 {effective_date} 过滤后: {len(df)} 条记录")
            return df
        except Exception as e:
            self.logger.warning(f"日期过滤失败: {e}，返回原始数据")
            return df
    
    def _filter_by_datetime(self, df: pl.DataFrame, effective_date: str, datetime_col: str) -> pl.DataFrame:
        """根据日期时间列过滤数据"""
        try:
            df = df.filter(pl.col(datetime_col).str.slice(0, 10) <= effective_date)
            self.logger.info(f"按日期时间 {effective_date} 过滤后: {len(df)} 条记录")
            return df
        except Exception as e:
            self.logger.warning(f"日期时间过滤失败: {e}，返回原始数据")
            return df
    
    def load_from_kline(self) -> pl.DataFrame:
        """从K线数据加载并计算技术指标"""
        import glob
        
        self.logger.info(f"从K线目录加载数据: {self.kline_dir}")
        
        parquet_files = glob.glob(f"{self.kline_dir}/*.parquet")
        if not parquet_files:
            raise FileNotFoundError(f"K线目录为空: {self.kline_dir}")
        
        self.logger.info(f"找到 {len(parquet_files)} 个K线文件")
        
        effective_date, _ = self.get_effective_date()
        
        all_data = []
        for f in parquet_files:
            try:
                df = pl.read_parquet(f)
                if len(df) > 0:
                    if 'trade_date' in df.columns:
                        df = df.filter(pl.col('trade_date') <= effective_date)
                    all_data.append(df)
            except Exception as e:
                self.logger.warning(f"读取文件失败 {f}: {e}")
        
        if not all_data:
            raise ValueError("没有有效的K线数据")
        
        kline_df = pl.concat(all_data)
        self.logger.info(f"合并K线数据(截止{effective_date}): {len(kline_df)} 条记录")
        
        result_df = self.calculate_technical_indicators(kline_df, effective_date)
        self.logger.info(f"技术指标计算完成: {len(result_df)} 只股票")
        
        return result_df
    
    def calculate_technical_indicators(self, df: pl.DataFrame, effective_date: str = None) -> pl.DataFrame:
        """计算技术指标"""
        if effective_date:
            latest_date = effective_date
        else:
            latest_date = df['trade_date'].max()
        self.logger.info(f"K线数据有效日期: {latest_date}")
        
        latest_df = df.filter(pl.col('trade_date') == latest_date)
        
        prev_date_df = df.filter(
            (pl.col('trade_date') < latest_date)
        ).sort('trade_date', descending=True).group_by('code').first()
        
        stock_list = None
        if self.stock_list_path:
            try:
                stock_list = pl.read_parquet(self.stock_list_path)
                self.logger.info(f"加载股票列表: {len(stock_list)} 条")
            except Exception as e:
                self.logger.warning(f"加载股票列表失败: {e}")
        
        result = latest_df.join(prev_date_df, on='code', suffix='_prev')
        
        result = result.with_columns([
            ((pl.col('close') - pl.col('close_prev')) / pl.col('close_prev') * 100)
            .alias('change_pct')
        ])
        
        if stock_list is not None and 'name' in stock_list.columns:
            result = result.join(
                stock_list.select(['code', 'name']), 
                on='code', 
                how='left'
            )
        else:
            result = result.with_columns(pl.lit('').alias('name'))
        
        result = result.with_columns([
            pl.col('close').alias('price'),
            pl.lit(50.0).alias('enhanced_score'),
            pl.lit('C').alias('grade'),
            pl.lit(0).alias('trend'),
            pl.lit(50.0).alias('momentum'),
            pl.lit(50.0).alias('tech'),
            pl.lit(50.0).alias('rsi'),
            pl.lit(0.0).alias('momentum_3d'),
            pl.lit(0.0).alias('momentum_10d'),
            pl.lit(0.0).alias('momentum_20d'),
            pl.lit(0.5).alias('position'),
            pl.lit('待分析').alias('reasons')
        ])
        
        result = result.with_columns([
            pl.when(pl.col('change_pct') > 3).then(pl.lit('S'))
            .when(pl.col('change_pct') > 1).then(pl.lit('A'))
            .when(pl.col('change_pct') > 0).then(pl.lit('B'))
            .otherwise(pl.lit('C')).alias('grade'),
            
            pl.when(pl.col('change_pct') > 0).then(60 + pl.col('change_pct') * 2)
            .otherwise(50 + pl.col('change_pct')).alias('enhanced_score'),
            
            pl.when(pl.col('change_pct') > 0).then(pl.lit('上涨趋势'))
            .otherwise(pl.lit('下跌趋势')).alias('reasons')
        ])
        
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score', 
                          'change_pct', 'trend', 'reasons']
        result = result.select(required_fields + ['trade_date'])
        
        return result
    
    def validate_data(self, df: pl.DataFrame):
        """验证数据完整性"""
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score', 
                          'change_pct', 'trend', 'reasons']
        missing = [f for f in required_fields if f not in df.columns]
        if missing:
            raise ValueError(f"缺少必需字段: {missing}")
    
    def load_key_levels(self) -> Optional[pl.DataFrame]:
        """加载关键位数据"""
        if not self.key_levels_path or not Path(self.key_levels_path).exists():
            self.logger.warning(f"关键位数据文件不存在: {self.key_levels_path}")
            return None
        
        try:
            df = pl.read_parquet(self.key_levels_path)
            
            key_cols = [
                'code', 'trade_date', 'price',
                'support_5d', 'support_20d', 'support_60d', 'support_strong',
                'resistance_5d', 'resistance_20d', 'resistance_60d', 'resistance_strong',
                'ma5', 'ma10', 'ma20', 'ma60',
                'bb_upper', 'bb_lower',
                'pivot', 'pivot_r1', 'pivot_s1',
                'support_change', 'resistance_change',
                'support_status', 'resistance_status'
            ]
            
            available_cols = [c for c in key_cols if c in df.columns]
            df = df.select(available_cols)
            
            self.logger.info(f"关键位数据加载成功: {len(df)} 条记录")
            return df
        except Exception as e:
            self.logger.warning(f"加载关键位数据失败: {e}")
            return None
    
    def merge_key_levels(self, df: pl.DataFrame, key_levels: pl.DataFrame) -> pl.DataFrame:
        """合并关键位数据到主数据"""
        if key_levels is None or len(key_levels) == 0:
            self.logger.info("无关键位数据可合并")
            return df
        
        try:
            merged = df.join(
                key_levels.drop(['trade_date', 'price'], strict=False),
                on='code',
                how='left'
            )
            self.logger.info(f"关键位数据合并完成: {len(merged)} 条记录")
            return merged
        except Exception as e:
            self.logger.warning(f"合并关键位数据失败: {e}")
            return df
    
    def load_cvd(self) -> Optional[pl.DataFrame]:
        """加载CVD数据"""
        if not self.cvd_path or not Path(self.cvd_path).exists():
            self.logger.warning(f"CVD数据文件不存在: {self.cvd_path}")
            return None
        
        try:
            df = pl.read_parquet(self.cvd_path)
            
            cvd_cols = [
                'code', 'trade_date', 'price', 'volume',
                'cvd_60d', 'cvd_signal', 'cvd_trend',
                'cvd_change_1d', 'cvd_change_5d', 'cvd_change_10d',
                'divergence_5d', 'divergence_10d'
            ]
            
            available_cols = [c for c in cvd_cols if c in df.columns]
            df = df.select(available_cols)
            
            self.logger.info(f"CVD数据加载成功: {len(df)} 条记录")
            return df
        except Exception as e:
            self.logger.warning(f"加载CVD数据失败: {e}")
            return None
    
    def merge_cvd(self, df: pl.DataFrame, cvd: pl.DataFrame) -> pl.DataFrame:
        """合并CVD数据到主数据"""
        if cvd is None or len(cvd) == 0:
            self.logger.info("无CVD数据可合并")
            return df
        
        try:
            merged = df.join(
                cvd.drop(['trade_date', 'price', 'volume'], strict=False),
                on='code',
                how='left'
            )
            self.logger.info(f"CVD数据合并完成: {len(merged)} 条记录")
            return merged
        except Exception as e:
            self.logger.warning(f"合并CVD数据失败: {e}")
            return df
    
    def analyze_key_levels_conclusion(self, row: dict) -> str:
        """分析关键位结论"""
        conclusions = []
        
        price = row.get('price', 0)
        support = row.get('support_strong', 0)
        resistance = row.get('resistance_strong', 0)
        ma20 = row.get('ma20', 0)
        ma60 = row.get('ma60', 0)
        support_status = row.get('support_status', '持平')
        resistance_status = row.get('resistance_status', '持平')
        
        if support and price <= support * 1.05:
            conclusions.append("接近强支撑位")
        if resistance and price >= resistance * 0.95:
            conclusions.append("接近强压力位")
        
        if support_status == '上移':
            conclusions.append("支撑位上移(强势)")
        elif support_status == '下移':
            conclusions.append("支撑位下移(弱势)")
        
        if ma20 and ma60:
            if ma20 > ma60 and price > ma20:
                conclusions.append("均线多头排列")
            elif ma20 < ma60 and price < ma20:
                conclusions.append("均线空头排列")
        
        return " | ".join(conclusions) if conclusions else "关键位正常"
    
    def analyze_cvd_conclusion(self, row: dict) -> str:
        """分析CVD结论"""
        conclusions = []
        
        cvd_signal = row.get('cvd_signal', 'neutral')
        cvd_trend = row.get('cvd_trend', 'neutral')
        divergence_5d = row.get('divergence_5d', 'no_divergence')
        cvd_change_5d = row.get('cvd_change_5d', 0)
        
        if cvd_signal == 'buy_dominant':
            conclusions.append("买方占优")
        elif cvd_signal == 'sell_dominant':
            conclusions.append("卖方占优")
        
        if cvd_trend == 'strong_buy':
            conclusions.append("强买趋势")
        elif cvd_trend == 'strong_sell':
            conclusions.append("强卖趋势")
        elif cvd_trend == 'weak_buy':
            conclusions.append("弱买趋势")
        elif cvd_trend == 'weak_sell':
            conclusions.append("弱卖趋势")
        
        if divergence_5d == 'top_divergence':
            conclusions.append("⚠️顶背离(卖压累积)")
        elif divergence_5d == 'bottom_divergence':
            conclusions.append("✅底背离(买压累积)")
        
        if cvd_change_5d and cvd_change_5d > 0:
            conclusions.append(f"CVD5日+{cvd_change_5d:.0f}")
        elif cvd_change_5d and cvd_change_5d < 0:
            conclusions.append(f"CVD5日{cvd_change_5d:.0f}")
        
        return " | ".join(conclusions) if conclusions else "CVD中性"


class BaseFilter(ABC):
    """筛选器基类"""
    
    @abstractmethod
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        """应用筛选条件"""
        pass


class SGradeFilter(BaseFilter):
    """S级股票筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        return (
            df.filter(pl.col('grade') == 'S')
            .filter(pl.col('enhanced_score') >= config['min_score'])
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )


class AGradeFilter(BaseFilter):
    """A级股票筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        return (
            df.filter(pl.col('grade') == 'A')
            .filter(pl.col('enhanced_score') >= config['min_score'])
            .filter(pl.col('enhanced_score') < config['max_score'])
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )


class BullishFilter(BaseFilter):
    """多头排列筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        return (
            df.filter(pl.col('trend') == config['trend'])
            .filter(pl.col('change_pct') > config['change_pct_min'])
            .filter(pl.col('change_pct') < config['change_pct_max'])
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )


class MACDVolumeFilter(BaseFilter):
    """MACD金叉+量价齐升筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        keywords = config.get('keywords', [])
        df_filtered = df
        for keyword in keywords:
            df_filtered = df_filtered.filter(
                pl.col('reasons').str.contains(keyword)
            )
        return (
            df_filtered
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )


class FilterEngine:
    """筛选引擎"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.filters = {
            's_grade': SGradeFilter(),
            'a_grade': AGradeFilter(),
            'bullish': BullishFilter(),
            'macd_volume': MACDVolumeFilter()
        }
        self.logger = logging.getLogger(__name__)
    
    def apply_all_filters(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """应用所有筛选器"""
        results = {}
        for filter_name, filter_obj in self.filters.items():
            try:
                config = self.config_manager.get_filter_config(filter_name)
                results[filter_name] = filter_obj.apply(df, config)
                self.logger.info(f"筛选器 {filter_name} 完成: {len(results[filter_name])} 条记录")
            except Exception as e:
                self.logger.warning(f"筛选器 {filter_name} 失败: {e}")
                results[filter_name] = pl.DataFrame()
        return results


class BaseReporter(ABC):
    """报告生成器基类"""
    
    @abstractmethod
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        """生成报告"""
        pass


class TextReporter(BaseReporter):
    """文本报告生成器"""
    
    def __init__(self, data_loader: 'DataLoader' = None):
        self.data_loader = data_loader
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        lines = []
        lines.append("=" * 70)
        lines.append("明日股票推荐 (基于技术分析)")
        lines.append("=" * 70)
        
        for filter_name, df in filter_results.items():
            if len(df) == 0:
                continue
            
            config = config_manager.get_filter_config(filter_name)
            lines.append(f"\n【{config['description']}】")
            
            for row in df.iter_rows(named=True):
                change = f"+{row['change_pct']}" if row['change_pct'] >= 0 else str(row['change_pct'])
                line = f"  {row['code']} {row['name']:8} {row['price']:7.2f}元 {change:>6}% 评分{row['enhanced_score']:.0f}"
                lines.append(line)
                
                if 'support_strong' in row and row['support_strong']:
                    support_status = row.get('support_status', '持平')
                    support_change = row.get('support_change', 0)
                    change_str = f"({support_change:+.1f}%)" if support_change else ""
                    lines.append(f"    支撑位: {row['support_strong']:.2f} [{support_status}]{change_str}")
                
                if 'resistance_strong' in row and row['resistance_strong']:
                    resistance_status = row.get('resistance_status', '持平')
                    resistance_change = row.get('resistance_change', 0)
                    change_str = f"({resistance_change:+.1f}%)" if resistance_change else ""
                    lines.append(f"    压力位: {row['resistance_strong']:.2f} [{resistance_status}]{change_str}")
                
                if 'ma20' in row and row['ma20']:
                    lines.append(f"    MA20: {row['ma20']:.2f}  MA60: {row.get('ma60', 0):.2f}")
                
                key_levels_conclusion = self._analyze_key_levels(row)
                if key_levels_conclusion:
                    lines.append(f"    📍关键位结论: {key_levels_conclusion}")
                
                cvd_conclusion = self._analyze_cvd(row)
                if cvd_conclusion:
                    lines.append(f"    📊CVD结论: {cvd_conclusion}")
                
                if 'reasons' in row and row['reasons']:
                    lines.append(f"    理由: {row['reasons'][:50]}...")
        
        lines.append("\n" + "=" * 70)
        lines.append("统计摘要")
        lines.append("=" * 70)
        lines.append(f"  S级: {stats['s_grade_count']} 只 (强烈推荐)")
        lines.append(f"  A级: {stats['a_grade_count']} 只 (建议关注)")
        lines.append(f"  多头排列: {stats['bullish_count']} 只")
        lines.append(f"  今日上涨: {stats['rising_count']} 只")
        
        lines.append("\n【风险提示】")
        lines.append("  以上分析基于技术指标，仅供参考，不构成投资建议。")
        lines.append("  股市有风险，投资需谨慎。")
        
        return "\n".join(lines)
    
    def _analyze_key_levels(self, row: dict) -> str:
        """分析关键位结论"""
        conclusions = []
        
        price = row.get('price', 0)
        support = row.get('support_strong', 0)
        resistance = row.get('resistance_strong', 0)
        ma20 = row.get('ma20', 0)
        ma60 = row.get('ma60', 0)
        support_status = row.get('support_status', '持平')
        
        if support and price <= support * 1.05:
            conclusions.append("接近强支撑")
        if resistance and price >= resistance * 0.95:
            conclusions.append("接近强压力")
        
        if support_status == '上移':
            conclusions.append("支撑上移(强)")
        elif support_status == '下移':
            conclusions.append("支撑下移(弱)")
        
        if ma20 and ma60:
            if ma20 > ma60 and price > ma20:
                conclusions.append("多头排列")
            elif ma20 < ma60 and price < ma20:
                conclusions.append("空头排列")
        
        return " | ".join(conclusions) if conclusions else ""
    
    def _analyze_cvd(self, row: dict) -> str:
        """分析CVD结论"""
        conclusions = []
        
        cvd_signal = row.get('cvd_signal', '')
        cvd_trend = row.get('cvd_trend', '')
        divergence_5d = row.get('divergence_5d', '')
        cvd_change_5d = row.get('cvd_change_5d', 0)
        
        if cvd_signal == 'buy_dominant':
            conclusions.append("买方占优")
        elif cvd_signal == 'sell_dominant':
            conclusions.append("卖方占优")
        
        if cvd_trend == 'strong_buy':
            conclusions.append("强买")
        elif cvd_trend == 'strong_sell':
            conclusions.append("强卖")
        
        if divergence_5d == 'top_divergence':
            conclusions.append("⚠️顶背离")
        elif divergence_5d == 'bottom_divergence':
            conclusions.append("✅底背离")
        
        if cvd_change_5d:
            conclusions.append(f"5日{cvd_change_5d:+.0f}")
        
        return " | ".join(conclusions) if conclusions else ""


class HTMLReporter(BaseReporter):
    """HTML报告生成器"""
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        html_parts = [
            '<!DOCTYPE html>',
            '<html lang="zh-CN">',
            '<head>',
            '    <meta charset="UTF-8">',
            '    <meta name="viewport" content="width=device-width, initial-scale=1.0">',
            '    <title>股票推荐报告</title>',
            '    <style>',
            '        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }',
            '        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }',
            '        h1 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }',
            '        .section { margin: 20px 0; padding: 15px; border-radius: 5px; }',
            '        .s-grade { background-color: #d4edda; border-left: 4px solid #28a745; }',
            '        .a-grade { background-color: #fff3cd; border-left: 4px solid #ffc107; }',
            '        .bullish { background-color: #cce5ff; border-left: 4px solid #007bff; }',
            '        .macd-volume { background-color: #f8d7da; border-left: 4px solid #dc3545; }',
            '        .stock { padding: 8px; margin: 5px 0; background: white; border-radius: 3px; }',
            '        .code { font-weight: bold; color: #007bff; }',
            '        .name { margin-left: 10px; }',
            '        .price { margin-left: 10px; color: #28a745; }',
            '        .change { margin-left: 10px; }',
            '        .positive { color: #28a745; }',
            '        .negative { color: #dc3545; }',
            '        .score { margin-left: 10px; font-weight: bold; }',
            '        .key-levels { margin-left: 20px; color: #666; font-size: 0.9em; padding: 5px; background: #f8f9fa; border-radius: 3px; }',
            '        .support { color: #28a745; }',
            '        .resistance { color: #dc3545; }',
            '        .ma { color: #6c757d; }',
            '        .reasons { margin-left: 20px; color: #666; font-size: 0.9em; }',
            '        .stats { background-color: #e9ecef; padding: 15px; border-radius: 5px; margin-top: 20px; }',
            '        .warning { background-color: #fff3cd; padding: 10px; border-radius: 5px; margin-top: 20px; }',
            '    </style>',
            '</head>',
            '<body>',
            '    <div class="container">',
            '        <h1>📈 明日股票推荐报告</h1>',
            '        <p>生成时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '</p>',
        ]
        
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
                
                key_level_parts = []
                if 'support_strong' in row and row['support_strong']:
                    support_status = row.get('support_status', '持平')
                    support_change = row.get('support_change', 0)
                    change_str = f"({support_change:+.1f}%)" if support_change else ""
                    key_level_parts.append(f'<span class="support">支撑: {row["support_strong"]:.2f} [{support_status}]{change_str}</span>')
                
                if 'resistance_strong' in row and row['resistance_strong']:
                    resistance_status = row.get('resistance_status', '持平')
                    resistance_change = row.get('resistance_change', 0)
                    change_str = f"({resistance_change:+.1f}%)" if resistance_change else ""
                    key_level_parts.append(f'<span class="resistance">压力: {row["resistance_strong"]:.2f} [{resistance_status}]{change_str}</span>')
                
                if 'ma20' in row and row['ma20']:
                    key_level_parts.append(f'<span class="ma">MA20: {row["ma20"]:.2f} MA60: {row.get("ma60", 0):.2f}</span>')
                
                if key_level_parts:
                    html_parts.append(f'                <div class="key-levels">{" | ".join(key_level_parts)}</div>')
                
                if 'reasons' in row and row['reasons']:
                    html_parts.append(f'                <div class="reasons">理由: {row["reasons"][:50]}...</div>')
                
                html_parts.append('            </div>')
            
            html_parts.append('        </div>')
        
        html_parts.append('        <div class="stats">')
        html_parts.append('            <h2>📊 统计摘要</h2>')
        html_parts.append(f'            <p>✅ S级: {stats["s_grade_count"]} 只 (强烈推荐)</p>')
        html_parts.append(f'            <p>✅ A级: {stats["a_grade_count"]} 只 (建议关注)</p>')
        html_parts.append(f'            <p>📈 多头排列: {stats["bullish_count"]} 只</p>')
        html_parts.append(f'            <p>⬆️  今日上涨: {stats["rising_count"]} 只</p>')
        html_parts.append('        </div>')
        
        html_parts.append('        <div class="warning">')
        html_parts.append('            <h3>⚠️ 风险提示</h3>')
        html_parts.append('            <p>以上分析基于技术指标，仅供参考，不构成投资建议。</p>')
        html_parts.append('            <p>股市有风险，投资需谨慎。</p>')
        html_parts.append('        </div>')
        
        html_parts.append('    </div>')
        html_parts.append('</body>')
        html_parts.append('</html>')
        
        return '\n'.join(html_parts)


class JSONReporter(BaseReporter):
    """JSON报告生成器"""
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        report = {
            'timestamp': datetime.now().isoformat(),
            'filters': {},
            'stats': stats
        }
        
        for filter_name, df in filter_results.items():
            config = config_manager.get_filter_config(filter_name)
            report['filters'][filter_name] = {
                'description': config.get('description', ''),
                'stocks': df.to_dicts()
            }
        
        return json.dumps(report, ensure_ascii=False, indent=2)


class EmailNotifier:
    """邮件通知器 - HTTP API 优先，SMTP 备用"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.email_service = EmailService()
    
    def send_report(self, subject: str, content: str, html_content: str = None):
        """发送报告邮件"""
        if not self.config.get('enabled', False):
            self.logger.info("邮件通知未启用")
            return False
        
        recipients = self.config.get('recipients', [])
        if not recipients:
            self.logger.error("未配置收件人")
            return False
        
        return self.email_service.send(
            to_addrs=recipients,
            subject=subject,
            content=content,
            html_content=html_content
        )


class MySQLStorage:
    """MySQL存储器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.engine = None
        self.table_prefix = config.get('table_prefix', 'xcn_')
        
        if not MYSQL_AVAILABLE:
            self.logger.warning("sqlalchemy 或 python-dotenv 未安装，MySQL存储功能不可用")
            return
        
        if not config.get('enabled', False):
            self.logger.info("MySQL存储未启用")
            return
        
        self._init_connection()
    
    def _init_connection(self):
        """初始化数据库连接"""
        try:
            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = os.getenv('DB_PORT', '3306')
            db_user = os.getenv('DB_USER', 'root')
            db_password = os.getenv('DB_PASSWORD', '')
            db_name = os.getenv('DB_NAME', 'quantdb')
            db_charset = os.getenv('DB_CHARSET', 'utf8mb4')
            
            connection_string = (
                f"mysql+pymysql://{db_user}:{db_password}"
                f"@{db_host}:{db_port}/{db_name}"
                f"?charset={db_charset}"
            )
            
            self.engine = create_engine(
                connection_string,
                pool_size=int(os.getenv('DB_POOL_SIZE', 10)),
                pool_recycle=int(os.getenv('DB_POOL_RECYCLE', 3600)),
                echo=False
            )
            
            self._init_tables()
            self.logger.info("MySQL连接初始化成功")
            
        except Exception as e:
            self.logger.error(f"MySQL连接初始化失败: {e}")
            self.engine = None
    
    def _init_tables(self):
        """初始化数据库表"""
        create_batch_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}daily_batch (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            batch_date DATE NOT NULL COMMENT '批次日期',
            batch_id VARCHAR(20) NOT NULL COMMENT '批次ID(YYYYMMDD)',
            status VARCHAR(10) DEFAULT 'active' COMMENT '状态(active/completed)',
            total_picks INT DEFAULT 0 COMMENT '推荐股票总数',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            UNIQUE KEY uk_batch_date (batch_date),
            UNIQUE KEY uk_batch_id (batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每日批次记录';
        """
        
        create_picks_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}daily_picks (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            batch_id VARCHAR(20) NOT NULL COMMENT '批次ID',
            pick_date DATE NOT NULL COMMENT '推荐日期',
            code VARCHAR(10) NOT NULL COMMENT '股票代码',
            name VARCHAR(50) COMMENT '股票名称',
            price DECIMAL(10,2) COMMENT '当前价格',
            change_pct DECIMAL(6,2) COMMENT '涨跌幅(%)',
            grade VARCHAR(1) COMMENT '评级(S/A/B/C)',
            enhanced_score DECIMAL(6,2) COMMENT '综合评分',
            filter_type VARCHAR(20) NOT NULL COMMENT '筛选类型',
            reasons TEXT COMMENT '推荐理由',
            support_strong DECIMAL(10,2) COMMENT '强支撑位',
            resistance_strong DECIMAL(10,2) COMMENT '强压力位',
            ma20 DECIMAL(10,2) COMMENT '20日均线',
            ma60 DECIMAL(10,2) COMMENT '60日均线',
            support_status VARCHAR(10) COMMENT '支撑位状态',
            resistance_status VARCHAR(10) COMMENT '压力位状态',
            support_change DECIMAL(6,2) COMMENT '支撑位变化(%)',
            resistance_change DECIMAL(6,2) COMMENT '压力位变化(%)',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            UNIQUE KEY uk_batch_code_filter (batch_id, code, filter_type),
            KEY idx_pick_date (pick_date),
            KEY idx_batch_id (batch_id),
            KEY idx_code (code),
            KEY idx_grade (grade),
            KEY idx_filter_type (filter_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每日股票推荐记录';
        """
        
        create_stats_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}daily_stats (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            batch_id VARCHAR(20) NOT NULL COMMENT '批次ID',
            stat_date DATE NOT NULL COMMENT '统计日期',
            total_stocks INT COMMENT '总股票数',
            s_grade_count INT COMMENT 'S级数量',
            a_grade_count INT COMMENT 'A级数量',
            bullish_count INT COMMENT '多头排列数量',
            rising_count INT COMMENT '上涨数量',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            UNIQUE KEY uk_stat_date (stat_date),
            KEY idx_batch_id (batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每日统计摘要';
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_batch_sql))
            conn.execute(text(create_picks_sql))
            conn.execute(text(create_stats_sql))
            conn.commit()
        
        self.logger.info("MySQL表初始化完成")
    
    def _get_or_create_batch(self, pick_date: str) -> str:
        """获取或创建当日批次ID"""
        batch_id = pick_date.replace('-', '')
        
        with self.engine.connect() as conn:
            existing = conn.execute(text(f"""
                SELECT batch_id FROM {self.table_prefix}daily_batch 
                WHERE batch_date = :pick_date
            """), {'pick_date': pick_date}).fetchone()
            
            if existing:
                self.logger.info(f"使用已存在批次: {existing[0]}")
                return existing[0]
            
            conn.execute(text(f"""
                INSERT INTO {self.table_prefix}daily_batch (batch_date, batch_id, status)
                VALUES (:pick_date, :batch_id, 'active')
            """), {'pick_date': pick_date, 'batch_id': batch_id})
            conn.commit()
            
            self.logger.info(f"创建新批次: {batch_id}")
            return batch_id
    
    def save_picks(self, filter_results: Dict[str, pl.DataFrame], 
                   stats: dict, pick_date: str):
        """保存推荐结果到MySQL"""
        if self.engine is None:
            self.logger.warning("MySQL连接不可用，跳过存储")
            return False
        
        try:
            batch_id = self._get_or_create_batch(pick_date)
            
            self._save_stats(stats, pick_date, batch_id)
            
            total_saved = 0
            for filter_type, df in filter_results.items():
                if len(df) == 0:
                    continue
                
                saved = self._save_filter_results(df, filter_type, pick_date, batch_id)
                total_saved += saved
            
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {self.table_prefix}daily_batch 
                    SET total_picks = :total_picks, status = 'completed'
                    WHERE batch_id = :batch_id
                """), {'total_picks': total_saved, 'batch_id': batch_id})
                conn.commit()
            
            self.logger.info(f"MySQL存储完成: 批次={batch_id}, 记录数={total_saved}")
            return True
            
        except Exception as e:
            self.logger.error(f"MySQL存储失败: {e}")
            return False
    
    def _save_stats(self, stats: dict, stat_date: str, batch_id: str):
        """保存统计信息"""
        insert_sql = f"""
        INSERT INTO {self.table_prefix}daily_stats 
            (batch_id, stat_date, total_stocks, s_grade_count, a_grade_count, bullish_count, rising_count)
        VALUES 
            (:batch_id, :stat_date, :total_stocks, :s_grade_count, :a_grade_count, :bullish_count, :rising_count)
        ON DUPLICATE KEY UPDATE
            batch_id = VALUES(batch_id),
            total_stocks = VALUES(total_stocks),
            s_grade_count = VALUES(s_grade_count),
            a_grade_count = VALUES(a_grade_count),
            bullish_count = VALUES(bullish_count),
            rising_count = VALUES(rising_count)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(insert_sql), {
                'batch_id': batch_id,
                'stat_date': stat_date,
                'total_stocks': stats.get('total_stocks', 0),
                's_grade_count': stats.get('s_grade_count', 0),
                'a_grade_count': stats.get('a_grade_count', 0),
                'bullish_count': stats.get('bullish_count', 0),
                'rising_count': stats.get('rising_count', 0)
            })
            conn.commit()
        
        self.logger.info(f"统计信息已保存: {stat_date}, 批次: {batch_id}")
    
    def _save_filter_results(self, df: pl.DataFrame, filter_type: str, pick_date: str, batch_id: str) -> int:
        """保存单个筛选器的结果"""
        records = df.to_dicts()
        saved_count = 0
        
        insert_sql = f"""
        INSERT INTO {self.table_prefix}daily_picks 
            (batch_id, pick_date, code, name, price, change_pct, grade, enhanced_score, 
             filter_type, reasons, support_strong, resistance_strong, ma20, ma60,
             support_status, resistance_status, support_change, resistance_change)
        VALUES 
            (:batch_id, :pick_date, :code, :name, :price, :change_pct, :grade, :enhanced_score,
             :filter_type, :reasons, :support_strong, :resistance_strong, :ma20, :ma60,
             :support_status, :resistance_status, :support_change, :resistance_change)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            price = VALUES(price),
            change_pct = VALUES(change_pct),
            grade = VALUES(grade),
            enhanced_score = VALUES(enhanced_score),
            reasons = VALUES(reasons),
            support_strong = VALUES(support_strong),
            resistance_strong = VALUES(resistance_strong),
            ma20 = VALUES(ma20),
            ma60 = VALUES(ma60),
            support_status = VALUES(support_status),
            resistance_status = VALUES(resistance_status),
            support_change = VALUES(support_change),
            resistance_change = VALUES(resistance_change)
        """
        
        with self.engine.connect() as conn:
            for row in records:
                try:
                    params = {
                        'batch_id': batch_id,
                        'pick_date': pick_date,
                        'code': row.get('code', ''),
                        'name': row.get('name', ''),
                        'price': float(row.get('price', 0)) if row.get('price') else None,
                        'change_pct': float(row.get('change_pct', 0)) if row.get('change_pct') else None,
                        'grade': row.get('grade', ''),
                        'enhanced_score': float(row.get('enhanced_score', 0)) if row.get('enhanced_score') else None,
                        'filter_type': filter_type,
                        'reasons': row.get('reasons', '')[:500] if row.get('reasons') else None,
                        'support_strong': float(row.get('support_strong', 0)) if row.get('support_strong') else None,
                        'resistance_strong': float(row.get('resistance_strong', 0)) if row.get('resistance_strong') else None,
                        'ma20': float(row.get('ma20', 0)) if row.get('ma20') else None,
                        'ma60': float(row.get('ma60', 0)) if row.get('ma60') else None,
                        'support_status': row.get('support_status', ''),
                        'resistance_status': row.get('resistance_status', ''),
                        'support_change': float(row.get('support_change', 0)) if row.get('support_change') else None,
                        'resistance_change': float(row.get('resistance_change', 0)) if row.get('resistance_change') else None,
                    }
                    conn.execute(text(insert_sql), params)
                    saved_count += 1
                except Exception as e:
                    self.logger.warning(f"保存记录失败 {row.get('code')}: {e}")
            
            conn.commit()
        
        self.logger.info(f"筛选器 {filter_type} 已保存 {saved_count} 条记录")
        return saved_count


class StockRecommender:
    """股票推荐系统"""
    
    def __init__(self, config_path: str):
        self.config_manager = ConfigManager(config_path)
        self.data_loader = DataLoader(
            self.config_manager.get_data_path(),
            kline_dir=self.config_manager.get_kline_dir(),
            stock_list_path=self.config_manager.get_stock_list_path(),
            key_levels_path=self.config_manager.get_key_levels_path(),
            cvd_path=self.config_manager.get_cvd_path()
        )
        self.filter_engine = FilterEngine(self.config_manager)
        self.reporters = {
            'text': TextReporter(self.data_loader),
            'html': HTMLReporter(),
            'json': JSONReporter()
        }
        self.email_notifier = EmailNotifier(self.config_manager.get_email_config())
        
        mysql_config = self.config_manager.config.get('mysql', {})
        self.mysql_storage = MySQLStorage(mysql_config) if mysql_config.get('enabled', False) else None
        
        validation_config = self.config_manager.config.get('recommendation', {}).get('data_validation', {})
        self.data_validator = DataValidator(validation_config) if validation_config.get('enabled', False) else None
        
        self.logger = self.setup_logger()
    
    def setup_logger(self) -> logging.Logger:
        """配置日志系统"""
        logger = logging.getLogger('StockRecommender')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # 文件处理器
            log_dir = Path('logs')
            log_dir.mkdir(exist_ok=True)
            fh = logging.FileHandler(log_dir / 'recommender.log')
            fh.setLevel(logging.INFO)
            
            # 控制台处理器
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            
            # 格式化器
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            
            logger.addHandler(fh)
            logger.addHandler(ch)
        
        return logger
    
    def _filter_suspended_stocks(self, df: pl.DataFrame) -> pl.DataFrame:
        """过滤停牌股票"""
        original_count = len(df)
        
        conditions = []
        
        if 'volume' in df.columns:
            conditions.append(pl.col('volume') > 0)
        
        if 'change_pct' in df.columns:
            conditions.append(pl.col('change_pct').is_not_null())
        
        if 'turnover' in df.columns:
            conditions.append(pl.col('turnover') > 0)
        
        if conditions:
            filter_expr = conditions[0]
            for cond in conditions[1:]:
                filter_expr = filter_expr & cond
            
            df = df.filter(filter_expr)
        
        suspended_count = original_count - len(df)
        if suspended_count > 0:
            self.logger.info(f"🚫 过滤停牌股票: {suspended_count} 只")
        
        return df
    
    def calculate_stats(self, df: pl.DataFrame) -> dict:
        """计算统计信息"""
        return {
            'total_stocks': len(df),
            's_grade_count': len(df.filter(pl.col('grade') == 'S')),
            'a_grade_count': len(df.filter(pl.col('grade') == 'A')),
            'bullish_count': len(df.filter(pl.col('trend') == 100)),
            'rising_count': len(df.filter(pl.col('change_pct') > 0))
        }
    
    def save_reports(self, reports: dict):
        """保存报告到文件"""
        output_dir = Path(self.config_manager.get_output_dir())
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d')
        prefix = self.config_manager.get_output_prefix()
        
        for format_name, content in reports.items():
            if format_name == 'text':
                filename = f"{prefix}_{timestamp}.txt"
            elif format_name == 'html':
                filename = f"{prefix}_{timestamp}.html"
            else:
                filename = f"{prefix}_{timestamp}.json"
            
            filepath = output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.logger.info(f"报告已保存: {filepath}")
    
    def run(self):
        """执行推荐流程"""
        try:
            self.logger.info("="*70)
            self.logger.info("开始股票推荐流程")
            self.logger.info("="*70)
            
            analysis_time = datetime.now()
            self.logger.info(f"📅 分析时间: {analysis_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            data_path = self.config_manager.get_data_path()
            self.logger.info(f"📁 数据路径: {data_path}")
            
            self.logger.info("检查数据是否需要更新...")
            self.data_loader.check_and_update_data()
            
            df = self.data_loader.load_data()
            
            key_levels = self.data_loader.load_key_levels()
            if key_levels is not None:
                df = self.data_loader.merge_key_levels(df, key_levels)
            
            cvd_data = self.data_loader.load_cvd()
            if cvd_data is not None:
                df = self.data_loader.merge_cvd(df, cvd_data)
            
            if 'trade_date' in df.columns:
                latest_date = df['trade_date'].max()
                self.logger.info(f"📊 数据最新日期: {latest_date}")
            elif 'update_time' in df.columns:
                latest_time = df['update_time'].max()
                self.logger.info(f"📊 数据更新时间: {latest_time}")
            else:
                self.logger.info(f"📊 数据记录数: {len(df)}")
            
            # 2. 数据检查（新增）
            if self.data_validator:
                self.logger.info("执行数据检查...")
                validation_results = self.data_validator.validate_all(df)
                
                if not validation_results['passed']:
                    self.logger.warning("数据检查发现问题:")
                    for check_name, result in validation_results.items():
                        if isinstance(result, dict) and not result.get('passed', True):
                            self.logger.warning(f"  {check_name}: {result}")
                    self.logger.warning("继续执行推荐流程（数据质量问题已记录）")
                else:
                    self.logger.info("数据检查通过 ✓")
            
            # 2.5 过滤停牌股票
            df = self._filter_suspended_stocks(df)
            
            # 3. 应用筛选器
            filter_results = self.filter_engine.apply_all_filters(df)
            
            # 3. 计算统计信息
            stats = self.calculate_stats(df)
            
            # 4. 生成报告
            reports = {}
            for format_name in self.config_manager.get_output_formats():
                if format_name in self.reporters:
                    reporter = self.reporters[format_name]
                    reports[format_name] = reporter.generate(
                        filter_results, stats, self.config_manager
                    )
            
            # 5. 保存报告
            if self.config_manager.config['recommendation']['output']['save_to_file']:
                self.save_reports(reports)
            
            # 6. 保存到MySQL
            if self.mysql_storage:
                pick_date = analysis_time.strftime('%Y-%m-%d')
                self.mysql_storage.save_picks(filter_results, stats, pick_date)
            
            # 7. 发送邮件
            email_config = self.config_manager.get_email_config()
            self.logger.info(f"邮件配置: enabled={email_config.get('enabled', False)}")
            if email_config.get('enabled', False):
                subject = f"{email_config['subject_prefix']} - {datetime.now().strftime('%Y-%m-%d')}"
                self.logger.info(f"准备发送邮件: {subject}")
                result = self.email_notifier.send_report(
                    subject=subject,
                    content=reports.get('text', ''),
                    html_content=reports.get('html')
                )
                self.logger.info(f"邮件发送结果: {result}")
            
            # 8. 输出到控制台
            if 'text' in reports:
                print(reports['text'])
            
            self.logger.info("="*70)
            self.logger.info("股票推荐流程完成")
            self.logger.info("="*70)
            
        except Exception as e:
            self.logger.error(f"推荐流程失败: {e}")
            raise


@check_data_freshness
def main():
    """主函数"""
    PROJECT_ROOT = Path(__file__).parent.parent
    CONFIG_FILE = PROJECT_ROOT / "config" / "xcn_comm.yaml"
    
    recommender = StockRecommender(str(CONFIG_FILE))
    recommender.run()


if __name__ == '__main__':
    main()
