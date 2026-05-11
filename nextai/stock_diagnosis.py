#!/usr/bin/env python3
"""
个股诊断 CLI 工具
基于 Gemini AI 的全面诊断分析，每日自动存入 MySQL

用法:
    python stock_diagnosis.py                    # 全量诊断
    python stock_diagnosis.py 000001             # 单股票诊断
    python stock_diagnosis.py --limit 100        # 限制数量
    python stock_diagnosis.py --date 2026-05-10  # 指定日期
"""
import sys
import os
import json
import logging
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

import numpy as np
import pandas as pd
import pymysql
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/stock_diagnosis.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyDSpbFu_5ez6PoTpOtOyR_WZI42HyFe5M0")
GEMINI_API_BASE_URL = os.getenv("GEMINI_API_BASE_URL", "https://generativelanguage.googleapis.com/v1beta")
GEMINI_MODEL = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")
GEMINI_TIMEOUT = 60
GEMINI_MAX_TOKENS = 8192

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "49.233.10.199"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USER", "nextai"),
    "password": os.getenv("MYSQL_PASSWORD", "100200"),
    "database": os.getenv("MYSQL_DATABASE", "xcn_db"),
    "charset": "utf8mb4",
}

DATA_DIR = Path(__file__).parent.parent / "data"
KLINE_DIR = DATA_DIR / "kline"
PREDICTIONS_DIR = DATA_DIR / "predictions"
PREDICTIONS_DIR.mkdir(exist_ok=True)


@dataclass
class StockInfo:
    code: str
    name: str


@dataclass
class TechIndicators:
    rsi_14: float = 50.0
    macd: float = 0.0
    macd_signal: float = 0.0
    macd_hist: float = 0.0
    kama_10: float = 0.0
    boll_upper: float = 0.0
    boll_middle: float = 0.0
    boll_lower: float = 0.0
    atr_14: float = 0.0
    ema5: float = 0.0
    ema10: float = 0.0
    ema20: float = 0.0
    ema60: float = 0.0
    volume_ratio: float = 1.0
    change_pct: float = 0.0


@dataclass
class DiagnosisScores:
    valuation_score: int = 50
    valuation_label: str = "适中"
    technical_score: int = 50
    technical_label: str = "健康"
    fund_score: int = 45
    fund_label: str = "正常"
    fundamental_score: int = 40
    fundamental_label: str = "一般"
    macro_score: int = 50
    macro_label: str = "中性"
    sentiment_score: int = 50
    sentiment_label: str = "中性"
    catalyst_score: int = 50
    catalyst_label: str = "中性"
    risk_score: int = 50
    risk_level: str = "medium"
    confidence_score: int = 50
    confidence_level: str = "中"


@dataclass
class DiagnosisResult:
    code: str
    name: str
    diagnosis_time: datetime
    current_price: float
    scores: DiagnosisScores
    indicators: TechIndicators
    support_price: float = 0.0
    resistance_price: float = 0.0
    stop_loss_price: float = 0.0
    take_profit_price: float = 0.0
    risk_items: List[str] = field(default_factory=list)
    signal_items: List[str] = field(default_factory=list)
    ai_summary: str = ""
    ai_suggestion: str = ""
    macro_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "diagnosis_time": self.diagnosis_time.isoformat(),
            "current_price": round(self.current_price, 2),
            "scores": asdict(self.scores),
            "indicators": asdict(self.indicators),
            "support_price": round(self.support_price, 2),
            "resistance_price": round(self.resistance_price, 2),
            "stop_loss_price": round(self.stop_loss_price, 2),
            "take_profit_price": round(self.take_profit_price, 2),
            "risk_items": self.risk_items,
            "signal_items": self.signal_items,
            "ai_summary": self.ai_summary,
            "ai_suggestion": self.ai_suggestion,
            "macro_data": self.macro_data,
        }


class DatabaseManager:
    def __init__(self):
        self.config = MYSQL_CONFIG

    def get_connection(self):
        return pymysql.connect(**self.config)

    def _map_confidence_level(self, label: str) -> str:
        mapping = {
            "高": "high", "中": "medium", "低": "low", "投机": "speculative",
            "high": "high", "medium": "medium", "low": "low", "speculative": "speculative"
        }
        return mapping.get(label, "medium")

    def _map_risk_level(self, score: int) -> str:
        if score >= 80: return "extreme_high"
        elif score >= 60: return "high"
        elif score >= 40: return "medium"
        elif score >= 20: return "low"
        else: return "extreme_low"

    def save_diagnosis(self, result: DiagnosisResult) -> bool:
        cols = [
            'report_no', 'code', 'name', 'diagnosis_time', 'current_price',
            'risk_score', 'risk_level', 'confidence_score', 'confidence_level',
            'valuation_score', 'valuation_label', 'technical_score', 'technical_label',
            'fund_score', 'fund_label', 'fundamental_score', 'fundamental_label',
            'macro_score', 'macro_label', 'sentiment_score', 'sentiment_label',
            'catalyst_score', 'catalyst_label',
            'rsi_14', 'macd', 'macd_signal', 'kama_10', 'boll_upper', 'boll_middle', 'boll_lower', 'atr_14',
            'support_price', 'resistance_price', 'stop_loss_price', 'take_profit_price',
            'advice_action', 'advice_message', 'position_suggestion',
            'risk_count', 'signal_count', 'summary_text', 'calculation_time_ms',
            'data_source_version', 'cache_hit', 'created_at', 'updated_at',
            'dimensions_detail', 'key_levels',
            'volume_ratio', 'pivot_point', 'vwap', 'fibonacci_618', 'fibonacci_382'
        ]
        placeholders = ', '.join(['%s'] * len(cols))
        sql = f"INSERT INTO diag_report ({', '.join(cols)}) VALUES ({placeholders})"

        report_no = f"DIAG_{result.code}_{result.diagnosis_time.strftime('%Y%m%d%H%M%S')}"
        action = "买入" if "买" in result.ai_suggestion else "持有" if "持有" in result.ai_suggestion else "观望"
        now = result.diagnosis_time.strftime('%Y-%m-%d %H:%M:%S')
        risk_level = self._map_risk_level(result.scores.risk_score)
        confidence_level = self._map_confidence_level(result.scores.confidence_level)

        values = (
            report_no, result.code, result.name, now, result.current_price,
            result.scores.risk_score, risk_level,
            result.scores.confidence_score, confidence_level,
            result.scores.valuation_score, result.scores.valuation_label,
            result.scores.technical_score, result.scores.technical_label,
            result.scores.fund_score, result.scores.fund_label,
            result.scores.fundamental_score, result.scores.fundamental_label,
            result.scores.macro_score, result.scores.macro_label,
            result.scores.sentiment_score, result.scores.sentiment_label,
            result.scores.catalyst_score, result.scores.catalyst_label,
            result.indicators.rsi_14, result.indicators.macd,
            result.indicators.macd_signal, result.indicators.kama_10,
            result.indicators.boll_upper, result.indicators.boll_middle,
            result.indicators.boll_lower, result.indicators.atr_14,
            result.support_price, result.resistance_price,
            result.stop_loss_price, result.take_profit_price,
            action,
            result.ai_summary[:500] if result.ai_summary else "",
            "稳健型",
            len(result.risk_items), len(result.signal_items),
            result.ai_summary[:2000] if result.ai_summary else "",
            0,
            "v1.0", 0, now, now,
            "{}", "{}",
            result.indicators.volume_ratio,
            0.0, 0.0, 0.0, 0.0
        )
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, values)
                conn.commit()
            logger.info(f"[DB] 保存成功: {result.code} {result.name}")
            return True
        except Exception as e:
            logger.error(f"[DB] 保存失败: {result.code} - {e}")
            return False


class GeminiClient:
    def __init__(self):
        self.api_key = GEMINI_API_KEY
        self.base_url = f"{GEMINI_API_BASE_URL}/models/{GEMINI_MODEL}:generateContent"

    def analyze(self, result: DiagnosisResult, macro_text: str = "") -> Tuple[str, str]:
        if not self.api_key:
            return self._mock_analysis(result)

        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": self.api_key
        }

        system_prompt = """你是一位专业的股票分析师，拥有10年以上的投资研究经验。你的职责是对股票进行全面的AI智能诊断分析。

分析维度包括：
1. 估值维度 (Valuation) - PE、PB等估值指标
2. 技术维度 (Technical) - 趋势、成交量、技术指标
3. 资金维度 (Fund) - 资金流向、换手率
4. 基本面维度 (Fundamental) - 业绩、行业地位、竞争力
5. 宏观维度 (Macro) - 政策环境、市场情绪
6. 情绪维度 (Sentiment) - 市场情绪、机构观点
7. 催化剂维度 (Catalyst) - 利好/利空因素

分析要求：
1. 简洁专业，用数据说话
2. 给出明确的投资建议（买入/持有/卖出）
3. 识别主要风险点和机会点
4. 提供目标价格参考
5. 控制在500字以内

输出格式（请严格遵循）：
[诊断结论]
[详细分析]
[投资建议]"""

        user_prompt = f"""请分析以下股票：

**股票信息**
- 代码：{result.code}
- 名称：{result.name}
- 当前价格：{result.current_price}元（今日涨跌幅：{result.indicators.change_pct:.2f}%）

**宏观环境**
{macro_text}

**七维度评分（0-100分）**
- 估值：{result.scores.valuation_score}分 ({result.scores.valuation_label})
- 技术：{result.scores.technical_score}分 ({result.scores.technical_label})
- 资金：{result.scores.fund_score}分 ({result.scores.fund_label})
- 基本面：{result.scores.fundamental_score}分 ({result.scores.fundamental_label})
- 宏观：{result.scores.macro_score}分 ({result.scores.macro_label})
- 情绪：{result.scores.sentiment_score}分 ({result.scores.sentiment_label})
- 催化剂：{result.scores.catalyst_score}分 ({result.scores.catalyst_label})

**技术指标**
- RSI(14)：{result.indicators.rsi_14:.2f}
- MACD：DIF={result.indicators.macd:.4f}，DEA={result.indicators.macd_signal:.4f}
- KAMA(10)：{result.indicators.kama_10:.2f}
- ATR(14)：{result.indicators.atr_14:.2f}
- 布林上轨：{result.indicators.boll_upper:.2f} | 中轨：{result.indicators.boll_middle:.2f} | 下轨：{result.indicators.boll_lower:.2f}

**关键价位**
- 阻力位：{result.resistance_price:.2f}元
- 支撑位：{result.support_price:.2f}元
- 止损位：{result.stop_loss_price:.2f}元
- 止盈位：{result.take_profit_price:.2f}元

请基于以上数据，给出专业的AI诊断分析。"""

        payload = {
            "contents": [{"parts": [
                {"text": f"[系统提示]\n{system_prompt}"},
                {"text": f"[用户]\n{user_prompt}"}
            ]}],
            "generationConfig": {
                "maxOutputTokens": GEMINI_MAX_TOKENS,
                "temperature": 0.7,
            }
        }

        try:
            with httpx.Client(timeout=GEMINI_TIMEOUT, trust_env=False) as client:
                response = client.post(self.base_url, json=payload, headers=headers)
                response.raise_for_status()
                result_json = response.json()
                content = result_json["candidates"][0]["content"]["parts"][0]["text"]
                return self._parse_ai_response(content)
        except Exception as e:
            logger.warning(f"[Gemini] API调用失败: {e}，使用模拟分析")
            return self._mock_analysis(result)

    def _parse_ai_response(self, content: str) -> Tuple[str, str]:
        lines = content.split('\n')
        summary_parts = []
        suggestion_parts = []
        in_suggestion = False

        for line in lines:
            line = line.strip()
            if '[投资建议]' in line or '投资建议' in line:
                in_suggestion = True
                continue
            if in_suggestion:
                suggestion_parts.append(line)
            elif '[诊断结论]' in line or '[详细分析]' in line or line.startswith('**'):
                continue
            else:
                summary_parts.append(line)

        summary = ' '.join(summary_parts[:5])
        suggestion = ' '.join(suggestion_parts[:3])

        if not summary:
            summary = content[:300]
        if not suggestion:
            suggestion = "建议持有观察"

        return summary, suggestion

    def _mock_analysis(self, result: DiagnosisResult) -> Tuple[str, str]:
        avg_score = (
            result.scores.valuation_score +
            result.scores.technical_score +
            result.scores.fund_score +
            result.scores.fundamental_score
        ) / 4

        if avg_score >= 70:
            summary = f"{result.name}({result.code})技术面强势，多头趋势明显。估值{result.scores.valuation_label}，基本面{result.scores.fundamental_label}。MACD零轴上方运行，RSI处于{('超买' if result.indicators.rsi_14 > 70 else '强势区域' if result.indicators.rsi_14 > 50 else '中性')}。"
            suggestion = "建议逢低买入"
        elif avg_score >= 50:
            summary = f"{result.name}({result.code})整体平稳，趋势中性。技术面{result.scores.technical_label}，资金面{result.scores.fund_label}。震荡整理中，等待方向选择。"
            suggestion = "建议持有观望"
        else:
            summary = f"{result.name}({result.code})面临一定压力，技术面偏弱。估值{result.scores.valuation_label}，注意控制风险。"
            suggestion = "建议谨慎持有或减仓"

        return summary, suggestion


class TechCalculator:
    @staticmethod
    def calculate_ema(prices: np.ndarray, period: int) -> float:
        if len(prices) < period:
            return float(prices[-1]) if len(prices) > 0 else 0.0
        ema = prices[0]
        multiplier = 2 / (period + 1)
        for price in prices[1:]:
            ema = price * multiplier + ema * (1 - multiplier)
        return float(ema)

    @staticmethod
    def calculate_macd(prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float, float]:
        if len(prices) < slow:
            return 0.0, 0.0, 0.0

        ema_fast = prices[0]
        ema_slow = prices[0]
        multiplier_fast = 2 / (fast + 1)
        multiplier_slow = 2 / (slow + 1)

        for price in prices[1:]:
            ema_fast = price * multiplier_fast + ema_fast * (1 - multiplier_fast)
            ema_slow = price * multiplier_slow + ema_slow * (1 - multiplier_slow)

        dif = ema_fast - ema_slow

        ema_signal = dif
        multiplier_signal = 2 / (signal + 1)
        for _ in range(signal - 1):
            ema_signal = dif * multiplier_signal + ema_signal * (1 - multiplier_signal)

        dea = ema_signal
        hist = (dif - dea) * 2

        return float(dif), float(dea), float(hist)

    @staticmethod
    def calculate_rsi(prices: np.ndarray, period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0

        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)

    @staticmethod
    def calculate_boll(prices: np.ndarray, period: int = 20, std_dev: float = 2.0) -> Tuple[float, float, float]:
        if len(prices) < period:
            return prices[-1], prices[-1], prices[-1] if len(prices) > 0 else (0, 0, 0)

        recent = prices[-period:]
        middle = np.mean(recent)
        std = np.std(recent)
        upper = middle + std_dev * std
        lower = middle - std_dev * std
        return float(upper), float(middle), float(lower)

    @staticmethod
    def calculate_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
        if len(highs) < period + 1:
            return 0.0

        high_low = highs[1:] - lows[1:]
        high_close = np.abs(highs[1:] - closes[:-1])
        low_close = np.abs(lows[1:] - closes[:-1])

        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        atr = np.mean(true_range[-period:])
        return float(atr)

    @staticmethod
    def calculate_kama(prices: np.ndarray, period: int = 10) -> float:
        if len(prices) < period * 2:
            return float(prices[-1]) if len(prices) > 0 else 0.0

        changes = np.abs(np.diff(prices))
        volatility = np.sum(changes[-period * 2:])
        if volatility == 0:
            return float(prices[-1])

        er = changes[-period] / volatility
        fast, slow = 2, 30
        sc = (er * (2 / (fast + 1) - 2 / (slow + 1)) + 2 / (slow + 1)) ** 2

        kama = prices[-1]
        for i in range(period, len(prices)):
            kama = sc * prices[i] + (1 - sc) * kama
        return float(kama)


class StockDiagnosis:
    def __init__(self):
        self.db = DatabaseManager()
        self.gemini = GeminiClient()
        self.tech = TechCalculator()

    def load_kline_data(self, code: str) -> Optional[pd.DataFrame]:
        kline_file = KLINE_DIR / f"{code}.parquet"
        if not kline_file.exists():
            logger.warning(f"[数据] K线文件不存在: {kline_file}")
            return None

        try:
            pdf = pd.read_parquet(str(kline_file))
            if 'date' in pdf.columns:
                pdf = pdf.rename(columns={'date': 'trade_date'})
            if 'trade_date' in pdf.columns:
                pdf['trade_date'] = pd.to_datetime(pdf['trade_date'])
                pdf = pdf.sort_values('trade_date')
            return pdf
        except Exception as e:
            logger.error(f"[数据] 读取K线失败: {code} - {e}")
            return None

    def calculate_indicators(self, kline: pd.DataFrame) -> TechIndicators:
        closes = kline['close'].values.astype(float)
        highs = kline['high'].values.astype(float)
        lows = kline['low'].values.astype(float)
        volumes = kline['volume'].values.astype(float)

        current_price = float(closes[-1])
        prev_price = float(closes[-2]) if len(closes) > 1 else current_price
        change_pct = ((current_price - prev_price) / prev_price * 100) if prev_price > 0 else 0.0

        rsi = self.tech.calculate_rsi(closes)
        macd_dif, macd_dea, macd_hist = self.tech.calculate_macd(closes)
        boll_upper, boll_middle, boll_lower = self.tech.calculate_boll(closes)
        atr = self.tech.calculate_atr(highs, lows, closes)
        kama = self.tech.calculate_kama(closes)

        ema5 = self.tech.calculate_ema(closes, 5)
        ema10 = self.tech.calculate_ema(closes, 10)
        ema20 = self.tech.calculate_ema(closes, 20)
        ema60 = self.tech.calculate_ema(closes, 60) if len(closes) >= 60 else ema20

        vol_ma5 = np.mean(volumes[-5:]) if len(volumes) >= 5 else np.mean(volumes)
        vol_ma20 = np.mean(volumes[-20:]) if len(volumes) >= 20 else np.mean(volumes)
        volume_ratio = float(vol_ma5 / vol_ma20) if vol_ma20 > 0 else 1.0

        return TechIndicators(
            rsi_14=rsi,
            macd=macd_dif,
            macd_signal=macd_dea,
            macd_hist=macd_hist,
            kama_10=kama,
            boll_upper=boll_upper,
            boll_middle=boll_middle,
            boll_lower=boll_lower,
            atr_14=atr,
            ema5=ema5,
            ema10=ema10,
            ema20=ema20,
            ema60=ema60,
            volume_ratio=volume_ratio,
            change_pct=change_pct
        )

    def calculate_scores(self, indicators: TechIndicators, kline: pd.DataFrame) -> DiagnosisScores:
        prices = kline['close'].values.astype(float)
        current_price = float(prices[-1])

        rsi_score = int(max(0, min(100, 100 - abs(indicators.rsi_14 - 50) * 2)))
        rsi_label = "超买" if indicators.rsi_14 > 70 else "超卖" if indicators.rsi_14 < 30 else "正常"

        if indicators.macd > 0 and indicators.macd_signal > 0:
            tech_score = min(100, 60 + int(indicators.macd * 100))
            tech_label = "强势"
        elif indicators.macd > 0:
            tech_score = min(100, 50 + int(indicators.macd * 100))
            tech_label = "偏强"
        elif indicators.macd < 0 and indicators.macd_signal < 0:
            tech_score = max(0, 40 + int(indicators.macd * 100))
            tech_label = "弱势"
        else:
            tech_score = 50
            tech_label = "中性"

        if indicators.ema5 > indicators.ema10 > indicators.ema20:
            tech_score = min(100, tech_score + 15)
            tech_label = "多头排列"
        elif indicators.ema5 < indicators.ema10 < indicators.ema20:
            tech_score = max(0, tech_score - 15)
            tech_label = "空头排列"

        vol_score = int(min(100, indicators.volume_ratio * 40))
        fund_score = min(100, vol_score)

        if current_price > indicators.boll_upper:
            risk_score = 75
            risk_level = "high"
        elif current_price < indicators.boll_lower:
            risk_score = 60
            risk_level = "medium"
        else:
            risk_score = 45
            risk_level = "medium"

        confidence = int((rsi_score + fund_score) / 2)

        return DiagnosisScores(
            valuation_score=50,
            valuation_label="适中",
            technical_score=tech_score,
            technical_label=tech_label,
            fund_score=fund_score,
            fund_label="正常",
            fundamental_score=50,
            fundamental_label="一般",
            macro_score=50,
            macro_label="中性",
            sentiment_score=rsi_score,
            sentiment_label=rsi_label,
            catalyst_score=50,
            catalyst_label="中性",
            risk_score=risk_score,
            risk_level=risk_level,
            confidence_score=confidence,
            confidence_level="中"
        )

    def calculate_key_levels(self, indicators: TechIndicators, current_price: float) -> Tuple[float, float, float, float]:
        support = indicators.boll_lower
        resistance = indicators.boll_upper
        stop_loss = current_price * (1 - indicators.atr_14 / current_price * 2)
        take_profit = current_price * (1 + indicators.atr_14 / current_price * 3)

        if indicators.change_pct > 5:
            stop_loss = indicators.boll_middle

        return round(support, 2), round(resistance, 2), round(stop_loss, 2), round(take_profit, 2)

    def load_macro_data(self) -> Dict[str, Any]:
        macro_file = DATA_DIR / "foreign_index.json"
        if not macro_file.exists():
            return {}

        try:
            with open(macro_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.warning(f"[数据] 宏观数据读取失败: {e}")
            return {}

    def build_macro_text(self, macro_data: Dict[str, Any]) -> str:
        if not macro_data:
            return "今日宏观数据暂无"

        lines = ["【外围市场】"]
        us = macro_data.get("us_index", {}).get("data", {})
        if us:
            nasdaq = us.get("nasdaq", {})
            if nasdaq:
                lines.append(f"- 纳斯达克: {nasdaq.get('price', 'N/A')} ({nasdaq.get('change_pct', 0):+.2f}%)")

        asia = macro_data.get("asia_index", {}).get("data", {})
        if asia:
            hk = asia.get("hang_seng", {})
            if hk:
                lines.append(f"- 恒生指数: {hk.get('price', 'N/A')} ({hk.get('change_pct', 0):+.2f}%)")

        commodity = macro_data.get("commodity", {}).get("data", {})
        if commodity:
            gold = commodity.get("gold", {})
            if gold:
                lines.append(f"- 黄金: ${gold.get('price', 'N/A')}/盎司 ({gold.get('change_pct', 0):+.2f}%)")

        return "\n".join(lines) if len(lines) > 1 else "外围市场数据暂无"

    def diagnose(self, code: str, name: str = "") -> Optional[DiagnosisResult]:
        logger.info(f"[诊断] 开始诊断: {code} {name}")

        kline = self.load_kline_data(code)
        if kline is None or len(kline) < 60:
            logger.warning(f"[诊断] 数据不足，跳过: {code}")
            return None

        indicators = self.calculate_indicators(kline)
        scores = self.calculate_scores(indicators, kline)
        current_price = float(kline['close'].values[-1])

        if not name:
            name = str(code)

        support, resistance, stop_loss, take_profit = self.calculate_key_levels(indicators, current_price)

        macro_data = self.load_macro_data()
        macro_text = self.build_macro_text(macro_data)

        ai_summary, ai_suggestion = self.gemini.analyze(
            DiagnosisResult(
                code=code, name=name, diagnosis_time=datetime.now(),
                current_price=current_price, scores=scores,
                indicators=indicators, support_price=support,
                resistance_price=resistance, stop_loss_price=stop_loss,
                take_profit_price=take_profit, macro_data=macro_data
            ),
            macro_text
        )

        risk_items = []
        signal_items = []

        if indicators.rsi_14 > 75:
            risk_items.append("RSI超买")
        elif indicators.rsi_14 < 25:
            signal_items.append("RSI超卖反弹机会")

        if indicators.macd_hist < 0 and indicators.macd_hist < indicators.macd_signal * 0.8:
            risk_items.append("MACD死叉")
        elif indicators.macd_hist > 0 and indicators.macd_hist > indicators.macd_signal * 1.2:
            signal_items.append("MACD金叉")

        if current_price < indicators.boll_lower:
            signal_items.append("价格触及布林下轨支撑")
        elif current_price > indicators.boll_upper:
            risk_items.append("价格触及布林上轨压力")

        if not name:
            name = str(code)

        result = DiagnosisResult(
            code=code,
            name=name,
            diagnosis_time=datetime.now(),
            current_price=current_price,
            scores=scores,
            indicators=indicators,
            support_price=support,
            resistance_price=resistance,
            stop_loss_price=stop_loss,
            take_profit_price=take_profit,
            risk_items=risk_items,
            signal_items=signal_items,
            ai_summary=ai_summary,
            ai_suggestion=ai_suggestion,
            macro_data=macro_data
        )

        logger.info(f"[诊断] 完成: {code} - 风险:{len(risk_items)}项, 信号:{len(signal_items)}项")
        return result

    def save_result(self, result: DiagnosisResult) -> bool:
        return self.db.save_diagnosis(result)

    def save_json(self, result: DiagnosisResult):
        output_file = PREDICTIONS_DIR / f"diagnosis_{date.today().isoformat()}_{result.code}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        logger.info(f"[输出] JSON: {output_file}")

    def print_result(self, result: DiagnosisResult):
        print("\n" + "=" * 60)
        print(f"  {result.name}({result.code}) 个股诊断报告")
        print("=" * 60)
        print(f"  诊断时间: {result.diagnosis_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  当前价格: {result.current_price:.2f} 元 ({result.indicators.change_pct:+.2f}%)")
        print("-" * 60)
        print("  【七维度评分】")
        print(f"    估值: {result.scores.valuation_score}分 ({result.scores.valuation_label})")
        print(f"    技术: {result.scores.technical_score}分 ({result.scores.technical_label})")
        print(f"    资金: {result.scores.fund_score}分 ({result.scores.fund_label})")
        print(f"    基本面: {result.scores.fundamental_score}分 ({result.scores.fundamental_label})")
        print(f"    宏观: {result.scores.macro_score}分 ({result.scores.macro_label})")
        print(f"    情绪: {result.scores.sentiment_score}分 ({result.scores.sentiment_label})")
        print(f"    催化剂: {result.scores.catalyst_score}分 ({result.scores.catalyst_label})")
        print("-" * 60)
        print("  【技术指标】")
        print(f"    RSI(14): {result.indicators.rsi_14:.2f}")
        print(f"    MACD: DIF={result.indicators.macd:.4f} DEA={result.indicators.macd_signal:.4f} HIST={result.indicators.macd_hist:.4f}")
        print(f"    KAMA: {result.indicators.kama_10:.2f}")
        print(f"    ATR(14): {result.indicators.atr_14:.2f}")
        print(f"    布林带: {result.indicators.boll_upper:.2f} / {result.indicators.boll_middle:.2f} / {result.indicators.boll_lower:.2f}")
        print("-" * 60)
        print("  【关键价位】")
        print(f"    阻力位: {result.resistance_price:.2f} 元")
        print(f"    支撑位: {result.support_price:.2f} 元")
        print(f"    止损位: {result.stop_loss_price:.2f} 元")
        print(f"    止盈位: {result.take_profit_price:.2f} 元")
        if result.risk_items:
            print("-" * 60)
            print("  【风险提示】")
            for item in result.risk_items:
                print(f"    ⚠️ {item}")
        if result.signal_items:
            print("-" * 60)
            print("  【信号机会】")
            for item in result.signal_items:
                print(f"    ✅ {item}")
        print("-" * 60)
        print(f"  【AI 诊断结论】")
        print(f"    {result.ai_summary[:200]}...")
        print(f"  【操作建议】: {result.ai_suggestion}")
        print("=" * 60 + "\n")


def load_stock_list(limit: Optional[int] = None) -> List[StockInfo]:
    stock_list_file = DATA_DIR / "stock_list.parquet"
    if not stock_list_file.exists():
        stock_list_file = DATA_DIR / "stock_list.csv"

    try:
        if stock_list_file.suffix == '.csv':
            df = pd.read_csv(stock_list_file)
        else:
            df = pd.read_parquet(str(stock_list_file))

        stocks = []
        for _, row in df.iterrows():
            code = str(row.get('code', row.get('股票代码', '')))
            name = str(row.get('name', row.get('股票名称', code)))
            if code and len(code) == 6 and code.isdigit():
                stocks.append(StockInfo(code=code, name=name))
            if limit and len(stocks) >= limit:
                break

        return stocks
    except Exception as e:
        logger.error(f"[数据] 股票列表加载失败: {e}")
        return []


def main():
    import argparse
    parser = argparse.ArgumentParser(description='个股诊断 CLI 工具')
    parser.add_argument('code', nargs='?', help='股票代码 (如 000001)')
    parser.add_argument('--limit', type=int, default=None, help='限制处理股票数量')
    parser.add_argument('--date', help='诊断日期 (YYYY-MM-DD)')
    parser.add_argument('--skip-save', action='store_true', help='跳过数据库保存')
    parser.add_argument('--workers', type=int, default=3, help='并发数')
    args = parser.parse_args()

    diagnosis = StockDiagnosis()

    if args.code:
        result = diagnosis.diagnose(args.code)
        if result:
            diagnosis.print_result(result)
            if not args.skip_save:
                diagnosis.save_result(result)
            diagnosis.save_json(result)
        return

    stocks = load_stock_list(limit=args.limit)
    logger.info(f"[批次] 开始批量诊断: {len(stocks)} 只股票")

    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_stock = {
            executor.submit(diagnosis.diagnose, s.code, s.name): s
            for s in stocks
        }

        for future in as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                result = future.result()
                if result:
                    diagnosis.print_result(result)
                    if not args.skip_save:
                        diagnosis.save_result(result)
                    diagnosis.save_json(result)
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                logger.error(f"[批次] 处理失败: {stock.code} - {e}")
                fail_count += 1

            time.sleep(0.5)

    logger.info(f"[批次] 完成: 成功 {success_count}, 失败 {fail_count}")

    output_csv = PREDICTIONS_DIR / f"diagnosis_{date.today().isoformat()}.csv"
    logger.info(f"[输出] 诊断结果目录: {PREDICTIONS_DIR}")


if __name__ == "__main__":
    main()
