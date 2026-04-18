#!/usr/bin/env python3
"""
预测置信度计算与自我修正机制
功能：
1. 计算预测置信度（技术面、基本面、情绪面）
2. 检测需要修正的触发条件
3. 08:30盘前报告自动覆盖18:00预测

使用方法:
    python scripts/pipeline/prediction_confidence.py --calculate
    python scripts/pipeline/prediction_confidence.py --check-correction
"""
import sys
import os
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.trading_calendar import get_recent_trade_dates


@dataclass
class PredictionConfidence:
    """预测置信度数据类"""
    technical: float      # 技术面置信度 (0-1)
    fundamental: float    # 基本面置信度 (0-1)
    sentiment: float      # 情绪面置信度 (0-1)
    policy: float         # 政策面置信度 (0-1)
    overall: float        # 综合置信度 (0-1)
    level: str            # 置信等级: high/medium/low


class PredictionConfidenceCalculator:
    """预测置信度计算器"""

    def __init__(self, target_date: str = None):
        self.project_root = project_root
        self.data_dir = project_root / "data" / "predictions"
        self.data_dir.mkdir(exist_ok=True)

        if target_date:
            self.target_date = target_date
        else:
            trade_dates = get_recent_trade_dates(1)
            self.target_date = trade_dates[0] if trade_dates else datetime.now().strftime('%Y-%m-%d')

    def _log(self, message: str, level: str = 'info'):
        """输出日志"""
        prefix = {'info': 'ℹ️', 'warning': '⚠️', 'error': '❌', 'success': '✅'}.get(level, 'ℹ️')
        print(f"{prefix} {message}")

    def calculate_technical_confidence(self) -> float:
        """
        计算技术面置信度
        基于：趋势一致性、成交量配合度、技术指标共振度
        """
        # 模拟计算（实际使用时基于真实技术指标）
        trend_consistency = 0.8
        volume_confirmation = 0.7
        indicator_resonance = 0.75

        technical = (trend_consistency + volume_confirmation + indicator_resonance) / 3
        return round(technical, 2)

    def calculate_fundamental_confidence(self) -> float:
        """
        计算基本面置信度
        基于：宏观经济数据、行业景气度、公司业绩预期
        """
        # 模拟计算
        macro_stability = 0.6
        industry_outlook = 0.7
        earnings_expectation = 0.65

        fundamental = (macro_stability + industry_outlook + earnings_expectation) / 3
        return round(fundamental, 2)

    def calculate_sentiment_confidence(self) -> float:
        """
        计算情绪面置信度
        基于：市场情绪指标、资金流向、恐慌贪婪指数
        """
        # 模拟计算
        market_sentiment = 0.75
        fund_flow = 0.8
        fear_greed_index = 0.7

        sentiment = (market_sentiment + fund_flow + fear_greed_index) / 3
        return round(sentiment, 2)

    def calculate_policy_confidence(self) -> float:
        """
        计算政策面置信度
        基于：政策稳定性、监管环境、重大政策公告
        """
        # 模拟计算
        policy_stability = 0.8
        regulatory_environment = 0.75

        policy = (policy_stability + regulatory_environment) / 2
        return round(policy, 2)

    def calculate_overall_confidence(self) -> PredictionConfidence:
        """计算综合置信度"""
        technical = self.calculate_technical_confidence()
        fundamental = self.calculate_fundamental_confidence()
        sentiment = self.calculate_sentiment_confidence()
        policy = self.calculate_policy_confidence()

        # 加权平均（技术面和情绪面权重更高）
        weights = {
            'technical': 0.35,
            'fundamental': 0.20,
            'sentiment': 0.30,
            'policy': 0.15
        }

        overall = (
            technical * weights['technical'] +
            fundamental * weights['fundamental'] +
            sentiment * weights['sentiment'] +
            policy * weights['policy']
        )

        # 确定置信等级
        if overall >= 0.75:
            level = 'high'
        elif overall >= 0.55:
            level = 'medium'
        else:
            level = 'low'

        return PredictionConfidence(
            technical=technical,
            fundamental=fundamental,
            sentiment=sentiment,
            policy=policy,
            overall=round(overall, 2),
            level=level
        )

    def save_confidence(self, confidence: PredictionConfidence):
        """保存置信度数据"""
        data = {
            'date': self.target_date,
            'calculated_at': datetime.now().isoformat(),
            'confidence': {
                'technical': confidence.technical,
                'fundamental': confidence.fundamental,
                'sentiment': confidence.sentiment,
                'policy': confidence.policy,
                'overall': confidence.overall,
                'level': confidence.level
            }
        }

        confidence_file = self.data_dir / f"{self.target_date}_confidence.json"
        with open(confidence_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        self._log(f"置信度已保存: {confidence_file}")

    def load_confidence(self) -> Optional[Dict]:
        """加载置信度数据"""
        confidence_file = self.data_dir / f"{self.target_date}_confidence.json"
        if confidence_file.exists():
            with open(confidence_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None


class PredictionSelfCorrection:
    """预测自我修正机制"""

    # 修正触发条件
    CORRECTION_TRIGGERS = {
        'news_sentiment_delta': 0.30,      # 新闻情绪变化超过30%
        'policy_announcement': True,        # 检测到政策公告
        'overnight_gap': 0.02,              # 隔夜跳空超过2%
        'major_event': True,                # 重大事件
    }

    def __init__(self, target_date: str = None):
        self.project_root = project_root
        self.data_dir = project_root / "data" / "predictions"
        self.news_dir = project_root / "data" / "news"

        if target_date:
            self.target_date = target_date
        else:
            trade_dates = get_recent_trade_dates(1)
            self.target_date = trade_dates[0] if trade_dates else datetime.now().strftime('%Y-%m-%d')

    def _log(self, message: str, level: str = 'info'):
        """输出日志"""
        prefix = {'info': 'ℹ️', 'warning': '⚠️', 'error': '❌', 'success': '✅', 'correction': '🔄'}.get(level, 'ℹ️')
        print(f"{prefix} {message}")

    def check_news_sentiment_change(self) -> Tuple[bool, float]:
        """检查新闻情绪变化"""
        # 模拟：比较18:00和08:30的新闻情绪
        evening_sentiment = 0.6  # 18:00情绪
        morning_sentiment = 0.75  # 08:30情绪（隔夜新闻后）

        delta = abs(morning_sentiment - evening_sentiment)
        should_correct = delta > self.CORRECTION_TRIGGERS['news_sentiment_delta']

        return should_correct, delta

    def check_policy_announcement(self) -> Tuple[bool, str]:
        """检查政策公告"""
        # 模拟：检查是否有重大政策公告
        # 实际使用时分析新闻联播、证监会公告等

        news_file = self.news_dir / f"{self.target_date}_cctv.json"
        if news_file.exists():
            with open(news_file, 'r', encoding='utf-8') as f:
                news_data = json.load(f)
                # 检查是否包含政策关键词
                policy_keywords = ['政策', '监管', '证监会', '央行']
                for item in news_data.get('items', []):
                    for keyword in policy_keywords:
                        if keyword in item.get('content', ''):
                            return True, f"检测到政策相关新闻: {keyword}"

        return False, ""

    def check_overnight_gap(self) -> Tuple[bool, float]:
        """检查隔夜跳空"""
        # 模拟：检查隔夜美股、A50走势
        us_market_change = 0.015  # 美股变化1.5%
        a50_change = 0.025       # A50变化2.5%

        max_gap = max(abs(us_market_change), abs(a50_change))
        should_correct = max_gap > self.CORRECTION_TRIGGERS['overnight_gap']

        return should_correct, max_gap

    def check_correction_needed(self) -> Dict:
        """检查是否需要修正预测"""
        self._log("检查预测修正条件...")

        triggers = []

        # 检查新闻情绪变化
        news_trigger, news_delta = self.check_news_sentiment_change()
        if news_trigger:
            triggers.append({
                'type': 'news_sentiment',
                'description': f'新闻情绪变化 {news_delta:.1%}，超过阈值',
                'severity': 'high'
            })

        # 检查政策公告
        policy_trigger, policy_desc = self.check_policy_announcement()
        if policy_trigger:
            triggers.append({
                'type': 'policy',
                'description': policy_desc,
                'severity': 'critical'
            })

        # 检查隔夜跳空
        gap_trigger, gap_size = self.check_overnight_gap()
        if gap_trigger:
            triggers.append({
                'type': 'overnight_gap',
                'description': f'隔夜跳空 {gap_size:.1%}，超过阈值',
                'severity': 'high'
            })

        return {
            'correction_needed': len(triggers) > 0,
            'triggers': triggers,
            'timestamp': datetime.now().isoformat()
        }

    def generate_corrected_prediction(self, original_prediction: Dict) -> Dict:
        """生成修正后的预测"""
        correction_check = self.check_correction_needed()

        if not correction_check['correction_needed']:
            self._log("无需修正预测", 'success')
            return original_prediction

        self._log(f"检测到 {len(correction_check['triggers'])} 个修正触发条件", 'correction')

        for trigger in correction_check['triggers']:
            self._log(f"  - {trigger['description']}", 'warning')

        # 根据触发条件调整预测
        corrected = original_prediction.copy()
        corrected['corrected'] = True
        corrected['correction_reasons'] = correction_check['triggers']
        corrected['correction_timestamp'] = datetime.now().isoformat()

        # 调整预测方向（简化示例）
        for trigger in correction_check['triggers']:
            if trigger['type'] == 'overnight_gap' and 'gap_size' in trigger.get('description', ''):
                # 如果隔夜大涨，调整预测为看涨
                if '2.5%' in trigger['description']:
                    corrected['direction'] = 'bullish'
                    corrected['confidence'] = min(original_prediction.get('confidence', 0.7) + 0.1, 0.95)

        self._log("预测已修正", 'correction')
        return corrected

    def save_correction(self, correction_data: Dict):
        """保存修正记录"""
        correction_file = self.data_dir / f"{self.target_date}_correction.json"
        with open(correction_file, 'w', encoding='utf-8') as f:
            json.dump(correction_data, f, ensure_ascii=False, indent=2)

        self._log(f"修正记录已保存: {correction_file}")


def main():
    parser = argparse.ArgumentParser(description='预测置信度与自我修正')
    parser.add_argument('--date', type=str, help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--calculate', action='store_true', help='计算置信度')
    parser.add_argument('--check-correction', action='store_true', help='检查是否需要修正')
    parser.add_argument('--output', type=str, help='输出文件路径')

    args = parser.parse_args()

    results = {}

    # 计算置信度
    if args.calculate:
        calculator = PredictionConfidenceCalculator(target_date=args.date)
        confidence = calculator.calculate_overall_confidence()
        calculator.save_confidence(confidence)

        results['confidence'] = {
            'technical': confidence.technical,
            'fundamental': confidence.fundamental,
            'sentiment': confidence.sentiment,
            'policy': confidence.policy,
            'overall': confidence.overall,
            'level': confidence.level
        }

        print("\n" + "=" * 60)
        print("预测置信度")
        print("=" * 60)
        print(f"技术面置信度: {confidence.technical:.0%}")
        print(f"基本面置信度: {confidence.fundamental:.0%}")
        print(f"情绪面置信度: {confidence.sentiment:.0%}")
        print(f"政策面置信度: {confidence.policy:.0%}")
        print(f"综合置信度: {confidence.overall:.0%}")
        print(f"置信等级: {confidence.level.upper()}")

    # 检查修正
    if args.check_correction:
        corrector = PredictionSelfCorrection(target_date=args.date)
        correction_check = corrector.check_correction_needed()

        results['correction'] = correction_check

        print("\n" + "=" * 60)
        print("预测修正检查")
        print("=" * 60)
        if correction_check['correction_needed']:
            print(f"⚠️ 需要修正预测，触发条件:")
            for trigger in correction_check['triggers']:
                print(f"  - {trigger['description']}")
        else:
            print("✅ 无需修正预测")

    # 输出
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    elif results:
        print("\n" + json.dumps(results, ensure_ascii=False, indent=2))

    sys.exit(0)


if __name__ == "__main__":
    main()
