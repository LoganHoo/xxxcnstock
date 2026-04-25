#!/usr/bin/env python3
"""
涨停开板预测模型

功能：
- 预测涨停股票开板概率
- 基于多因子模型
- 支持实时更新

预测因子：
1. 封单金额 / 流通市值
2. 换手率
3. 历史涨停开板率
4. 板块热度
5. 大盘情绪
6. 时间因子（早盘/午盘）
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
import json
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import pandas as pd
import polars as pl
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score

from core.logger import setup_logger
from core.paths import get_data_path


@dataclass
class OpeningPrediction:
    """开板预测结果"""
    code: str
    name: str
    current_price: float
    limit_up_price: float
    seal_amount: float  # 封单金额
    seal_ratio: float   # 封单比例
    opening_prob: float # 开板概率
    confidence: float   # 置信度
    factors: Dict[str, float]  # 各因子得分
    recommendation: str  # 建议: 'buy', 'watch', 'avoid'
    
    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'name': self.name,
            'current_price': self.current_price,
            'limit_up_price': self.limit_up_price,
            'seal_amount': self.seal_amount,
            'seal_ratio': self.seal_ratio,
            'opening_prob': self.opening_prob,
            'confidence': self.confidence,
            'factors': self.factors,
            'recommendation': self.recommendation
        }


class LimitUpOpeningPredictor:
    """涨停开板预测器"""
    
    def __init__(self, model_path: Optional[str] = None):
        self.logger = setup_logger("opening_predictor")
        
        # 模型
        self.model = None
        self.model_path = model_path or str(get_data_path() / "models" / "opening_predictor.pkl")
        
        # 特征列
        self.feature_columns = [
            'seal_amount_ratio',      # 封单金额/流通市值
            'turnover_rate',          # 换手率
            'historical_opening_rate', # 历史开板率
            'sector_heat',            # 板块热度
            'market_sentiment',       # 大盘情绪
            'time_factor',            # 时间因子
            'limit_up_count',         # 连板数
            'volume_ratio'            # 量比
        ]
        
        # 加载模型
        self._load_model()
    
    def _load_model(self):
        """加载模型"""
        if Path(self.model_path).exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model = pickle.load(f)
                self.logger.info("模型加载成功")
            except Exception as e:
                self.logger.error(f"模型加载失败: {e}")
                self.model = None
        else:
            self.logger.info("模型文件不存在，将创建新模型")
            self.model = None
    
    def _save_model(self):
        """保存模型"""
        Path(self.model_path).parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(self.model_path, 'wb') as f:
                pickle.dump(self.model, f)
            self.logger.info("模型保存成功")
        except Exception as e:
            self.logger.error(f"模型保存失败: {e}")
    
    def train(
        self,
        training_data: pd.DataFrame,
        target_column: str = 'opened'
    ) -> Dict:
        """
        训练模型
        
        Args:
            training_data: 训练数据
            target_column: 目标列名
        
        Returns:
            训练结果
        """
        self.logger.info("开始训练开板预测模型...")
        
        # 准备数据
        X = training_data[self.feature_columns]
        y = training_data[target_column]
        
        # 划分训练集和测试集
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # 创建模型
        self.model = GradientBoostingClassifier(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42
        )
        
        # 训练
        self.model.fit(X_train, y_train)
        
        # 评估
        y_pred = self.model.predict(X_test)
        y_prob = self.model.predict_proba(X_test)[:, 1]
        
        metrics = {
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred),
            'recall': recall_score(y_test, y_pred),
            'auc': roc_auc_score(y_test, y_prob)
        }
        
        self.logger.info(f"训练完成: {metrics}")
        
        # 保存模型
        self._save_model()
        
        return metrics
    
    def predict(
        self,
        code: str,
        market_data: Dict,
        historical_data: Optional[Dict] = None
    ) -> Optional[OpeningPrediction]:
        """
        预测开板概率
        
        Args:
            code: 股票代码
            market_data: 市场数据
            historical_data: 历史数据
        
        Returns:
            预测结果
        """
        if self.model is None:
            self.logger.warning("模型未训练，使用规则模型")
            return self._rule_based_predict(code, market_data, historical_data)
        
        # 提取特征
        features = self._extract_features(code, market_data, historical_data)
        
        if features is None:
            return None
        
        # 预测
        X = pd.DataFrame([features])
        prob = self.model.predict_proba(X)[0][1]
        
        # 生成建议
        recommendation = self._generate_recommendation(prob, features)
        
        return OpeningPrediction(
            code=code,
            name=market_data.get('name', ''),
            current_price=market_data.get('price', 0),
            limit_up_price=market_data.get('limit_up_price', 0),
            seal_amount=features.get('seal_amount', 0),
            seal_ratio=features.get('seal_amount_ratio', 0),
            opening_prob=prob,
            confidence=self._calculate_confidence(features),
            factors=features,
            recommendation=recommendation
        )
    
    def predict_batch(
        self,
        stocks_data: List[Dict],
        historical_data: Optional[Dict] = None
    ) -> List[OpeningPrediction]:
        """
        批量预测
        
        Args:
            stocks_data: 股票数据列表
            historical_data: 历史数据
        
        Returns:
            预测结果列表
        """
        results = []
        
        for data in stocks_data:
            code = data.get('code')
            prediction = self.predict(code, data, historical_data)
            
            if prediction:
                results.append(prediction)
        
        # 按开板概率排序
        results.sort(key=lambda x: x.opening_prob)
        
        return results
    
    def _extract_features(
        self,
        code: str,
        market_data: Dict,
        historical_data: Optional[Dict]
    ) -> Optional[Dict]:
        """提取特征"""
        try:
            features = {}
            
            # 1. 封单金额比例
            seal_amount = market_data.get('seal_amount', 0)
            market_cap = market_data.get('market_cap', 1)
            features['seal_amount_ratio'] = seal_amount / market_cap if market_cap > 0 else 0
            features['seal_amount'] = seal_amount
            
            # 2. 换手率
            features['turnover_rate'] = market_data.get('turnover_rate', 0)
            
            # 3. 历史开板率
            features['historical_opening_rate'] = self._get_historical_opening_rate(code, historical_data)
            
            # 4. 板块热度
            features['sector_heat'] = market_data.get('sector_heat', 0.5)
            
            # 5. 大盘情绪
            features['market_sentiment'] = market_data.get('market_sentiment', 0.5)
            
            # 6. 时间因子
            features['time_factor'] = self._calculate_time_factor()
            
            # 7. 连板数
            features['limit_up_count'] = market_data.get('limit_up_count', 1)
            
            # 8. 量比
            features['volume_ratio'] = market_data.get('volume_ratio', 1.0)
            
            return features
            
        except Exception as e:
            self.logger.error(f"特征提取失败: {code}, {e}")
            return None
    
    def _rule_based_predict(
        self,
        code: str,
        market_data: Dict,
        historical_data: Optional[Dict]
    ) -> OpeningPrediction:
        """基于规则的预测（模型未训练时使用）"""
        features = self._extract_features(code, market_data, historical_data)
        
        # 简单规则
        prob = 0.5
        
        # 封单金额越大，开板概率越低
        seal_ratio = features.get('seal_amount_ratio', 0)
        if seal_ratio > 0.1:
            prob -= 0.2
        elif seal_ratio < 0.01:
            prob += 0.2
        
        # 换手率越高，开板概率越高
        turnover = features.get('turnover_rate', 0)
        if turnover > 0.1:
            prob += 0.15
        
        # 连板数越多，开板概率越高
        limit_count = features.get('limit_up_count', 1)
        if limit_count > 3:
            prob += 0.1 * (limit_count - 3)
        
        # 限制概率范围
        prob = max(0.1, min(0.9, prob))
        
        recommendation = self._generate_recommendation(prob, features)
        
        return OpeningPrediction(
            code=code,
            name=market_data.get('name', ''),
            current_price=market_data.get('price', 0),
            limit_up_price=market_data.get('limit_up_price', 0),
            seal_amount=features.get('seal_amount', 0),
            seal_ratio=seal_ratio,
            opening_prob=prob,
            confidence=0.6,
            factors=features,
            recommendation=recommendation
        )
    
    def _get_historical_opening_rate(
        self,
        code: str,
        historical_data: Optional[Dict]
    ) -> float:
        """获取历史开板率"""
        if historical_data and code in historical_data:
            return historical_data[code].get('opening_rate', 0.3)
        
        # 默认30%
        return 0.3
    
    def _calculate_time_factor(self) -> float:
        """计算时间因子"""
        now = datetime.now()
        hour = now.hour
        minute = now.minute
        
        # 9:30-10:00 早盘，开板概率较低
        if hour == 9 and minute >= 30:
            return 0.3
        # 10:00-11:30 上午，开板概率中等
        elif hour == 10 or (hour == 11 and minute <= 30):
            return 0.5
        # 13:00-14:00 下午开盘，开板概率较高
        elif hour == 13:
            return 0.6
        # 14:00-15:00 尾盘，开板概率最高
        else:
            return 0.7
    
    def _calculate_confidence(self, features: Dict) -> float:
        """计算置信度"""
        # 基于数据完整性计算置信度
        confidence = 0.5
        
        # 封单数据完整
        if features.get('seal_amount_ratio', 0) > 0:
            confidence += 0.2
        
        # 历史数据完整
        if features.get('historical_opening_rate', 0.3) != 0.3:
            confidence += 0.2
        
        # 板块数据完整
        if features.get('sector_heat', 0.5) != 0.5:
            confidence += 0.1
        
        return min(0.95, confidence)
    
    def _generate_recommendation(self, prob: float, features: Dict) -> str:
        """生成建议"""
        seal_ratio = features.get('seal_amount_ratio', 0)
        
        if prob < 0.3 and seal_ratio > 0.05:
            return 'buy'  # 开板概率低，建议买入
        elif prob < 0.5:
            return 'watch'  # 观望
        else:
            return 'avoid'  # 开板概率高，建议回避
    
    def get_feature_importance(self) -> Optional[Dict]:
        """获取特征重要性"""
        if self.model is None:
            return None
        
        importance = dict(zip(
            self.feature_columns,
            self.model.feature_importances_
        ))
        
        # 排序
        return dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))


# 便捷函数
def create_opening_predictor(model_path: Optional[str] = None) -> LimitUpOpeningPredictor:
    """创建开板预测器"""
    return LimitUpOpeningPredictor(model_path)


def predict_opening(
    code: str,
    market_data: Dict,
    historical_data: Optional[Dict] = None
) -> Optional[OpeningPrediction]:
    """便捷函数：预测开板"""
    predictor = create_opening_predictor()
    return predictor.predict(code, market_data, historical_data)


if __name__ == "__main__":
    # 测试
    predictor = LimitUpOpeningPredictor()
    
    # 模拟数据
    market_data = {
        'name': '测试股票',
        'price': 10.0,
        'limit_up_price': 11.0,
        'seal_amount': 50000000,  # 5000万封单
        'market_cap': 1000000000,  # 100亿市值
        'turnover_rate': 0.05,
        'sector_heat': 0.8,
        'market_sentiment': 0.7,
        'limit_up_count': 2,
        'volume_ratio': 2.5
    }
    
    # 预测
    result = predictor.predict("000001", market_data)
    
    if result:
        print(f"股票: {result.code} {result.name}")
        print(f"开板概率: {result.opening_prob:.2%}")
        print(f"建议: {result.recommendation}")
        print(f"置信度: {result.confidence:.2%}")
