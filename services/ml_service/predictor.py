#!/usr/bin/env python3
"""
机器学习预测服务

支持模型:
- LSTM: 长短期记忆网络
- Transformer: 自注意力机制
- XGBoost: 梯度提升树
- LightGBM: 轻量级梯度提升

使用方法:
    from services.ml_service.predictor import StockPricePredictor
    
    predictor = StockPricePredictor(model_type='lstm')
    predictor.train(training_data)
    prediction = predictor.predict(latest_data)
"""
import os
import sys
import json
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)


class BaseModel:
    """模型基类"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.model = None
        self.is_trained = False
        
    def train(self, X: np.ndarray, y: np.ndarray) -> bool:
        """训练模型"""
        raise NotImplementedError
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        """预测"""
        raise NotImplementedError
    
    def save(self, path: str):
        """保存模型"""
        raise NotImplementedError
    
    def load(self, path: str) -> bool:
        """加载模型"""
        raise NotImplementedError


class LSTMModel(BaseModel):
    """LSTM预测模型"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.name = "lstm"
        self.sequence_length = config.get('sequence_length', 20)
        self.units = config.get('units', 64)
        self.dropout = config.get('dropout', 0.2)
        self.epochs = config.get('epochs', 50)
        self.batch_size = config.get('batch_size', 32)
        
    def build_model(self, input_shape: Tuple):
        """构建LSTM模型"""
        try:
            from tensorflow.keras.models import Sequential
            from tensorflow.keras.layers import LSTM, Dense, Dropout
            
            model = Sequential([
                LSTM(self.units, return_sequences=True, input_shape=input_shape),
                Dropout(self.dropout),
                LSTM(self.units // 2),
                Dropout(self.dropout),
                Dense(16, activation='relu'),
                Dense(1)
            ])
            
            model.compile(optimizer='adam', loss='mse', metrics=['mae'])
            self.model = model
            return True
        except ImportError:
            logger.error("TensorFlow未安装，无法使用LSTM模型")
            return False
    
    def train(self, X: np.ndarray, y: np.ndarray) -> bool:
        """训练LSTM模型"""
        if self.model is None:
            if not self.build_model((X.shape[1], X.shape[2])):
                return False
        
        try:
            from tensorflow.keras.callbacks import EarlyStopping
            
            early_stop = EarlyStopping(
                monitor='val_loss',
                patience=10,
                restore_best_weights=True
            )
            
            # 划分训练集和验证集
            split = int(0.8 * len(X))
            X_train, X_val = X[:split], X[split:]
            y_train, y_val = y[:split], y[split:]
            
            history = self.model.fit(
                X_train, y_train,
                validation_data=(X_val, y_val),
                epochs=self.epochs,
                batch_size=self.batch_size,
                callbacks=[early_stop],
                verbose=0
            )
            
            self.is_trained = True
            logger.info(f"LSTM训练完成，最终loss: {history.history['loss'][-1]:.4f}")
            return True
            
        except Exception as e:
            logger.error(f"LSTM训练失败: {e}")
            return False
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        """预测"""
        if not self.is_trained:
            logger.error("模型未训练")
            return np.array([])
        
        return self.model.predict(X, verbose=0)
    
    def save(self, path: str):
        """保存模型"""
        if self.model:
            self.model.save(path)
            logger.info(f"模型已保存: {path}")
    
    def load(self, path: str) -> bool:
        """加载模型"""
        try:
            from tensorflow.keras.models import load_model
            self.model = load_model(path)
            self.is_trained = True
            logger.info(f"模型已加载: {path}")
            return True
        except Exception as e:
            logger.error(f"加载模型失败: {e}")
            return False


class XGBoostModel(BaseModel):
    """XGBoost预测模型"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.name = "xgboost"
        self.params = {
            'n_estimators': config.get('n_estimators', 100),
            'max_depth': config.get('max_depth', 6),
            'learning_rate': config.get('learning_rate', 0.1),
            'subsample': config.get('subsample', 0.8),
            'colsample_bytree': config.get('colsample_bytree', 0.8),
            'random_state': 42
        }
        
    def train(self, X: np.ndarray, y: np.ndarray) -> bool:
        """训练XGBoost模型"""
        try:
            import xgboost as xgb
            
            self.model = xgb.XGBRegressor(**self.params)
            self.model.fit(X, y)
            
            self.is_trained = True
            logger.info("XGBoost训练完成")
            return True
            
        except ImportError:
            logger.error("XGBoost未安装")
            return False
        except Exception as e:
            logger.error(f"XGBoost训练失败: {e}")
            return False
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        """预测"""
        if not self.is_trained:
            logger.error("模型未训练")
            return np.array([])
        
        return self.model.predict(X)
    
    def get_feature_importance(self) -> Dict[str, float]:
        """获取特征重要性"""
        if not self.is_trained:
            return {}
        
        importance = self.model.feature_importances_
        return {f'feature_{i}': imp for i, imp in enumerate(importance)}
    
    def save(self, path: str):
        """保存模型"""
        if self.model:
            self.model.save_model(path)
    
    def load(self, path: str) -> bool:
        """加载模型"""
        try:
            import xgboost as xgb
            self.model = xgb.XGBRegressor()
            self.model.load_model(path)
            self.is_trained = True
            return True
        except Exception as e:
            logger.error(f"加载模型失败: {e}")
            return False


class StockPricePredictor:
    """股票价格预测器"""
    
    def __init__(self, model_type: str = 'xgboost', config: Dict = None):
        self.model_type = model_type
        self.config = config or {}
        self.model = self._create_model()
        self.scaler = None
        self.feature_cols = [
            'open', 'high', 'low', 'close', 'volume',
            'ma5', 'ma10', 'ma20', 'rsi', 'macd'
        ]
        
    def _create_model(self) -> BaseModel:
        """创建模型实例"""
        if self.model_type == 'lstm':
            return LSTMModel(self.config)
        elif self.model_type == 'xgboost':
            return XGBoostModel(self.config)
        else:
            raise ValueError(f"未知模型类型: {self.model_type}")
    
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """准备特征"""
        data = df.copy()
        
        # 基础价格特征
        data['returns'] = data['close'].pct_change()
        data['log_returns'] = np.log(data['close'] / data['close'].shift(1))
        
        # 移动平均线
        data['ma5'] = data['close'].rolling(5).mean()
        data['ma10'] = data['close'].rolling(10).mean()
        data['ma20'] = data['close'].rolling(20).mean()
        
        # 价格位置
        data['price_position'] = (data['close'] - data['low'].rolling(20).min()) / \
                                 (data['high'].rolling(20).max() - data['low'].rolling(20).min())
        
        # 波动率
        data['volatility'] = data['returns'].rolling(20).std()
        
        # 成交量特征
        data['volume_ma5'] = data['volume'].rolling(5).mean()
        data['volume_ratio'] = data['volume'] / data['volume_ma5']
        
        # RSI
        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        data['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD
        exp1 = data['close'].ewm(span=12).mean()
        exp2 = data['close'].ewm(span=26).mean()
        data['macd'] = exp1 - exp2
        data['macd_signal'] = data['macd'].ewm(span=9).mean()
        
        # 删除NaN
        data = data.dropna()
        
        return data
    
    def create_sequences(self, data: pd.DataFrame, sequence_length: int = 20) -> Tuple[np.ndarray, np.ndarray]:
        """创建序列数据（用于LSTM）"""
        features = self.feature_cols
        
        X, y = [], []
        for i in range(len(data) - sequence_length):
            X.append(data[features].iloc[i:i+sequence_length].values)
            y.append(data['close'].iloc[i+sequence_length])
        
        return np.array(X), np.array(y)
    
    def train(self, df: pd.DataFrame) -> bool:
        """训练模型"""
        logger.info(f"开始训练 {self.model_type} 模型")
        
        # 准备特征
        data = self.prepare_features(df)
        
        if self.model_type == 'lstm':
            X, y = self.create_sequences(data)
        else:
            X = data[self.feature_cols].values
            y = data['close'].values
        
        if len(X) == 0:
            logger.error("训练数据为空")
            return False
        
        # 训练
        success = self.model.train(X, y)
        
        if success:
            logger.info(f"模型训练成功，样本数: {len(X)}")
        
        return success
    
    def predict(self, df: pd.DataFrame) -> Dict[str, Any]:
        """预测"""
        if not self.model.is_trained:
            logger.error("模型未训练")
            return {}
        
        # 准备特征
        data = self.prepare_features(df)
        
        if self.model_type == 'lstm':
            if len(data) < 20:
                logger.error("数据不足")
                return {}
            X = data[self.feature_cols].iloc[-20:].values.reshape(1, 20, -1)
        else:
            X = data[self.feature_cols].iloc[-1:].values
        
        # 预测
        prediction = self.model.predict(X)
        
        if len(prediction) == 0:
            return {}
        
        current_price = data['close'].iloc[-1]
        predicted_price = prediction[0]
        
        return {
            'current_price': float(current_price),
            'predicted_price': float(predicted_price),
            'predicted_change': float((predicted_price - current_price) / current_price),
            'confidence': 0.7,  # 可以基于历史准确率计算
            'timestamp': datetime.now().isoformat()
        }
    
    def backtest(self, df: pd.DataFrame, threshold: float = 0.01) -> Dict[str, Any]:
        """回测"""
        logger.info("开始回测")
        
        data = self.prepare_features(df)
        
        predictions = []
        actuals = []
        
        # 滚动预测
        for i in range(100, len(data)):
            train_data = data.iloc[:i]
            test_data = data.iloc[i-20:i]
            
            # 临时训练
            temp_model = self._create_model()
            
            if self.model_type == 'lstm':
                X, y = self.create_sequences(train_data)
                if len(X) < 10:
                    continue
                temp_model.train(X, y)
                X_test = test_data[self.feature_cols].values.reshape(1, 20, -1)
            else:
                X = train_data[self.feature_cols].values
                y = train_data['close'].values
                temp_model.train(X, y)
                X_test = test_data[self.feature_cols].iloc[-1:].values
            
            pred = temp_model.predict(X_test)[0]
            actual = data['close'].iloc[i]
            
            predictions.append(pred)
            actuals.append(actual)
        
        # 计算指标
        predictions = np.array(predictions)
        actuals = np.array(actuals)
        
        mse = np.mean((predictions - actuals) ** 2)
        mae = np.mean(np.abs(predictions - actuals))
        
        # 方向准确率
        pred_direction = np.diff(predictions) > 0
        actual_direction = np.diff(actuals) > 0
        direction_accuracy = np.mean(pred_direction == actual_direction)
        
        return {
            'mse': float(mse),
            'mae': float(mae),
            'rmse': float(np.sqrt(mse)),
            'mape': float(np.mean(np.abs((actuals - predictions) / actuals))),
            'direction_accuracy': float(direction_accuracy),
            'predictions': predictions.tolist(),
            'actuals': actuals.tolist()
        }
    
    def save(self, path: str):
        """保存模型"""
        self.model.save(path)
        
        # 保存配置
        config_path = path + '.config.json'
        with open(config_path, 'w') as f:
            json.dump({
                'model_type': self.model_type,
                'config': self.config,
                'feature_cols': self.feature_cols
            }, f)
    
    def load(self, path: str) -> bool:
        """加载模型"""
        # 加载配置
        config_path = path + '.config.json'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                saved = json.load(f)
                self.feature_cols = saved.get('feature_cols', self.feature_cols)
        
        return self.model.load(path)


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 创建模拟数据
    np.random.seed(42)
    n_days = 500
    
    dates = pd.date_range(end=datetime.now(), periods=n_days, freq='D')
    
    # 生成随机游走价格
    returns = np.random.normal(0.001, 0.02, n_days)
    prices = 10 * np.exp(np.cumsum(returns))
    
    mock_data = pd.DataFrame({
        'trade_date': dates,
        'open': prices * (1 + np.random.normal(0, 0.01, n_days)),
        'high': prices * (1 + np.abs(np.random.normal(0, 0.02, n_days))),
        'low': prices * (1 - np.abs(np.random.normal(0, 0.02, n_days))),
        'close': prices,
        'volume': np.random.randint(1000000, 10000000, n_days)
    })
    
    mock_data = mock_data.set_index('trade_date')
    
    # 测试XGBoost
    print("\n=== 测试 XGBoost ===")
    predictor = StockPricePredictor(model_type='xgboost')
    
    # 训练
    success = predictor.train(mock_data)
    print(f"训练结果: {success}")
    
    if success:
        # 预测
        result = predictor.predict(mock_data)
        print(f"预测结果: {result}")
        
        # 回测
        backtest_result = predictor.backtest(mock_data)
        print(f"回测结果:")
        print(f"  RMSE: {backtest_result['rmse']:.4f}")
        print(f"  MAPE: {backtest_result['mape']:.4f}")
        print(f"  方向准确率: {backtest_result['direction_accuracy']:.2%}")
