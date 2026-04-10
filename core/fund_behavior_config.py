"""
资金行为学系统配置管理
"""
import os
import yaml
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class FundBehaviorConfigManager:
    """资金行为学系统配置管理器"""
    
    def __init__(self, config_file: str = 'config/fund_behavior_config.yaml'):
        self.config_file = config_file
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """加载配置文件"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f)
                logger.info(f"成功加载配置文件: {self.config_file}")
            else:
                logger.warning(f"配置文件不存在: {self.config_file}")
                self._create_default_config()
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            self._create_default_config()
    
    def _create_default_config(self):
        """创建默认配置"""
        default_config = {
            'factors': {
                'v_ratio10': {
                    'enabled': True,
                    'category': 'volume_price',
                    'description': '早盘09:30-10:00成交量与前一日同期比值'
                },
                'v_total': {
                    'enabled': True,
                    'category': 'market',
                    'description': '全市场实时总成交金额'
                },
                'cost_peak': {
                    'enabled': True,
                    'category': 'market',
                    'description': '筹码分布最大密集峰位',
                    'params': {
                        'window': 20,
                        'bins': 50
                    }
                },
                'limit_up_score': {
                    'enabled': True,
                    'category': 'market',
                    'description': '涨停家数 - 跌停家数 + 连板高度'
                },
                'pioneer_status': {
                    'enabled': True,
                    'category': 'market',
                    'description': '核心领涨个股实时涨跌幅',
                    'params': {
                        'pioneer_codes': ['300255']  # 七彩化学
                    }
                },
                'ma5_bias': {
                    'enabled': True,
                    'category': 'technical',
                    'description': '(当前价 - 5日均线) / 5日均线'
                }
            },
            'indicators': {
                'market_sentiment': {
                    'thresholds': {
                        'strong_v_total': 2.85,  # 万亿
                        'oscillating_v_total_min': 2.5,  # 万亿
                        'oscillating_v_total_max': 2.8,  # 万亿
                        'sentiment_temperature_strong': 50,  # 度
                        'sentiment_temperature_overheat': 80,  # 度
                        'cost_peak_support': 0.995  # 动态筹码峰位系数
                    }
                },
                '10am_pivot': {
                    'v_ratio10_threshold': 1.1,
                    'price_threshold': 4081
                },
                'hedge': {
                    'support_level': 4067,
                    'resistance_levels': [4117, 4140]
                }
            },
            'strategy': {
                'position': {
                    'trend': 0.5,  # 50% 仓位
                    'short_term': 0.4,  # 40% 仓位
                    'cash': 0.1  # 10% 现金
                },
                'stock_selection': {
                    'trend_stocks_limit': 20,
                    'short_term_stocks_limit': 10
                },
                'four_step_exit': {
                    'time_points': ['09:26', '10:00', '14:56']
                }
            },
            'backtest': {
                'start_date': '2023-01-01',
                'end_date': '2023-12-31',
                'initial_capital': 1000000,
                'transaction_cost': 0.0003
            }
        }
        
        self.config = default_config
        self.save_config()
        logger.info(f"创建默认配置文件: {self.config_file}")
    
    def save_config(self):
        """保存配置文件"""
        try:
            # 确保配置文件目录存在
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
            logger.info(f"成功保存配置文件: {self.config_file}")
        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值
        
        Args:
            key: 配置键，支持点号分隔的路径
            default: 默认值
        
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any, auto_save: bool = True):
        """设置配置值

        Args:
            key: 配置键，支持点号分隔的路径
            value: 配置值
            auto_save: 是否自动保存（默认True，批量更新时设为False）
        """
        keys = key.split('.')
        config = self.config

        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value
        if auto_save:
            self.save_config()

    def batch_set(self, items: Dict[str, Any]):
        """批量设置配置值（只保存一次）

        Args:
            items: 配置字典 {key: value}
        """
        for key, value in items.items():
            keys = key.split('.')
            config = self.config

            for k in keys[:-1]:
                if k not in config:
                    config[k] = {}
                config = config[k]

            config[keys[-1]] = value

        self.save_config()
    
    def get_enabled_factors(self) -> list:
        """获取启用的因子列表
        
        Returns:
            启用的因子名称列表
        """
        factors = []
        
        if 'factors' in self.config:
            for factor_name, factor_config in self.config['factors'].items():
                if factor_config.get('enabled', True):
                    factors.append(factor_name)
        
        return factors
    
    def get_factor_params(self, factor_name: str) -> Dict[str, Any]:
        """获取因子参数
        
        Args:
            factor_name: 因子名称
        
        Returns:
            因子参数
        """
        if 'factors' in self.config and factor_name in self.config['factors']:
            return self.config['factors'][factor_name].get('params', {})
        return {}
    
    def validate_config(self) -> bool:
        """验证配置
        
        Returns:
            是否有效
        """
        # 检查必要的配置项
        required_sections = ['factors', 'indicators', 'strategy', 'backtest']
        
        for section in required_sections:
            if section not in self.config:
                logger.error(f"配置缺少必要的部分: {section}")
                return False
        
        return True


# 全局配置管理器实例
config_manager = FundBehaviorConfigManager()
