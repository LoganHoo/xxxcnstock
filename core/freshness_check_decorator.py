"""数据新鲜度检查装饰器

为脚本提供统一的数据新鲜度检查功能，确保分析前数据是最新的。
"""
import logging
from functools import wraps
from pathlib import Path

from core.data_freshness_checker import DataFreshnessChecker

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 确保核心日志记录器也被配置
logger = logging.getLogger('core')
logger.setLevel(logging.INFO)


def check_data_freshness(func):
    """数据新鲜度检查装饰器
    
    用于装饰需要数据新鲜度检查的函数，确保分析前数据是最新的。
    
    Args:
        func: 被装饰的函数
    
    Returns:
        装饰后的函数
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 确定数据目录
        project_root = Path(__file__).parent.parent
        data_dir = project_root / "data"
        
        # 初始化数据新鲜度检查器
        checker = DataFreshnessChecker(str(data_dir))
        
        # 确保数据新鲜度
        if not checker.ensure_data_freshness():
            import sys
            sys.exit(1)
        
        # 执行原函数
        return func(*args, **kwargs)
    
    return wrapper


def check_data_freshness_manual():
    """手动检查数据新鲜度
    
    Returns:
        bool: 数据是否新鲜
    """
    logger = logging.getLogger(__name__)
    
    # 确定数据目录
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    
    # 初始化数据新鲜度检查器
    checker = DataFreshnessChecker(str(data_dir))
    
    # 确保数据新鲜度
    return checker.ensure_data_freshness()
