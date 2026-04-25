#!/usr/bin/env python3
"""
退市股票守护模块

用于检测和过滤已退市、暂停上市、风险警示的股票
确保数据采集时自动排除这些股票
"""
import re
import logging
from typing import List, Set, Optional
from pathlib import Path
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class DelistingGuard:
    """
    退市股票守护类
    
    功能：
    1. 检测股票名称是否包含退市相关关键词
    2. 维护退市股票黑名单
    3. 过滤股票列表中的退市股票
    4. 记录退市股票历史
    """
    
    # 退市相关关键词模式
    DELISTING_PATTERNS = [
        r"退市",           # 包含"退市"
        r"退",             # 包含"退"
        r"终止上市",       # 终止上市
        r"暂停上市",       # 暂停上市
        r"风险警示",       # 风险警示
        r"ST",             # ST股票
        r"\*ST",           # *ST股票
        r"S\*ST",          # S*ST股票
        r"SST",            # SST股票
    ]
    
    # 已知的退市股票代码（静态列表，用于快速过滤）
    KNOWN_DELISTED_CODES: Set[str] = set()
    
    def __init__(self, blacklist_file: Optional[Path] = None):
        """
        初始化退市守护
        
        Args:
            blacklist_file: 退市股票黑名单文件路径
        """
        self.blacklist_file = blacklist_file or self._get_default_blacklist_file()
        self.delisted_codes: Set[str] = set()
        self.suspended_codes: Set[str] = set()
        self.risk_codes: Set[str] = set()
        
        self._load_blacklist()
    
    def _get_default_blacklist_file(self) -> Path:
        """获取默认黑名单文件路径"""
        from core.paths import DATA_DIR
        return DATA_DIR / "delisted_stocks.json"
    
    def _load_blacklist(self):
        """加载退市股票黑名单"""
        if self.blacklist_file.exists():
            try:
                with open(self.blacklist_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.delisted_codes = set(data.get('delisted', []))
                    self.suspended_codes = set(data.get('suspended', []))
                    self.risk_codes = set(data.get('risk', []))
                logger.info(f"已加载退市股票黑名单: {len(self.delisted_codes)} 只退市, "
                          f"{len(self.suspended_codes)} 只暂停, "
                          f"{len(self.risk_codes)} 只风险警示")
            except Exception as e:
                logger.warning(f"加载退市股票黑名单失败: {e}")
    
    def _save_blacklist(self):
        """保存退市股票黑名单"""
        try:
            self.blacklist_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.blacklist_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'delisted': sorted(list(self.delisted_codes)),
                    'suspended': sorted(list(self.suspended_codes)),
                    'risk': sorted(list(self.risk_codes)),
                    'updated_at': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存退市股票黑名单失败: {e}")
    
    def is_delisted_by_name(self, name: str) -> bool:
        """
        根据股票名称判断是否已退市或有退市风险
        
        Args:
            name: 股票名称
        
        Returns:
            是否已退市或有退市风险
        """
        if not name:
            return False
        
        pattern = "|".join(self.DELISTING_PATTERNS)
        return bool(re.search(pattern, name))
    
    def is_delisted_by_code(self, code: str) -> bool:
        """
        根据股票代码判断是否已退市
        
        Args:
            code: 股票代码
        
        Returns:
            是否已退市
        """
        return code in self.delisted_codes or code in self.suspended_codes
    
    def is_risk_stock(self, code: str, name: Optional[str] = None) -> bool:
        """
        判断是否为风险股票（退市、暂停、风险警示）
        
        Args:
            code: 股票代码
            name: 股票名称（可选）
        
        Returns:
            是否为风险股票
        """
        # 检查代码是否在黑名单中
        if code in self.delisted_codes:
            return True
        if code in self.suspended_codes:
            return True
        if code in self.risk_codes:
            return True
        
        # 检查名称是否包含退市关键词
        if name and self.is_delisted_by_name(name):
            return True
        
        return False
    
    def filter_stock_list(self, stock_list, code_col: str = 'code', 
                          name_col: str = 'name') -> List[dict]:
        """
        过滤股票列表，移除退市股票
        
        Args:
            stock_list: 股票列表（DataFrame 或 dict list）
            code_col: 代码列名
            name_col: 名称列名
        
        Returns:
            过滤后的股票列表
        """
        import polars as pl
        import pandas as pd
        
        if isinstance(stock_list, pl.DataFrame):
            # Polars DataFrame
            if name_col in stock_list.columns:
                pattern = "|".join(self.DELISTING_PATTERNS)
                filtered = stock_list.filter(
                    ~pl.col(name_col).str.contains(pattern)
                )
                removed = len(stock_list) - len(filtered)
                if removed > 0:
                    logger.info(f"退市过滤: 移除 {removed} 只退市/风险股票")
                return filtered
            return stock_list
        
        elif isinstance(stock_list, pd.DataFrame):
            # Pandas DataFrame
            if name_col in stock_list.columns:
                pattern = "|".join(self.DELISTING_PATTERNS)
                mask = ~stock_list[name_col].str.contains(pattern, na=False, regex=True)
                filtered = stock_list[mask]
                removed = len(stock_list) - len(filtered)
                if removed > 0:
                    logger.info(f"退市过滤: 移除 {removed} 只退市/风险股票")
                return filtered
            return stock_list
        
        elif isinstance(stock_list, list):
            # List of dicts
            filtered = []
            removed = 0
            for stock in stock_list:
                code = stock.get(code_col, '')
                name = stock.get(name_col, '')
                
                if self.is_risk_stock(code, name):
                    removed += 1
                    continue
                
                filtered.append(stock)
            
            if removed > 0:
                logger.info(f"退市过滤: 移除 {removed} 只退市/风险股票")
            return filtered
        
        return stock_list
    
    def add_to_blacklist(self, code: str, category: str = 'delisted'):
        """
        添加股票到黑名单
        
        Args:
            code: 股票代码
            category: 类别 ('delisted', 'suspended', 'risk')
        """
        if category == 'delisted':
            self.delisted_codes.add(code)
        elif category == 'suspended':
            self.suspended_codes.add(code)
        elif category == 'risk':
            self.risk_codes.add(code)
        
        self._save_blacklist()
        logger.info(f"已将 {code} 添加到 {category} 黑名单")
    
    def remove_from_blacklist(self, code: str):
        """
        从黑名单中移除股票
        
        Args:
            code: 股票代码
        """
        self.delisted_codes.discard(code)
        self.suspended_codes.discard(code)
        self.risk_codes.discard(code)
        self._save_blacklist()
        logger.info(f"已将 {code} 从黑名单移除")
    
    def get_blacklist_stats(self) -> dict:
        """获取黑名单统计信息"""
        return {
            'delisted_count': len(self.delisted_codes),
            'suspended_count': len(self.suspended_codes),
            'risk_count': len(self.risk_codes),
            'total': len(self.delisted_codes | self.suspended_codes | self.risk_codes)
        }


# 全局退市守护实例
_delisting_guard: Optional[DelistingGuard] = None


def get_delisting_guard() -> DelistingGuard:
    """获取退市守护单例"""
    global _delisting_guard
    if _delisting_guard is None:
        _delisting_guard = DelistingGuard()
    return _delisting_guard


def is_delisted_stock(code: str, name: Optional[str] = None) -> bool:
    """
    便捷函数：判断是否为退市股票
    
    Args:
        code: 股票代码
        name: 股票名称
    
    Returns:
        是否为退市股票
    """
    return get_delisting_guard().is_risk_stock(code, name)


def filter_delisted_stocks(stock_list, code_col: str = 'code', 
                           name_col: str = 'name'):
    """
    便捷函数：过滤退市股票
    
    Args:
        stock_list: 股票列表
        code_col: 代码列名
        name_col: 名称列名
    
    Returns:
        过滤后的股票列表
    """
    return get_delisting_guard().filter_stock_list(stock_list, code_col, name_col)
