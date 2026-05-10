#!/usr/bin/env python3
"""
股票列表管理器 - 支持每日更新和变更追踪

功能：
1. 每日自动更新股票列表
2. 检测新增、退市、更名股票
3. 维护变更历史
4. 原子性写入（防止数据损坏）
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pandas as pd
import polars as pl
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass, asdict
import json
import shutil

from core.logger import setup_logger
from core.delisting_guard import get_delisting_guard

logger = setup_logger("stock_list_manager", log_file="system/stock_list_manager.log")

# 延迟导入避免循环依赖
_fetch_stock_list = None

def _get_fetch_stock_list():
    """延迟获取fetch_stock_list函数"""
    global _fetch_stock_list
    if _fetch_stock_list is None:
        try:
            from .stock_list_fetcher import fetch_stock_list
        except ImportError:
            from stock_list_fetcher import fetch_stock_list
        _fetch_stock_list = fetch_stock_list
    return _fetch_stock_list


@dataclass
class StockChange:
    """股票变更记录"""
    date: str
    code: str
    name: str
    change_type: str  # 'new', 'delisted', 'renamed', 'industry_changed'
    old_value: Optional[str] = None
    new_value: Optional[str] = None


@dataclass
class StockListSnapshot:
    """股票列表快照"""
    date: str
    total_count: int
    exchange_counts: Dict[str, int]
    industry_counts: Dict[str, int]


class StockListManager:
    """
    股票列表管理器
    
    使用方式：
        manager = StockListManager()
        result = manager.update_stock_list()  # 每日调用
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.stock_list_path = self.data_dir / "stock_list.parquet"
        self.backup_dir = self.data_dir / "backups" / "stock_list"
        self.change_log_path = self.data_dir / "stock_list_changes.json"
        self.snapshot_path = self.data_dir / "stock_list_snapshots.json"
        
        # 确保目录存在
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
    def load_current_list(self) -> Optional[pd.DataFrame]:
        """加载当前股票列表"""
        if not self.stock_list_path.exists():
            return None
        
        try:
            return pd.read_parquet(self.stock_list_path)
        except Exception as e:
            logger.error(f"加载当前股票列表失败: {e}")
            # 尝试从备份恢复
            return self._restore_from_backup()
    
    def _restore_from_backup(self) -> Optional[pd.DataFrame]:
        """从备份恢复"""
        backups = sorted(self.backup_dir.glob("stock_list_*.parquet"), reverse=True)
        if backups:
            try:
                logger.warning(f"尝试从备份恢复: {backups[0]}")
                return pd.read_parquet(backups[0])
            except Exception as e:
                logger.error(f"备份恢复失败: {e}")
        return None
    
    def _create_backup(self):
        """创建备份"""
        if self.stock_list_path.exists():
            backup_name = f"stock_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            backup_path = self.backup_dir / backup_name
            shutil.copy2(self.stock_list_path, backup_path)
            logger.info(f"已创建备份: {backup_path}")
            
            # 清理旧备份（保留最近30个）
            self._cleanup_old_backups()
    
    def _cleanup_old_backups(self, keep_count: int = 30):
        """清理旧备份"""
        backups = sorted(self.backup_dir.glob("stock_list_*.parquet"))
        if len(backups) > keep_count:
            for old_backup in backups[:-keep_count]:
                old_backup.unlink()
                logger.debug(f"删除旧备份: {old_backup}")
    
    def _detect_changes(
        self,
        old_df: Optional[pd.DataFrame],
        new_df: pd.DataFrame
    ) -> List[StockChange]:
        """检测股票列表变更"""
        changes = []
        today = date.today().isoformat()
        
        if old_df is None or old_df.empty:
            # 首次运行，所有股票都是新增的
            for _, row in new_df.iterrows():
                changes.append(StockChange(
                    date=today,
                    code=row['code'],
                    name=row['name'],
                    change_type='new',
                    new_value=row['name']
                ))
            return changes
        
        # 转换为字典便于查找
        old_stocks = {row['code']: row for _, row in old_df.iterrows()}
        new_stocks = {row['code']: row for _, row in new_df.iterrows()}
        
        # 检测新增股票
        for code, row in new_stocks.items():
            if code not in old_stocks:
                changes.append(StockChange(
                    date=today,
                    code=code,
                    name=row['name'],
                    change_type='new',
                    new_value=row['name']
                ))
        
        # 检测退市股票
        for code, row in old_stocks.items():
            if code not in new_stocks:
                changes.append(StockChange(
                    date=today,
                    code=code,
                    name=row['name'],
                    change_type='delisted',
                    old_value=row['name']
                ))
        
        # 检测更名股票
        for code in set(old_stocks.keys()) & set(new_stocks.keys()):
            old_name = old_stocks[code]['name']
            new_name = new_stocks[code]['name']
            if old_name != new_name:
                changes.append(StockChange(
                    date=today,
                    code=code,
                    name=new_name,
                    change_type='renamed',
                    old_value=old_name,
                    new_value=new_name
                ))
        
        # 检测行业变更
        for code in set(old_stocks.keys()) & set(new_stocks.keys()):
            old_industry = old_stocks[code].get('industry', '')
            new_industry = new_stocks[code].get('industry', '')
            if old_industry != new_industry:
                changes.append(StockChange(
                    date=today,
                    code=code,
                    name=new_stocks[code]['name'],
                    change_type='industry_changed',
                    old_value=old_industry,
                    new_value=new_industry
                ))
        
        return changes
    
    def _save_changes(self, changes: List[StockChange]):
        """保存变更记录"""
        if not changes:
            return
        
        # 加载历史变更
        history = []
        if self.change_log_path.exists():
            try:
                with open(self.change_log_path, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            except Exception as e:
                logger.warning(f"加载历史变更失败: {e}")
        
        # 添加新变更
        new_records = [asdict(c) for c in changes]
        history.extend(new_records)
        
        # 保存
        with open(self.change_log_path, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        
        logger.info(f"已记录 {len(changes)} 条变更")
    
    def _save_snapshot(self, df: pd.DataFrame):
        """保存快照统计"""
        today = date.today().isoformat()
        
        # 统计信息
        exchange_counts = df['exchange'].value_counts().to_dict() if 'exchange' in df.columns else {}
        industry_counts = df['industry'].value_counts().head(10).to_dict() if 'industry' in df.columns else {}
        
        snapshot = StockListSnapshot(
            date=today,
            total_count=len(df),
            exchange_counts=exchange_counts,
            industry_counts=industry_counts
        )
        
        # 加载历史快照
        snapshots = []
        if self.snapshot_path.exists():
            try:
                with open(self.snapshot_path, 'r', encoding='utf-8') as f:
                    snapshots = json.load(f)
            except Exception as e:
                logger.warning(f"加载历史快照失败: {e}")
        
        # 添加新快照
        snapshots.append(asdict(snapshot))
        
        # 保存
        with open(self.snapshot_path, 'w', encoding='utf-8') as f:
            json.dump(snapshots, f, ensure_ascii=False, indent=2)
    
    def _atomic_write_parquet(self, df: pd.DataFrame, output_path: Path):
        """
        原子性写入 Parquet 文件
        
        策略：
        1. 写入临时文件
        2. 验证临时文件
        3. 重命名替换原文件
        """
        temp_path = output_path.with_suffix('.tmp')
        
        try:
            # 1. 写入临时文件
            if isinstance(df, pl.DataFrame):
                df.write_parquet(temp_path)
            else:
                df.to_parquet(temp_path, index=False)
            
            # 2. 验证临时文件
            verify_df = pd.read_parquet(temp_path)
            if len(verify_df) != len(df):
                raise ValueError(f"验证失败: 写入{len(df)}行, 读取{len(verify_df)}行")
            
            # 3. 原子性替换
            temp_path.replace(output_path)
            logger.info(f"成功更新: {output_path}")
            
        except Exception as e:
            # 清理临时文件
            if temp_path.exists():
                temp_path.unlink()
            raise e
    
    def update_stock_list(self) -> Dict:
        """
        更新股票列表（主入口）
        
        Returns:
            {
                'success': bool,
                'total': int,
                'changes': List[StockChange],
                'message': str
            }
        """
        logger.info("=" * 60)
        logger.info("开始更新股票列表")
        logger.info("=" * 60)
        
        try:
            # 1. 加载当前列表
            old_df = self.load_current_list()
            if old_df is not None:
                logger.info(f"当前列表: {len(old_df)} 只股票")
            
            # 2. 获取最新列表
            logger.info("从数据源获取最新股票列表...")
            fetch_stock_list_func = _get_fetch_stock_list()
            stock_list = fetch_stock_list_func()
            
            if not stock_list:
                return {
                    'success': False,
                    'total': 0,
                    'changes': [],
                    'message': '获取股票列表失败'
                }
            
            # 3. 应用退市过滤
            guard = get_delisting_guard()
            filtered_list = guard.filter_stock_list(stock_list)
            logger.info(f"过滤后: {len(filtered_list)} 只股票")
            
            # 4. 转换为 DataFrame
            new_df = pd.DataFrame(filtered_list)
            
            # 5. 检测变更
            changes = self._detect_changes(old_df, new_df)
            
            # 6. 创建备份
            self._create_backup()
            
            # 7. 原子性写入
            self._atomic_write_parquet(new_df, self.stock_list_path)
            
            # 8. 保存变更记录
            self._save_changes(changes)
            
            # 9. 保存快照
            self._save_snapshot(new_df)
            
            # 10. 输出变更摘要
            self._print_change_summary(changes)
            
            return {
                'success': True,
                'total': len(new_df),
                'changes': changes,
                'message': f'更新成功: {len(new_df)} 只股票'
            }
            
        except Exception as e:
            logger.exception("更新股票列表失败")
            return {
                'success': False,
                'total': 0,
                'changes': [],
                'message': f'更新失败: {str(e)}'
            }
    
    def _print_change_summary(self, changes: List[StockChange]):
        """打印变更摘要"""
        if not changes:
            logger.info("今日无变更")
            return
        
        # 按类型分组
        new_stocks = [c for c in changes if c.change_type == 'new']
        delisted_stocks = [c for c in changes if c.change_type == 'delisted']
        renamed_stocks = [c for c in changes if c.change_type == 'renamed']
        
        logger.info("\n" + "=" * 60)
        logger.info("变更摘要")
        logger.info("=" * 60)
        
        if new_stocks:
            logger.info(f"\n📈 新增股票 ({len(new_stocks)} 只):")
            for c in new_stocks[:10]:  # 只显示前10个
                logger.info(f"   {c.code} - {c.name}")
            if len(new_stocks) > 10:
                logger.info(f"   ... 还有 {len(new_stocks) - 10} 只")
        
        if delisted_stocks:
            logger.info(f"\n📉 退市股票 ({len(delisted_stocks)} 只):")
            for c in delisted_stocks[:10]:
                logger.info(f"   {c.code} - {c.name}")
            if len(delisted_stocks) > 10:
                logger.info(f"   ... 还有 {len(delisted_stocks) - 10} 只")
        
        if renamed_stocks:
            logger.info(f"\n📝 更名股票 ({len(renamed_stocks)} 只):")
            for c in renamed_stocks[:10]:
                logger.info(f"   {c.code}: {c.old_value} → {c.name}")
            if len(renamed_stocks) > 10:
                logger.info(f"   ... 还有 {len(renamed_stocks) - 10} 只")
        
        logger.info("\n" + "=" * 60)

    async def update_stock_list_async(self) -> Dict:
        """
        异步更新股票列表（用于自动更新场景）

        与update_stock_list的区别:
        - 使用异步方式获取数据
        - 适用于已经在异步环境中的调用

        Returns:
            {
                'success': bool,
                'total': int,
                'changes': List[StockChange],
                'message': str
            }
        """
        logger.info("=" * 60)
        logger.info("开始异步更新股票列表")
        logger.info("=" * 60)

        try:
            # 1. 加载当前列表
            old_df = self.load_current_list()
            if old_df is not None:
                logger.info(f"当前列表: {len(old_df)} 只股票")

            # 2. 异步获取最新列表
            logger.info("从数据源异步获取最新股票列表...")
            try:
                from .unified_fetcher import get_unified_fetcher
            except ImportError:
                from unified_fetcher import get_unified_fetcher

            fetcher = await get_unified_fetcher()
            df = await fetcher.fetch_stock_list()

            if df.empty:
                return {
                    'success': False,
                    'total': 0,
                    'changes': [],
                    'message': '获取股票列表失败: 数据为空'
                }

            # 转换为标准格式
            stock_list = []
            for _, row in df.iterrows():
                stock_list.append({
                    'code': str(row.get('code', '')),
                    'name': str(row.get('name', '')),
                    'industry': str(row.get('industry', '')),
                    'exchange': str(row.get('exchange', '')),
                })

            # 3. 应用退市过滤
            guard = get_delisting_guard()
            filtered_list = guard.filter_stock_list(stock_list)
            logger.info(f"过滤后: {len(filtered_list)} 只股票")

            # 4. 转换为 DataFrame
            new_df = pd.DataFrame(filtered_list)

            # 5. 检测变更
            changes = self._detect_changes(old_df, new_df)

            # 6. 创建备份
            self._create_backup()

            # 7. 原子性写入
            self._atomic_write_parquet(new_df, self.stock_list_path)

            # 8. 保存变更记录
            self._save_changes(changes)

            # 9. 保存快照
            self._save_snapshot(new_df)

            # 10. 输出变更摘要
            self._print_change_summary(changes)

            return {
                'success': True,
                'total': len(new_df),
                'changes': changes,
                'message': f'更新成功: {len(new_df)} 只股票'
            }

        except Exception as e:
            logger.exception("异步更新股票列表失败")
            return {
                'success': False,
                'total': 0,
                'changes': [],
                'message': f'更新失败: {str(e)}'
            }

    def get_change_history(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        change_type: Optional[str] = None
    ) -> List[StockChange]:
        """获取变更历史"""
        if not self.change_log_path.exists():
            return []
        
        try:
            with open(self.change_log_path, 'r', encoding='utf-8') as f:
                records = json.load(f)
            
            changes = [StockChange(**r) for r in records]
            
            # 过滤
            if start_date:
                changes = [c for c in changes if c.date >= start_date]
            if end_date:
                changes = [c for c in changes if c.date <= end_date]
            if change_type:
                changes = [c for c in changes if c.change_type == change_type]
            
            return changes
        except Exception as e:
            logger.error(f"加载变更历史失败: {e}")
            return []
    
    def get_snapshots(self, days: int = 30) -> List[StockListSnapshot]:
        """获取历史快照"""
        if not self.snapshot_path.exists():
            return []
        
        try:
            with open(self.snapshot_path, 'r', encoding='utf-8') as f:
                records = json.load(f)
            
            snapshots = [StockListSnapshot(**r) for r in records]
            return snapshots[-days:]
        except Exception as e:
            logger.error(f"加载快照失败: {e}")
            return []


# 便捷函数
def update_stock_list_daily(data_dir: str = "data") -> Dict:
    """
    每日更新股票列表（便捷函数）

    使用示例:
        result = update_stock_list_daily()
        if result['success']:
            print(f"更新成功: {result['total']} 只股票")
            for change in result['changes']:
                print(f"{change.change_type}: {change.code}")
    """
    manager = StockListManager(data_dir)
    return manager.update_stock_list()


def get_stock_codes() -> List[str]:
    """
    统一获取股票代码列表

    优先级: Redis > MySQL > Parquet

    Returns:
        股票代码列表
    """
    try:
        from services.data_service.unified_data_service import UnifiedDataService
        service = UnifiedDataService()
        df = service.get_stock_list_sync()
        if df is not None and not df.empty:
            if 'code' in df.columns:
                return df['code'].tolist()
    except Exception as e:
        logger.warning(f"统一服务获取股票列表失败: {e}")

    try:
        manager = StockListManager()
        df = manager.load_current_list()
        if df is not None and not df.empty:
            if 'code' in df.columns:
                return df['code'].tolist()
    except Exception as e:
        logger.warning(f"StockListManager获取股票列表失败: {e}")

    return []


def get_stock_list_dataframe() -> pd.DataFrame:
    """
    统一获取股票列表DataFrame

    优先级: Redis > MySQL > Parquet

    Returns:
        股票列表DataFrame
    """
    try:
        from services.data_service.unified_data_service import UnifiedDataService
        service = UnifiedDataService()
        df = service.get_stock_list_sync()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning(f"统一服务获取股票列表失败: {e}")

    try:
        manager = StockListManager()
        df = manager.load_current_list()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning(f"StockListManager获取股票列表失败: {e}")

    return pd.DataFrame()


def get_active_stock_codes() -> List[str]:
    """
    获取活跃股票代码列表（已过滤退市/ST）

    Returns:
        活跃股票代码列表
    """
    codes = get_stock_codes()
    if not codes:
        return []

    try:
        from core.delisting_guard import get_delisting_guard
        guard = get_delisting_guard()
        manager = StockListManager()
        df = manager.load_current_list()
        if df is not None and not df.empty:
            filtered = guard.filter_stock_list(df)
            if 'code' in filtered.columns:
                return filtered['code'].tolist()
    except Exception as e:
        logger.warning(f"过滤退市股票失败: {e}")

    return codes


if __name__ == "__main__":
    result = update_stock_list_daily()
    print(json.dumps(result, default=lambda x: asdict(x) if isinstance(x, StockChange) else str(x), ensure_ascii=False, indent=2))
