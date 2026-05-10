#!/usr/bin/env python3
"""
股票列表缓存管理器

架构:
- Parquet: 主存储（持久化、备份）
- Redis: 高速缓存（供K线采集读取）
- 自动更新: 数据过期时自动从API获取最新数据

使用:
    # 每日更新时
    manager = StockListCacheManager()
    manager.sync_to_redis()  # Parquet → Redis

    # K线采集时（自动更新模式）
    manager = StockListCacheManager(auto_update=True)
    codes = manager.get_codes()  # 过期自动更新

    # K线采集时（手动模式）
    codes = manager.get_codes(auto_update=False)  # 只读取，不自动更新
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import json
import os
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import datetime, timedelta

from core.logger import setup_logger
from core.config import get_settings

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = setup_logger("stock_list_cache", log_file="system/stock_list_cache.log")

# 全局冷却时间控制（避免频繁更新）
_last_auto_update_time: Optional[datetime] = None
_AUTO_UPDATE_COOLDOWN_MINUTES = 60  # 自动更新冷却时间（分钟）


class StockListCacheManager:
    """
    股票列表缓存管理器

    职责:
    1. 从Parquet加载股票列表
    2. 同步到Redis
    3. 从Redis快速读取
    4. 故障降级到Parquet
    5. 节假日智能处理（TTL延长至7天）
    """

    REDIS_KEY = "xcnstock:stock_list"
    REDIS_HASH_KEY = "xcnstock:stock_map"  # 哈希存储，支持按code查询
    REDIS_TTL = 86400 * 7  # 7天过期（覆盖长节假日）
    MAX_DATA_AGE_DAYS = 7  # 最大数据新鲜度（天）
    AUTO_UPDATE_THRESHOLD_DAYS = 3  # 自动更新阈值（天）

    def __init__(self, data_dir: str = None, redis_client=None, auto_update: bool = False):
        settings = get_settings()
        self.data_dir = Path(data_dir or settings.DATA_DIR)
        self.stock_list_path = self.data_dir / "stock_list.parquet"
        self.auto_update = auto_update

        # Redis客户端
        self._redis = redis_client
        if self._redis is None and REDIS_AVAILABLE:
            self._redis = self._init_redis()
    
    def _init_redis(self) -> Optional[redis.Redis]:
        """初始化Redis连接"""
        try:
            settings = get_settings()
            client = redis.Redis(
                host=getattr(settings, 'REDIS_HOST', 'localhost'),
                port=getattr(settings, 'REDIS_PORT', 6379),
                password=getattr(settings, 'REDIS_PASSWORD', None),
                db=getattr(settings, 'REDIS_DB', 0),
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            client.ping()
            logger.info("Redis连接成功")
            return client
        except Exception as e:
            logger.warning(f"Redis连接失败: {e}，将使用降级方案")
            return None
    
    def load_from_parquet(self) -> pd.DataFrame:
        """从Parquet加载股票列表"""
        if not self.stock_list_path.exists():
            raise FileNotFoundError(f"股票列表不存在: {self.stock_list_path}")
        
        df = pd.read_parquet(self.stock_list_path)
        logger.info(f"从Parquet加载 {len(df)} 只股票")
        return df
    
    def sync_to_redis(self, df: pd.DataFrame = None) -> bool:
        """
        同步股票列表到Redis
        
        Args:
            df: 股票列表DataFrame，None则从Parquet加载
        
        Returns:
            是否成功
        """
        if self._redis is None:
            logger.warning("Redis不可用，跳过同步")
            return False
        
        try:
            if df is None:
                df = self.load_from_parquet()
            
            # 1. 存储为JSON列表（用于全量获取）
            stock_list = df.to_dict('records')
            self._redis.setex(
                self.REDIS_KEY,
                self.REDIS_TTL,
                json.dumps(stock_list)
            )
            
            # 2. 存储为Hash（用于单只股票查询）
            pipe = self._redis.pipeline()
            pipe.delete(self.REDIS_HASH_KEY)
            for _, row in df.iterrows():
                code = str(row['code'])
                pipe.hset(self.REDIS_HASH_KEY, code, json.dumps(row.to_dict()))
            pipe.expire(self.REDIS_HASH_KEY, self.REDIS_TTL)
            pipe.execute()
            
            # 3. 存储元数据
            self._redis.setex(
                f"{self.REDIS_KEY}:meta",
                self.REDIS_TTL,
                json.dumps({
                    'count': len(df),
                    'sync_time': datetime.now().isoformat(),
                    'source': 'parquet'
                })
            )
            
            logger.info(f"同步 {len(df)} 只股票到Redis成功")
            return True
            
        except Exception as e:
            logger.error(f"同步到Redis失败: {e}")
            return False
    
    def get_codes_from_redis(self) -> List[str]:
        """
        从Redis获取所有股票代码

        Returns:
            股票代码列表（失败则返回空列表）
        """
        if self._redis is None:
            logger.debug("Redis不可用，返回空列表")
            return []

        try:
            codes = self._redis.hkeys(self.REDIS_HASH_KEY)
            if codes:
                return sorted(codes)

            data = self._redis.get(self.REDIS_KEY)
            if data:
                stock_list = json.loads(data)
                return sorted([s['code'] for s in stock_list])

            return []

        except Exception as e:
            logger.error(f"从Redis读取失败: {e}")
            return []

    def get_stock_list_from_redis(self) -> Optional[pd.DataFrame]:
        """
        从Redis获取完整股票列表DataFrame

        Returns:
            股票列表DataFrame
        """
        if self._redis is None:
            logger.debug("Redis不可用")
            return None

        try:
            data = self._redis.get(self.REDIS_KEY)
            if data:
                stock_list = json.loads(data)
                df = pd.DataFrame(stock_list)
                logger.info(f"从Redis获取 {len(df)} 只股票")
                return df
            return None
        except Exception as e:
            logger.error(f"从Redis获取股票列表失败: {e}")
            return None
    
    def get_codes(self, use_redis: bool = True) -> List[str]:
        """
        获取股票代码列表（自动降级）
        
        Args:
            use_redis: 是否优先使用Redis
        
        Returns:
            股票代码列表
        """
        # 1. 尝试从Redis获取
        if use_redis and self._redis:
            codes = self.get_codes_from_redis()
            if codes:
                logger.debug(f"从Redis获取 {len(codes)} 只股票")
                return codes
        
        # 2. 降级到Parquet
        try:
            df = self.load_from_parquet()
            codes = df['code'].tolist()
            logger.info(f"从Parquet获取 {len(codes)} 只股票")
            return codes
        except Exception as e:
            logger.error(f"从Parquet加载失败: {e}")
            return []
    
    def get_stock_info(self, code: str) -> Optional[Dict]:
        """
        获取单只股票信息
        
        Args:
            code: 股票代码
        
        Returns:
            股票信息字典
        """
        # 1. 尝试从Redis获取
        if self._redis:
            try:
                data = self._redis.hget(self.REDIS_HASH_KEY, code)
                if data:
                    return json.loads(data)
            except Exception as e:
                logger.debug(f"从Redis获取单只股票失败: {e}")
        
        # 2. 降级到Parquet
        try:
            df = self.load_from_parquet()
            row = df[df['code'] == code]
            if not row.empty:
                return row.iloc[0].to_dict()
        except Exception as e:
            logger.error(f"从Parquet获取单只股票失败: {e}")
        
        return None
    
    def is_synced(self) -> bool:
        """检查Redis是否已同步"""
        if self._redis is None:
            return False
        
        try:
            return self._redis.exists(self.REDIS_KEY) > 0
        except:
            return False
    
    def get_sync_info(self) -> Optional[Dict]:
        """获取同步信息"""
        if self._redis is None:
            return None

        try:
            data = self._redis.get(f"{self.REDIS_KEY}:meta")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"获取同步信息失败: {e}")

        return None

    def check_data_freshness(self) -> Dict:
        """
        检查数据新鲜度

        Returns:
            {
                'is_fresh': bool,      # 数据是否新鲜
                'age_days': int,       # 数据年龄（天）
                'sync_time': str,      # 同步时间
                'warning': str         # 警告信息（如有）
            }
        """
        sync_info = self.get_sync_info()
        if not sync_info:
            return {
                'is_fresh': False,
                'age_days': -1,
                'sync_time': None,
                'warning': 'Redis未同步，使用Parquet数据'
            }

        try:
            sync_time = datetime.fromisoformat(sync_info['sync_time'])
            age = datetime.now() - sync_time
            age_days = age.days

            is_fresh = age_days <= self.MAX_DATA_AGE_DAYS

            warning = None
            if age_days > self.MAX_DATA_AGE_DAYS:
                warning = f'数据已过期 {age_days} 天，请尽快更新股票列表'
            elif age_days > 3:
                warning = f'数据已 {age_days} 天未更新，建议在交易日更新'

            return {
                'is_fresh': is_fresh,
                'age_days': age_days,
                'sync_time': sync_info['sync_time'],
                'warning': warning
            }
        except Exception as e:
            logger.error(f"检查数据新鲜度失败: {e}")
            return {
                'is_fresh': False,
                'age_days': -1,
                'sync_time': sync_info.get('sync_time'),
                'warning': f'检查失败: {e}'
            }

    def get_codes_with_freshness_check(self, use_redis: bool = True) -> Dict:
        """
        获取股票代码列表（带新鲜度检查）

        Args:
            use_redis: 是否优先使用Redis

        Returns:
            {
                'codes': List[str],        # 股票代码列表
                'source': str,             # 数据来源 (redis/parquet)
                'freshness': Dict          # 新鲜度信息
            }
        """
        freshness = self.check_data_freshness()

        # 如果Redis数据过期，强制使用Parquet
        if use_redis and freshness['is_fresh']:
            codes = self.get_codes_from_redis()
            if codes:
                return {
                    'codes': codes,
                    'source': 'redis',
                    'freshness': freshness
                }

        # 降级到Parquet
        try:
            df = self.load_from_parquet()
            codes = df['code'].tolist()

            # 检查Parquet文件修改时间
            import os
            stat = os.stat(self.stock_list_path)
            file_mtime = datetime.fromtimestamp(stat.st_mtime)
            file_age_days = (datetime.now() - file_mtime).days

            parquet_freshness = {
                'is_fresh': file_age_days <= self.MAX_DATA_AGE_DAYS,
                'age_days': file_age_days,
                'sync_time': file_mtime.isoformat(),
                'warning': None
            }

            if file_age_days > self.MAX_DATA_AGE_DAYS:
                parquet_freshness['warning'] = f'Parquet数据已 {file_age_days} 天未更新，请尽快执行更新'

            return {
                'codes': codes,
                'source': 'parquet',
                'freshness': parquet_freshness
            }
        except Exception as e:
            logger.error(f"从Parquet加载失败: {e}")
            return {
                'codes': [],
                'source': 'none',
                'freshness': {'error': str(e)}
            }

    def _check_auto_update_cooldown(self) -> bool:
        """检查自动更新冷却时间"""
        global _last_auto_update_time

        if _last_auto_update_time is None:
            return True

        elapsed = datetime.now() - _last_auto_update_time
        return elapsed >= timedelta(minutes=_AUTO_UPDATE_COOLDOWN_MINUTES)

    def _update_from_api(self) -> bool:
        """
        从API获取最新数据并更新（完整管理流程）

        流程:
        1. 获取最新列表
        2. 剔除无效股票（退市、ST等）
        3. 检测变更（新增、退市、更名）
        4. 保存变更记录
        5. 更新Parquet和Redis

        Returns:
            是否成功
        """
        global _last_auto_update_time

        try:
            logger.info("🔄 自动更新股票列表...")

            # 加载当前列表用于对比
            old_df = self.load_from_parquet()
            if old_df is not None:
                logger.info(f"当前列表: {len(old_df)} 只股票")

            # 使用同步方式获取最新列表（避免异步嵌套问题）
            import asyncio
            try:
                from .unified_fetcher import get_unified_fetcher
            except ImportError:
                from unified_fetcher import get_unified_fetcher
            from core.delisting_guard import get_delisting_guard

            async def _fetch():
                fetcher = await get_unified_fetcher()
                return await fetcher.fetch_stock_list()

            # 在新线程中运行异步代码
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, _fetch())
                df = future.result(timeout=60)

            if df.empty:
                logger.error("❌ 从API获取股票列表失败: 数据为空")
                return False

            # 过滤指数，只保留个股
            if 'code' in df.columns:
                df = df[df['code'].str.match(r'^\d{6}$', na=False)]

            # 应用退市过滤
            guard = get_delisting_guard()
            df = guard.filter_stock_list(df)
            logger.info(f"过滤后: {len(df)} 只股票")

            # 检测变更
            changes = self._detect_changes(old_df, df)
            if changes:
                logger.info(f"检测到 {len(changes)} 条变更")
                for change in changes[:5]:
                    logger.info(f"  {change}")

            # 保存到Parquet
            self._atomic_write_parquet(df, self.stock_list_path)
            logger.info(f"✅ Parquet更新成功: {len(df)} 只")

            # 同步到Redis
            if self.sync_to_redis(df):
                logger.info("✅ Redis同步成功")
            else:
                logger.warning("⚠️ Redis同步失败")

            # 更新冷却时间
            _last_auto_update_time = datetime.now()

            return True

        except Exception as e:
            logger.error(f"❌ 自动更新失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False

    def _detect_changes(
        self,
        old_df: Optional[pd.DataFrame],
        new_df: pd.DataFrame
    ) -> List[Dict]:
        """检测股票列表变更"""
        from datetime import date
        changes = []
        today = date.today().isoformat()

        if old_df is None or old_df.empty:
            # 首次运行，所有股票都是新增的
            for _, row in new_df.iterrows():
                changes.append({
                    'date': today,
                    'code': row['code'],
                    'name': row.get('name', ''),
                    'change_type': 'new'
                })
            return changes

        # 转换为字典便于查找
        old_stocks = {row['code']: row for _, row in old_df.iterrows()}
        new_stocks = {row['code']: row for _, row in new_df.iterrows()}

        # 检测新增股票
        for code, row in new_stocks.items():
            if code not in old_stocks:
                changes.append({
                    'date': today,
                    'code': code,
                    'name': row.get('name', ''),
                    'change_type': 'new'
                })

        # 检测退市股票
        for code, row in old_stocks.items():
            if code not in new_stocks:
                changes.append({
                    'date': today,
                    'code': code,
                    'name': row.get('name', ''),
                    'change_type': 'delisted'
                })

        # 检测更名股票
        for code in set(old_stocks.keys()) & set(new_stocks.keys()):
            old_name = old_stocks[code].get('name', '')
            new_name = new_stocks[code].get('name', '')
            if old_name != new_name:
                changes.append({
                    'date': today,
                    'code': code,
                    'name': new_name,
                    'change_type': 'renamed',
                    'old_value': old_name,
                    'new_value': new_name
                })

        return changes

    def _atomic_write_parquet(self, df: pd.DataFrame, filepath: Path) -> bool:
        """原子性写入Parquet文件"""
        try:
            temp_file = filepath.with_suffix('.tmp')
            df.to_parquet(temp_file, index=False)
            temp_file.replace(filepath)
            return True
        except Exception as e:
            logger.error(f"原子写入失败: {e}")
            return False

    def get_codes_auto(self, force_update: bool = False) -> List[str]:
        """
        获取股票代码列表（支持自动更新）

        Args:
            force_update: 强制更新

        Returns:
            股票代码列表
        """
        # 检查数据新鲜度
        result = self.get_codes_with_freshness_check(use_redis=True)
        freshness = result['freshness']

        # 判断是否需要更新
        need_update = force_update

        if not need_update and freshness['age_days'] >= 0:
            # 数据超过阈值，需要更新
            if freshness['age_days'] > self.AUTO_UPDATE_THRESHOLD_DAYS:
                need_update = True
                logger.info(f"数据已 {freshness['age_days']} 天未更新，触发自动更新")

        # 检查冷却时间
        if need_update and not self._check_auto_update_cooldown():
            logger.warning("自动更新冷却中，跳过更新")
            need_update = False

        # 执行自动更新
        if need_update and self.auto_update:
            if self._update_from_api():
                # 更新成功后重新获取
                result = self.get_codes_with_freshness_check(use_redis=True)
            else:
                logger.warning("自动更新失败，使用现有数据")

        # 返回结果
        codes = result['codes']
        source = result['source']

        if codes:
            logger.info(f"📋 从{source}获取: {len(codes)} 只")
            if freshness.get('warning'):
                logger.warning(f"⚠️  {freshness['warning']}")

        return codes


# 便捷函数
def get_stock_codes(use_redis: bool = True, auto_update: bool = False) -> List[str]:
    """
    获取股票代码列表（便捷函数）

    Args:
        use_redis: 是否优先使用Redis
        auto_update: 数据过期时是否自动更新
    """
    if auto_update:
        manager = StockListCacheManager(auto_update=True)
        return manager.get_codes_auto()
    else:
        manager = StockListCacheManager()
        return manager.get_codes(use_redis=use_redis)


def sync_stock_list_to_redis(df: pd.DataFrame = None) -> bool:
    """同步股票列表到Redis（便捷函数）"""
    manager = StockListCacheManager()
    return manager.sync_to_redis(df)


def update_stock_list_auto() -> bool:
    """
    自动更新股票列表（便捷函数）

    Returns:
        是否成功
    """
    manager = StockListCacheManager(auto_update=True)
    return manager._update_from_api()


if __name__ == "__main__":
    # 测试
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--auto':
        # 自动更新模式测试
        print("测试自动更新模式...")
        manager = StockListCacheManager(auto_update=True)
        codes = manager.get_codes_auto(force_update=True)
        print(f"✓ 获取 {len(codes)} 只股票")
    else:
        # 普通模式测试
        manager = StockListCacheManager()

        # 同步到Redis
        if manager.sync_to_redis():
            print("✓ 同步成功")

            # 从Redis读取
            codes = manager.get_codes_from_redis()
            print(f"✓ 从Redis获取 {len(codes)} 只股票")
            print(f"  前5只: {codes[:5]}")

            # 获取同步信息
            info = manager.get_sync_info()
            if info:
                print(f"✓ 同步时间: {info['sync_time']}")
        else:
            print("✗ 同步失败")
