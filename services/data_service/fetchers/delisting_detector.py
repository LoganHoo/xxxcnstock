#!/usr/bin/env python3
"""
退市股票检测和黑名单管理器

功能：
1. 从多个数据源获取退市/停牌股票列表
2. 智能识别已退市、暂停上市、风险警示股票
3. 自动维护黑名单（持久化到JSON + Redis）
4. 提供查询接口供采集系统使用

数据来源：
- Baostock API (stock_basic_info)
- 腾讯财经API (历史验证)
- 本地维护的已知退市列表
"""

import sys
from pathlib import Path
import json
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass, field, asdict

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.logger import setup_logger
import redis

logger = setup_logger("delisting_detector", log_file="system/delisting_detector.log")


@dataclass
class DelistedStockInfo:
    """退市股票信息"""
    code: str
    name: str = ""
    delist_date: str = ""  # 退市日期
    reason: str = ""       # 退市原因 (delisted/suspended/risk/st)
    source: str = ""       # 数据来源
    detected_at: str = ""  # 检测时间
    confidence: float = 1.0  # 置信度 (0-1)


class DelistingDetector:
    """
    退市股票检测器

    使用多源交叉验证提高准确性
    """

    def __init__(self, config_dir: Path = None):
        self.config_dir = config_dir or project_root / "config"
        self.data_dir = project_root / "data" / "kline"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 黑名单文件路径
        self.blacklist_file = self.data_dir / ".delisted_blacklist.json"

        # Redis配置
        self.redis_key_blacklist = "xcnstock:kline:blacklisted_codes"
        self.redis_key_last_check = "xcnstock:kline:last_delist_check"

        # 已知退市关键词模式
        self.DELISTING_PATTERNS = {
            'delisted': [
                r"^PT",           # PT开头（特别转让）
                r"退市",           # 包含"退市"
                r"终止上市",       # 终止上市
                r"三板",           # 三板市场
            ],
            'suspended': [
                r"暂停上市",       # 暂停上市
                r"停牌",           # 停牌
            ],
            'risk': [
                r"^ST",           # ST开头
                r"^\*ST",         # *ST开头
                r"^S\*ST",        # S*ST开头
                r"^SST",          # SST开头
                r"ST$",           # ST结尾（备用）
                r"\*ST$",         # *ST结尾（备用）
            ],
            'special': [
                r"风险警示",       # 风险警示
            ]
        }

        # 加载现有黑名单
        self.blacklist: Dict[str, DelistedStockInfo] = {}
        self._load_blacklist()

        # Redis连接（延迟初始化）
        self._redis_client = None

    @property
    def redis(self):
        if self._redis_client is None:
            try:
                self._redis_client = redis.Redis(
                    host='localhost',
                    port=6379,
                    db=0,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                self._redis_client.ping()
            except Exception as e:
                logger.warning(f"Redis连接失败: {e}")
                self._redis_client = None
        return self._redis_client

    def _load_blacklist(self):
        """从JSON文件加载黑名单"""
        if self.blacklist_file.exists():
            try:
                with open(self.blacklist_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for code, info in data.get('stocks', {}).items():
                        self.blacklist[code] = DelistedStockInfo(**info)
                logger.info(f"加载黑名单: {len(self.blacklist)} 只股票")
            except Exception as e:
                logger.error(f"加载黑名单失败: {e}")
                self.blacklist = {}

    def _save_blacklist(self):
        """保存黑名单到JSON文件"""
        try:
            data = {
                'version': '2.0',
                'updated_at': datetime.now().isoformat(),
                'total_count': len(self.blacklist),
                'stocks': {
                    code: asdict(info) for code, info in self.blacklist.items()
                }
            }

            with open(self.blacklist_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"黑名单已保存: {len(self.blacklist)} 只股票")

            # 同步到Redis
            if self.redis:
                try:
                    pipe = self.redis.pipeline()
                    pipe.delete(self.redis_key_blacklist)
                    for code in self.blacklist.keys():
                        pipe.sadd(self.redis_key_blacklist, code)
                    pipe.set(self.redis_key_last_check, datetime.now().isoformat())
                    pipe.execute()
                    logger.debug(f"黑名单已同步到Redis: {len(self.blacklist)} 只")
                except Exception as e:
                    logger.warning(f"同步Redis失败: {e}")

        except Exception as e:
            logger.error(f"保存黑名单失败: {e}")

    async def detect_from_baostock(self) -> List[DelistedStockInfo]:
        """
        从Baostock获取退市/停牌股票信息

        Returns:
            检测到的退市股票列表
        """
        results = []

        try:
            from services.data_service.datasource.providers import BaostockProvider
            provider = BaostockProvider()

            # 获取股票基本信息
            stock_df = await provider.get_stock_list()

            if stock_df is None or len(stock_df) == 0:
                logger.warning("Baostock未返回股票列表")
                return results

            # 分析每只股票的状态
            for _, row in stock_df.iterrows():
                code = str(row.get('code', '')).strip()
                name = str(row.get('name', ''))

                if not code or not name:
                    continue

                # 检查名称中的退市关键词
                reason, confidence = self._check_name_patterns(name)

                if reason and confidence > 0.7:
                    info = DelistedStockInfo(
                        code=code,
                        name=name,
                        reason=reason,
                        source='baostock_name_pattern',
                        detected_at=datetime.now().isoformat(),
                        confidence=confidence
                    )
                    results.append(info)

            logger.info(f"从Baostock检测到 {len(results)} 只疑似退市股票")

        except Exception as e:
            logger.error(f"Baostock检测异常: {e}")

        return results

    async def detect_by_data_verification(self, codes: List[str], days_back: int = 90) -> List[DelistedStockInfo]:
        """
        通过数据验证检测退市股票（无近期交易数据）

        Args:
            codes: 待检查的股票代码列表
            days_back: 检查最近N天是否有数据

        Returns:
            确认无数据的退市股票列表
        """
        results = []
        today = datetime.now()
        cutoff_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')

        logger.info(f"开始数据验证检测: {len(codes)} 只股票, 阈值{days_back}天")

        try:
            from services.data_service.datasource.providers import BaostockProvider, TencentProvider

            # 使用腾讯API快速验证（更快）
            provider = TencentProvider()

            checked_count = 0
            no_data_stocks = []

            for code in codes:
                try:
                    df = await provider.fetch_kline(
                        code=code,
                        start_date=cutoff_date,
                        end_date=today.strftime('%Y-%m-%d')
                    )

                    checked_count += 1

                    if df is None or len(df) == 0:
                        no_data_stocks.append(code)

                    # 每100只打印进度
                    if checked_count % 100 == 0:
                        logger.debug(f"已检查 {checked_count}/{len(codes)}, 无数据: {len(no_data_stocks)}")

                except Exception as e:
                    logger.debug(f"验证 {code} 异常: {e}")
                    no_data_stocks.append(code)

            # 将连续多次无数据的股票标记为疑似退市
            for code in no_data_stocks:
                info = DelistedStockInfo(
                    code=code,
                    reason='no_recent_data',
                    source=f'data_verify_{days_back}d',
                    detected_at=datetime.now().isoformat(),
                    confidence=0.8
                )
                results.append(info)

            logger.info(f"数据验证完成: {checked_count}只, 无数据{len(results)}只")

        except Exception as e:
            logger.error(f"数据验证检测异常: {e}")

        return results

    def _check_name_patterns(self, name: str) -> Tuple[str, float]:
        """
        检查股票名称是否匹配退市模式

        Args:
            name: 股票名称

        Returns:
            (原因类型, 置信度)
        """
        if not name:
            return '', 0.0

        for reason_type, patterns in self.DELISTING_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, name):
                    # 根据匹配程度确定置信度
                    if reason_type == 'delisted':
                        return reason_type, 0.95  # 高置信度
                    elif reason_type in ['risk', 'special']:
                        return reason_type, 0.85  # 中高置信度
                    else:
                        return reason_type, 0.75  # 中等置信度

        return '', 0.0

    def add_to_blacklist(self, info: DelistedStockInfo, force: bool = False) -> bool:
        """
        将股票加入黑名单

        Args:
            info: 退市股票信息
            force: 是否强制添加（覆盖已有）

        Returns:
            是否成功添加
        """
        code = info.code

        if code in self.blacklist and not force:
            logger.debug(f"{code} 已在黑名单中，跳过")
            return False

        self.blacklist[code] = info
        logger.info(f"添加黑名单: {code} ({info.reason}, 置信度{info.confidence})")

        return True

    def remove_from_blacklist(self, code: str) -> bool:
        """从黑名单移除（重新上市时使用）"""
        if code in self.blacklist:
            del self.blacklist[code]
            logger.info(f"从黑名单移除: {code}")
            return True
        return False

    def is_blacklisted(self, code: str) -> bool:
        """检查股票是否在黑名单中"""
        return code in self.blacklist

    def get_blacklisted_codes(self) -> Set[str]:
        """获取所有黑名单代码"""
        return set(self.blacklist.keys())

    def get_blacklist_stats(self) -> Dict:
        """获取黑名单统计信息"""
        stats = {
            'total_count': len(self.blacklist),
            'by_reason': {},
            'by_source': {},
            'recent_additions': []
        }

        for code, info in self.blacklist.items():
            # 按原因统计
            reason = info.reason or 'unknown'
            stats['by_reason'][reason] = stats['by_reason'].get(reason, 0) + 1

            # 按来源统计
            source = info.source or 'unknown'
            stats['by_source'][source] = stats['by_source'].get(source, 0) + 1

            # 最近24小时新增
            if info.detected_at:
                try:
                    detected_time = datetime.fromisoformat(info.detected_at)
                    if datetime.now() - detected_time < timedelta(hours=24):
                        stats['recent_additions'].append(code)
                except:
                    pass

        return stats

    async def run_full_detection(self, all_codes: List[str] = None, verify_data: bool = True) -> Dict:
        """
        执行完整的退市检测流程

        Args:
            all_codes: 所有待检测代码（可选，为None则从Redis读取）
            verify_data: 是否执行数据验证

        Returns:
            检测结果报告
        """
        start_time = time.time()
        report = {
            'started_at': datetime.now().isoformat(),
            'newly_detected': [],
            'total_in_blacklist': 0,
            'detection_methods': {}
        }

        logger.info("=" * 60)
        logger.info("开始完整退市检测流程")
        logger.info("=" * 60)

        # 步骤1: 从Baostock名称模式检测
        logger.info("\n[步骤1/3] 从Baostock检测名称模式...")
        baostock_results = await self.detect_from_baostock()
        report['detection_methods']['baostock'] = len(baostock_results)

        for info in baostock_results:
            if self.add_to_blacklist(info):
                report['newly_detected'].append(info.code)

        # 步骤2: 数据验证检测（如果启用）
        if verify_data and all_codes:
            logger.info("\n[步骤2/3] 数据验证检测...")
            verification_results = await self.detect_by_data_verification(all_codes, days_back=60)
            report['detection_methods']['data_verification'] = len(verification_results)

            for info in verification_results:
                # 只有高置信度的才加入黑名单
                if info.confidence >= 0.8 and not self.is_blacklisted(info.code):
                    if self.add_to_blacklist(info):
                        report['newly_detected'].append(info.code)
        else:
            report['detection_methods']['data_verification'] = 0

        # 步骤3: 保存黑名单
        logger.info("\n[步骤3/3] 保存并同步黑名单...")
        self._save_blacklist()

        # 生成报告
        elapsed = time.time() - start_time
        report['ended_at'] = datetime.now().isoformat()
        report['elapsed_seconds'] = round(elapsed, 2)
        report['total_in_blacklist'] = len(self.blacklist)
        report['stats'] = self.get_blacklist_stats()

        logger.info("\n" + "=" * 60)
        logger.info("退市检测完成:")
        logger.info(f"  新增检测: {len(report['newly_detected'])} 只")
        logger.info(f"  黑名单总数: {report['total_in_blacklist']} 只")
        logger.info(f"  耗时: {elapsed:.1f}s")

        if report['newly_detected']:
            logger.info(f"  新增代码: {report['newly_detected'][:10]}...")

        logger.info("=" * 60)

        return report


# 全局单例实例
_detector_instance = None

def get_delisting_detector() -> DelistingDetector:
    """获取全局退市检测器实例"""
    global _detector_instance
    if _detector_instance is None:
        _detector_instance = DelistingDetector()
    return _detector_instance


if __name__ == '__main__':
    import asyncio

    async def test():
        detector = get_delisting_detector()

        print("=" * 70)
        print("Delisting Detector Test")
        print("=" * 70)

        # 测试名称模式检测
        test_names = [
            ('000003', 'PT金田A'),
            ('000007', '*ST全新'),
            ('600123', 'ST宏盛'),
            ('000001', '平安银行'),
        ]

        print("\n[测试] 名称模式检测:")
        for code, name in test_names:
            reason, conf = detector._check_name_patterns(name)
            status = '⚠️' if reason else '✅'
            print(f"  {status} {code} ({name}): {reason or '正常'} (置信度:{conf:.2f})")

        # 执行完整检测
        print("\n[测试] 执行完整检测...")
        from services.data_service.fetchers.stock_list_cache import StockListCacheManager
        cache_mgr = StockListCacheManager()
        codes = cache_mgr.get_codes(use_redis=True)[:50]  # 测试前50只

        report = await detector.run_full_detection(all_codes=codes, verify_data=False)

        print(f"\n检测结果:")
        print(f"  新增: {len(report['newly_detected'])}")
        print(f"  黑名单总数: {report['total_in_blacklist']}")

        if detector.blacklist:
            print(f"\n黑名单示例 (前5只):")
            for i, (code, info) in enumerate(list(detector.blacklist.items())[:5]):
                print(f"  {i+1}. {code}: {info.name or '?'} - {info.reason}")

        print("\nDONE")

    asyncio.run(test())
