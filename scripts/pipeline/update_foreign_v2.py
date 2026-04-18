#!/usr/bin/env python3
"""
外盘指数更新 - 微服务版
06:00执行

优化点：
1. 使用微服务架构，统一数据获取接口
2. 支持美股指数、亚洲股指、大宗商品
3. 多数据源自动合并
4. 代理自动检测和配置
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import asyncio
import json
from pathlib import Path
from datetime import datetime
from core.logger import get_logger
from services.data_service.fetchers import (
    fetch_foreign_indices_via_service,
    fetch_commodities_via_service
)

logger = get_logger(__name__)


class ForeignUpdaterV2:
    """外盘更新器 - 微服务版"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)
        self.output_path = self.project_root / "data" / "foreign_index.json"

    async def run(self) -> bool:
        """执行外盘指数更新"""
        self.logger.info("=" * 50)
        self.logger.info("开始外盘指数更新 (微服务版)")
        self.logger.info("=" * 50)

        try:
            # 并发采集外盘指数和大宗商品
            foreign_result, commodity_result = await asyncio.gather(
                fetch_foreign_indices_via_service(),
                fetch_commodities_via_service(),
                return_exceptions=True
            )

            # 处理异常
            if isinstance(foreign_result, Exception):
                self.logger.error(f"外盘指数采集失败: {foreign_result}")
                foreign_result = {'status': 'failed', 'error': str(foreign_result)}

            if isinstance(commodity_result, Exception):
                self.logger.error(f"大宗商品采集失败: {commodity_result}")
                commodity_result = {'status': 'failed', 'error': str(commodity_result)}

            # 合并结果
            combined_data = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'us_index': foreign_result.get('us_index', {'status': 'failed'}),
                'asia_index': foreign_result.get('asia_index', {'status': 'failed'}),
                'commodity': commodity_result,
                'status': 'success'
            }

            # 判断整体状态
            all_failed = (
                foreign_result.get('status') == 'failed' and
                commodity_result.get('status') == 'failed'
            )

            if all_failed:
                combined_data['status'] = 'failed'
                self.logger.error("所有外盘数据采集失败")
                return False

            # 保存到JSON文件
            self._save_to_json(combined_data)

            # 同步到MySQL
            await self._sync_to_mysql(combined_data)

            self.logger.info("外盘指数更新完成")
            return True

        except Exception as e:
            self.logger.error(f"外盘指数更新异常: {e}")
            return False

    def _save_to_json(self, data: dict) -> bool:
        """保存数据到JSON文件"""
        try:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"数据已保存到: {self.output_path}")
            return True
        except Exception as e:
            self.logger.error(f"保存JSON失败: {e}")
            return False

    async def _sync_to_mysql(self, data: dict) -> bool:
        """同步数据到MySQL"""
        self.logger.info("同步数据到MySQL...")
        try:
            # 这里可以调用MySQL同步脚本
            # 暂时只记录日志，实际同步逻辑可以根据需要添加
            self.logger.info("MySQL同步完成 (模拟)")
            return True
        except Exception as e:
            self.logger.error(f"MySQL同步失败: {e}")
            return False


async def main():
    """主函数"""
    updater = ForeignUpdaterV2()
    result = await updater.run()
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    asyncio.run(main())
