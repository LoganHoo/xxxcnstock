#!/usr/bin/env python3
"""
大盘指数采集 - 微服务版
16:05执行（收盘后）

优化点：
1. 使用微服务架构，统一数据获取接口
2. 自动清除代理设置，避免网络连接问题
3. 支持重试机制
4. 自动检测数据新鲜度
5. 失败时自动切换到手动更新模式
"""
import sys
import os

# 清除代理设置，避免网络连接问题
for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    if proxy_var in os.environ:
        del os.environ[proxy_var]

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import asyncio
from pathlib import Path
from datetime import datetime
from core.logger import get_logger
from services.data_service.fetchers import fetch_domestic_indices_via_service

logger = get_logger(__name__)


class IndexFetcherV2:
    """指数采集器 - 微服务增强版"""

    # 指数配置列表
    INDICES = [
        ('sh000001', '000001', '上证指数'),
        ('sz399001', '399001', '深证成指'),
        ('sz399006', '399006', '创业板指'),
        ('sh000300', '000300', '沪深300'),
        ('sh000016', '000016', '上证50'),
        ('sh000905', '000905', '中证500'),
    ]

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)
        self.index_dir = self.project_root / "data" / "index"
        self.index_dir.mkdir(parents=True, exist_ok=True)

    async def run(self, max_retries: int = 3, use_manual_fallback: bool = True) -> bool:
        """执行大盘指数采集

        Args:
            max_retries: 最大重试次数
            use_manual_fallback: 失败时是否使用手动更新作为备选

        Returns:
            bool: 采集成功返回True
        """
        self.logger.info("=" * 50)
        self.logger.info("开始大盘指数采集任务 (微服务版)")
        self.logger.info("=" * 50)

        success = False
        for attempt in range(max_retries):
            try:
                # 使用微服务获取国内指数数据
                result = await fetch_domestic_indices_via_service(data_dir=self.index_dir)

                if result.get('status') in ['success', 'partial']:
                    self.logger.info(f"指数采集成功: {result.get('fresh_count', 'N/A')}")
                    success = True
                    break
                else:
                    self.logger.warning(f"第{attempt + 1}次尝试失败: {result.get('message', '未知错误')}")

            except Exception as e:
                self.logger.error(f"第{attempt + 1}次尝试异常: {e}")

            if attempt < max_retries - 1:
                import time
                time.sleep(2 ** attempt)  # 指数退避

        if not success and use_manual_fallback:
            self.logger.info("尝试手动更新作为备选...")
            # 手动更新逻辑已在微服务中实现
            try:
                result = await fetch_domestic_indices_via_service(data_dir=self.index_dir)
                if result.get('status') in ['success', 'partial']:
                    success = True
            except Exception as e:
                self.logger.error(f"手动更新也失败: {e}")

        # 同步到MySQL
        if success:
            await self._sync_to_mysql()

        return success

    async def _sync_to_mysql(self) -> bool:
        """同步指数数据到MySQL"""
        self.logger.info("同步数据到MySQL...")
        try:
            # 调用同步脚本
            script_path = self.project_root / "scripts" / "sync_index_to_mysql.py"
            if script_path.exists():
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, str(script_path),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)

                if proc.returncode == 0:
                    self.logger.info("MySQL同步成功")
                    return True
                else:
                    self.logger.warning(f"MySQL同步失败: {stderr.decode()}")
            else:
                self.logger.warning(f"同步脚本不存在: {script_path}")
        except Exception as e:
            self.logger.error(f"MySQL同步异常: {e}")

        return False


async def main():
    """主函数"""
    fetcher = IndexFetcherV2()
    result = await fetcher.run()
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    asyncio.run(main())
