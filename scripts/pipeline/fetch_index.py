"""大盘指数采集 - 16:05执行（收盘后）

优化点：
1. 自动清除代理设置，避免网络连接问题
2. 支持重试机制
3. 自动检测数据新鲜度
4. 失败时自动切换到手动更新模式
"""
import sys
import os

# 清除代理设置，避免网络连接问题
for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    if proxy_var in os.environ:
        del os.environ[proxy_var]

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import subprocess
import time
from pathlib import Path
from datetime import datetime, date
from core.logger import get_logger
import polars as pl


class IndexFetcher:
    """指数采集器 - 增强版"""

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

    def check_data_freshness(self) -> dict:
        """检查指数数据新鲜度
        
        逻辑：
        - 收盘前（15:00前）：昨天数据也算最新
        - 收盘后（15:00后）：需要今天数据
        
        Returns:
            dict: 各指数的数据状态
        """
        from datetime import time
        now = datetime.now()
        today = now.date()
        market_close = time(15, 0)  # 收盘时间
        
        # 收盘后需要今天数据，收盘前昨天数据也算最新
        is_after_close = now.time() >= market_close
        max_acceptable_lag = 0 if is_after_close else 1
        
        status = {}
        
        for symbol, code, name in self.INDICES:
            parquet_file = self.index_dir / f"{code}.parquet"
            if parquet_file.exists():
                try:
                    df = pl.read_parquet(parquet_file)
                    latest_date = df['trade_date'].max()
                    latest_date = datetime.strptime(str(latest_date), '%Y-%m-%d').date() if isinstance(latest_date, str) else latest_date
                    days_diff = (today - latest_date).days
                    
                    status[code] = {
                        'name': name,
                        'latest_date': latest_date,
                        'days_diff': days_diff,
                        'is_fresh': days_diff <= max_acceptable_lag,
                        'rows': len(df),
                        'is_after_close': is_after_close
                    }
                except Exception as e:
                    status[code] = {
                        'name': name,
                        'error': str(e),
                        'is_fresh': False
                    }
            else:
                status[code] = {
                    'name': name,
                    'error': '文件不存在',
                    'is_fresh': False
                }
        
        return status

    def fetch_via_akshare(self) -> bool:
        """使用akshare采集指数数据"""
        self.logger.info("尝试使用akshare采集大盘指数...")
        
        try:
            script_path = self.project_root / "scripts" / "fetch_index_data.py"
            if not script_path.exists():
                self.logger.error(f"采集脚本不存在: {script_path}")
                return False

            # 使用子进程执行，确保环境变量被正确传递
            env = os.environ.copy()
            for key in list(env.keys()):
                if 'proxy' in key.lower():
                    del env[key]
            
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=300,
                env=env,
                cwd=str(self.project_root)
            )
            
            if result.returncode == 0:
                self.logger.info("akshare采集成功")
                return True
            else:
                self.logger.warning(f"akshare采集失败: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error("akshare采集超时")
            return False
        except Exception as e:
            self.logger.error(f"akshare采集异常: {e}")
            return False

    def fetch_via_manual(self) -> bool:
        """使用手动更新脚本"""
        self.logger.info("尝试使用手动更新脚本...")
        
        try:
            script_path = self.project_root / "scripts" / "update_index_manual.py"
            if not script_path.exists():
                self.logger.error(f"手动更新脚本不存在: {script_path}")
                return False

            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=str(self.project_root)
            )
            
            if result.returncode == 0:
                self.logger.info("手动更新成功")
                return True
            else:
                self.logger.warning(f"手动更新失败: {result.stderr}")
                return False

        except Exception as e:
            self.logger.error(f"手动更新异常: {e}")
            return False

    def run(self, max_retries: int = 3, use_manual_fallback: bool = True) -> bool:
        """执行大盘指数采集
        
        Args:
            max_retries: 最大重试次数
            use_manual_fallback: 失败时是否使用手动更新作为备选
            
        Returns:
            bool: 采集成功返回True
        """
        self.logger.info("=" * 50)
        self.logger.info("开始大盘指数采集任务")
        self.logger.info("=" * 50)
        
        # 1. 检查当前数据新鲜度
        self.logger.info("\n1. 检查当前数据状态...")
        before_status = self.check_data_freshness()
        fresh_count = sum(1 for s in before_status.values() if s.get('is_fresh', False))
        self.logger.info(f"   当前数据新鲜度: {fresh_count}/{len(self.INDICES)} 个指数已更新")
        
        for code, info in before_status.items():
            if 'error' in info:
                self.logger.warning(f"   - {info['name']}: {info['error']}")
            else:
                status = "✓" if info['is_fresh'] else "✗"
                self.logger.info(f"   - {info['name']}: {info['latest_date']} {status}")
        
        # 如果所有数据都是最新的，跳过采集但仍需同步MySQL
        if fresh_count == len(self.INDICES):
            self.logger.info("\n✓ 所有指数数据已是最新，无需采集")
            # 直接跳转到MySQL同步步骤
            self.logger.info("\n5. 同步数据到MySQL...")
            try:
                from services.index_sync_service import IndexSyncService
                sync_service = IndexSyncService()
                results = sync_service.sync_incremental(days=5)
                total_synced = sum(inserted + updated for inserted, updated in results.values())
                if total_synced > 0:
                    self.logger.info(f"   ✓ MySQL同步完成，共 {total_synced} 条记录")
                else:
                    self.logger.info("   ✓ MySQL数据已是最新")
            except Exception as e:
                self.logger.warning(f"   ✗ MySQL同步失败: {e}")
            
            self.logger.info("\n" + "=" * 50)
            self.logger.info("✓ 大盘指数任务完成")
            return True
        
        # 2. 尝试自动采集（带重试）
        self.logger.info("\n2. 尝试自动采集...")
        success = False
        
        for attempt in range(1, max_retries + 1):
            self.logger.info(f"   第 {attempt}/{max_retries} 次尝试...")
            if self.fetch_via_akshare():
                success = True
                break
            if attempt < max_retries:
                wait_time = 30 * attempt
                self.logger.info(f"   等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
        
        # 3. 如果自动采集失败，尝试手动更新
        if not success and use_manual_fallback:
            self.logger.info("\n3. 自动采集失败，尝试手动更新...")
            success = self.fetch_via_manual()
        
        # 4. 验证更新结果
        self.logger.info("\n4. 验证更新结果...")
        after_status = self.check_data_freshness()
        fresh_count_after = sum(1 for s in after_status.values() if s.get('is_fresh', False))
        
        for code, info in after_status.items():
            if 'error' in info:
                self.logger.warning(f"   - {info['name']}: {info['error']}")
            else:
                status = "✓" if info['is_fresh'] else "✗"
                self.logger.info(f"   - {info['name']}: {info['latest_date']} {status}")
        
        # 5. 同步到MySQL（无论是否有新数据都执行同步，确保数据一致性）
        self.logger.info("\n5. 同步数据到MySQL...")
        try:
            from services.index_sync_service import IndexSyncService
            sync_service = IndexSyncService()
            results = sync_service.sync_incremental(days=5)  # 只同步最近5天
            total_synced = sum(inserted + updated for inserted, updated in results.values())
            if total_synced > 0:
                self.logger.info(f"   ✓ MySQL同步完成，共 {total_synced} 条记录")
            else:
                self.logger.info("   ✓ MySQL数据已是最新")
        except Exception as e:
            self.logger.warning(f"   ✗ MySQL同步失败: {e}")
        
        # 6. 输出总结
        self.logger.info("\n" + "=" * 50)
        if fresh_count_after == len(self.INDICES):
            self.logger.info("✓ 大盘指数采集任务完成 - 所有数据已更新")
            return True
        elif fresh_count_after > fresh_count:
            self.logger.info(f"△ 大盘指数采集任务部分完成 - {fresh_count_after}/{len(self.INDICES)} 已更新")
            return True
        else:
            self.logger.error(f"✗ 大盘指数采集任务失败 - 仅 {fresh_count_after}/{len(self.INDICES)} 已更新")
            return False


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='大盘指数数据采集')
    parser.add_argument('--retry', action='store_true', help='重试模式')
    parser.add_argument('--max-retries', type=int, default=3, help='最大重试次数')
    parser.add_argument('--no-manual', action='store_true', help='禁用自动回退到手动更新')
    
    args = parser.parse_args()
    
    fetcher = IndexFetcher()
    success = fetcher.run(
        max_retries=args.max_retries,
        use_manual_fallback=not args.no_manual
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
