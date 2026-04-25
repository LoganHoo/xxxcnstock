#!/usr/bin/env python3
"""
增强版数据采集工作流

基于WorkflowExecutor框架，提供：
- 依赖检查
- 自动重试
- 断点续传
- 自动修复
- 发送报告
- Rich多线程/异步采集监控
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
import shutil
import asyncio
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import polars as pl

from core.workflow_framework import (
    WorkflowExecutor, WorkflowStatus, DependencyCheck, DependencyStatus,
    RetryConfig, Checkpoint, workflow_step
)
from core.logger import setup_logger
from core.paths import get_data_path
from core.market_guardian import enforce_market_closed
from services.data_service.unified_data_service import UnifiedDataService
from services.data_service.quality.ge_checkpoint_validators import GECheckpointValidators, CheckStatus, GERetryConfig

# Rich 进度条组件
from rich.console import Console
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn,
    TimeRemainingColumn, TimeElapsedColumn, TaskProgressColumn,
    MofNCompleteColumn
)
from rich.panel import Panel
from rich.live import Live
from rich.table import Table


class EnhancedDataCollectionWorkflow(WorkflowExecutor):
    """增强版数据采集工作流"""

    def __init__(self):
        super().__init__(
            workflow_name="data_collection",
            retry_config=RetryConfig(max_retries=3, retry_delay=1.0, backoff_factor=2.0),
            enable_checkpoint=True,
            enable_auto_fix=True
        )

        self.data_service = UnifiedDataService()
        retry_config = GERetryConfig(max_retries=3, retry_delay=1.0)
        self.checkpoint_validator = GECheckpointValidators(retry_config)

        self.data_dir = get_data_path()
        self.kline_dir = self.data_dir / "kline"

        # 采集状态
        self.collected_codes = []
        self.failed_codes = []
        self.total_codes = 0

        # Rich 控制台
        self.console = Console()

    async def _collect_single_stock_async(
        self,
        code: str,
        date: str,
        progress: Progress,
        task_id: int,
        max_retries: int = 2
    ) -> tuple[str, bool, str]:
        """
        异步采集单只股票数据（带重试）

        Args:
            code: 股票代码
            date: 日期
            progress: Rich进度条
            task_id: 任务ID
            max_retries: 最大重试次数

        Returns:
            (code, success, message)
        """
        from services.data_service.datasource.manager import DataSourceManager

        for attempt in range(max_retries + 1):
            try:
                progress.update(task_id, description=f"[cyan]采集 {code}...")

                ds_manager = DataSourceManager()
                df = await ds_manager.fetch_kline(code, start_date=None, end_date=date)

                if df is None or len(df) == 0:
                    if attempt < max_retries:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return code, False, "无数据返回"

                # 转换为polars DataFrame
                if isinstance(df, pd.DataFrame):
                    df = pl.from_pandas(df)

                # 保存到parquet
                output_path = self.kline_dir / f"{code}.parquet"
                df.write_parquet(output_path)

                progress.update(task_id, advance=1)
                return code, True, "成功"

            except Exception as e:
                error_msg = str(e)
                is_network_error = any(
                    keyword in error_msg.lower()
                    for keyword in ['connection', 'timeout', 'remote', 'reset', 'aborted', 'refused']
                )

                if is_network_error and attempt < max_retries:
                    # 网络错误，等待后重试
                    wait_time = 0.5 * (2 ** attempt)  # 指数退避
                    progress.update(task_id, description=f"[yellow]{code} 重试({attempt+1})...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # 非网络错误或重试次数用尽
                    progress.update(task_id, advance=1)
                    return code, False, error_msg

        return code, False, "重试次数用尽"

    async def _collect_batch_async(
        self,
        batch_codes: List[str],
        date: str,
        progress: Progress,
        main_task_id: int,
        semaphore: asyncio.Semaphore
    ) -> tuple[List[str], List[tuple[str, str]]]:
        """
        异步采集一批股票

        Returns:
            (成功列表, 失败列表[(code, error)])
        """
        collected = []
        failed = []

        async def collect_with_limit(code: str) -> None:
            async with semaphore:
                code, success, msg = await self._collect_single_stock_async(
                    code, date, progress, main_task_id
                )
                if success:
                    collected.append(code)
                else:
                    failed.append((code, msg))

        # 并发执行
        await asyncio.gather(*[collect_with_limit(code) for code in batch_codes])

        return collected, failed

    def execute_with_rich_progress(
        self,
        date: Optional[str] = None,
        codes: Optional[List[str]] = None,
        resume: bool = False,
        checkpoint: Optional[Checkpoint] = None,
        max_workers: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """
        使用Rich进度条的异步采集执行

        Args:
            date: 指定日期
            codes: 指定股票代码
            resume: 是否从断点恢复
            checkpoint: 断点信息
            max_workers: 并发数
        """
        start_time = time.time()

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        # 升级检测
        self.console.print(Panel.fit(
            f"[bold blue]数据采集工作流[/bold blue]\n"
            f"日期: {date}\n"
            f"并发数: {max_workers}",
            title="启动",
            border_style="blue"
        ))

        upgrade_result = self._upgrade_check_and_fix()
        if upgrade_result['status'] == 'error':
            self.console.print("[bold red]升级检测失败，请修复后再采集[/bold red]")
            return {'status': 'failed', 'errors': ['升级检测失败']}

        # 获取股票列表
        self.console.print("\n[bold yellow]获取股票列表...[/bold yellow]")
        try:
            stock_list_df = self.data_service.get_stock_list_sync()
            self.total_codes = len(stock_list_df)
            self.console.print(f"[green]股票总数: {self.total_codes}[/green]")
        except Exception as e:
            self.console.print(f"[bold red]获取股票列表失败: {e}[/bold red]")
            return {'status': 'failed', 'errors': [str(e)]}

        # 确定要采集的股票
        if codes:
            target_codes = codes
        else:
            target_codes = stock_list_df['code'].tolist()

        # 断点恢复
        if resume and checkpoint:
            completed = set(checkpoint.completed_items)
            target_codes = [c for c in target_codes if c not in completed]
            self.collected_codes = checkpoint.completed_items.copy()
            self.failed_codes = checkpoint.failed_items.copy()
            self.console.print(f"[yellow]从断点恢复，跳过{len(completed)}只已采集股票[/yellow]")

        self.console.print(f"[cyan]本次采集: {len(target_codes)}只股票[/cyan]\n")

        # 创建 Rich Progress
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console,
            transient=False
        )

        # 信号量控制并发
        semaphore = asyncio.Semaphore(max_workers)

        # 执行采集
        with progress:
            main_task = progress.add_task(
                f"[bold green]采集K线数据 ({date})",
                total=len(target_codes)
            )

            # 分批处理
            batch_size = 100
            total_batches = (len(target_codes) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                batch_start = batch_idx * batch_size
                batch_end = min(batch_start + batch_size, len(target_codes))
                batch_codes = target_codes[batch_start:batch_end]

                # 异步采集批次
                collected, failed = asyncio.run(
                    self._collect_batch_async(
                        batch_codes, date, progress, main_task, semaphore
                    )
                )

                self.collected_codes.extend(collected)
                self.failed_codes.extend([f[0] for f in failed])

                # 保存断点
                checkpoint = Checkpoint(
                    workflow_id=self.workflow_id,
                    step_name="collect_kline",
                    step_index=batch_idx,
                    completed_items=self.collected_codes.copy(),
                    failed_items=self.failed_codes.copy(),
                    metadata={'date': date, 'total': self.total_codes}
                )
                self.save_checkpoint(checkpoint)

        # 显示结果
        duration = time.time() - start_time
        success_rate = len(self.collected_codes) / self.total_codes * 100 if self.total_codes > 0 else 0

        # 创建结果表格
        table = Table(title="采集结果统计")
        table.add_column("指标", style="cyan")
        table.add_column("数值", style="magenta")

        table.add_row("总股票数", str(self.total_codes))
        table.add_row("成功采集", f"[green]{len(self.collected_codes)}[/green]")
        table.add_row("失败数量", f"[red]{len(self.failed_codes)}[/red]")
        table.add_row("成功率", f"[bold]{success_rate:.1f}%[/bold]")
        table.add_row("耗时", f"{duration:.1f}秒")
        table.add_row("平均速度", f"{len(self.collected_codes)/duration:.1f}只/秒" if duration > 0 else "N/A")

        self.console.print("\n")
        self.console.print(table)

        # 确定状态
        if len(self.failed_codes) == 0:
            status = 'completed'
        elif len(self.collected_codes) > 0:
            status = 'partial'
        else:
            status = 'failed'

        return {
            'status': status,
            'errors': [f"失败: {self.failed_codes[:10]}..."] if self.failed_codes else [],
            'summary': {
                'date': date,
                'total': self.total_codes,
                'collected': len(self.collected_codes),
                'failed': len(self.failed_codes),
                'duration_seconds': f"{duration:.2f}",
                'success_rate': f"{success_rate:.1f}%"
            }
        }

    def _upgrade_check_and_fix(self) -> Dict[str, Any]:
        """
        升级检测和修复
        在采集前检查数据源配置、依赖库版本、网络连接等
        """
        self.logger.info("\n🔧 升级检测和修复")
        results = {
            'checks': [],
            'fixes': [],
            'status': 'ok'
        }

        # 1. 检查数据源配置
        try:
            config_path = Path('/Volumes/Xdata/workstation/xxxcnstock/config/datasource.yaml')
            if config_path.exists():
                import yaml
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                primary = config.get('datasource', {}).get('primary', {}).get('name', '')
                # 只显示启用的备用数据源
                backups = [b.get('name') for b in config.get('datasource', {}).get('backups', []) if b.get('enabled', True)]

                self.logger.info(f"   主数据源: {primary}")
                self.logger.info(f"   备用数据源: {backups}")

                # 验证优先级：baostock优先，tencent其次
                if primary != 'baostock':
                    self.logger.warning(f"   主数据源配置为 {primary}，建议改为 baostock")
                    results['checks'].append({'name': '数据源优先级', 'status': 'warning', 'message': f'主源为{primary}，建议baostock优先'})
                else:
                    results['checks'].append({'name': '数据源优先级', 'status': 'ok', 'message': 'baostock优先配置正确'})

                if 'tencent' not in backups:
                    results['checks'].append({'name': '备用数据源', 'status': 'warning', 'message': 'tencent未配置为备用源'})
                else:
                    results['checks'].append({'name': '备用数据源', 'status': 'ok', 'message': 'tencent备用配置正确'})

                # 检查AKShare是否被禁用（网络不稳定）
                akshare_enabled = any(
                    b.get('name') == 'akshare' and b.get('enabled', True)
                    for b in config.get('datasource', {}).get('backups', [])
                )
                if not akshare_enabled:
                    self.logger.info("   AKShare已禁用（网络不稳定），仅使用baostock+tencent")
            else:
                results['checks'].append({'name': '配置文件', 'status': 'error', 'message': 'datasource.yaml不存在'})
                results['status'] = 'error'
        except Exception as e:
            results['checks'].append({'name': '配置检查', 'status': 'error', 'message': str(e)})
            results['status'] = 'error'

        # 2. 检查依赖库版本
        try:
            import baostock
            import akshare
            import requests

            self.logger.info(f"   Baostock: 已安装")
            self.logger.info(f"   AKShare: {akshare.__version__}")
            self.logger.info(f"   Requests: {requests.__version__}")
            results['checks'].append({'name': '依赖库', 'status': 'ok', 'message': '核心依赖库已安装'})
        except Exception as e:
            results['checks'].append({'name': '依赖库', 'status': 'error', 'message': str(e)})
            results['status'] = 'error'

        # 3. 检查网络连接
        try:
            import urllib.request
            # 测试Baostock
            try:
                urllib.request.urlopen('http://baostock.com', timeout=5)
                results['checks'].append({'name': 'Baostock网络', 'status': 'ok', 'message': '连接正常'})
            except:
                results['checks'].append({'name': 'Baostock网络', 'status': 'warning', 'message': '连接测试失败'})

            # 测试腾讯
            try:
                urllib.request.urlopen('https://qt.gtimg.cn', timeout=5)
                results['checks'].append({'name': '腾讯网络', 'status': 'ok', 'message': '连接正常'})
            except:
                results['checks'].append({'name': '腾讯网络', 'status': 'warning', 'message': '连接测试失败'})
        except Exception as e:
            results['checks'].append({'name': '网络检查', 'status': 'error', 'message': str(e)})

        # 4. 检查数据目录
        try:
            if not self.kline_dir.exists():
                self.kline_dir.mkdir(parents=True, exist_ok=True)
                results['fixes'].append({'name': '数据目录', 'action': '创建目录', 'path': str(self.kline_dir)})
            results['checks'].append({'name': '数据目录', 'status': 'ok', 'message': str(self.kline_dir)})
        except Exception as e:
            results['checks'].append({'name': '数据目录', 'status': 'error', 'message': str(e)})
            results['status'] = 'error'

        # 5. 测试数据源实际可用性
        try:
            self.logger.info("   测试数据源可用性...")
            import asyncio

            # 测试Baostock - 使用异步方法
            from services.data_service.datasource.providers import BaostockProvider
            bs = BaostockProvider()
            test_df = asyncio.get_event_loop().run_until_complete(
                bs.fetch_kline('600000', '2026-04-22', '2026-04-22')
            )
            if test_df is not None and not test_df.empty:
                results['checks'].append({'name': 'Baostock可用性', 'status': 'ok', 'message': '数据获取正常'})
            else:
                results['checks'].append({'name': 'Baostock可用性', 'status': 'warning', 'message': '数据获取为空'})
        except Exception as e:
            results['checks'].append({'name': 'Baostock可用性', 'status': 'warning', 'message': str(e)})

        # 汇总结果
        errors = [c for c in results['checks'] if c['status'] == 'error']
        warnings = [c for c in results['checks'] if c['status'] == 'warning']

        if errors:
            self.logger.error(f"   检测到 {len(errors)} 个错误，请修复后再采集")
            results['status'] = 'error'
        elif warnings:
            self.logger.warning(f"   检测到 {len(warnings)} 个警告，但可以继续采集")
            results['status'] = 'warning'
        else:
            self.logger.info("   所有检查通过，可以开始采集")
            results['status'] = 'ok'

        return results
    
    def check_dependencies(self) -> List[DependencyCheck]:
        """检查依赖"""
        checks = []
        
        # 1. 检查数据源可用性
        try:
            # 尝试获取股票列表验证数据源
            stock_list = self.data_service.get_stock_list_sync()
            if len(stock_list) > 0:
                checks.append(DependencyCheck(
                    name="数据源连接",
                    status=DependencyStatus.HEALTHY,
                    message=f"数据源正常，股票列表: {len(stock_list)}只"
                ))
            else:
                checks.append(DependencyCheck(
                    name="数据源连接",
                    status=DependencyStatus.UNHEALTHY,
                    message="数据源返回空股票列表"
                ))
        except Exception as e:
            checks.append(DependencyCheck(
                name="数据源连接",
                status=DependencyStatus.UNHEALTHY,
                message=f"数据源连接失败: {e}"
            ))
        
        # 2. 检查存储空间
        try:
            data_path = self.data_dir
            stat = shutil.disk_usage(data_path)
            free_gb = stat.free / (1024**3)
            total_gb = stat.total / (1024**3)
            
            if free_gb < 1:  # 小于1GB
                checks.append(DependencyCheck(
                    name="存储空间",
                    status=DependencyStatus.UNHEALTHY,
                    message=f"存储空间不足: 剩余{free_gb:.2f}GB"
                ))
            elif free_gb < 5:  # 小于5GB
                checks.append(DependencyCheck(
                    name="存储空间",
                    status=DependencyStatus.DEGRADED,
                    message=f"存储空间紧张: 剩余{free_gb:.2f}GB / 总共{total_gb:.2f}GB"
                ))
            else:
                checks.append(DependencyCheck(
                    name="存储空间",
                    status=DependencyStatus.HEALTHY,
                    message=f"存储空间充足: 剩余{free_gb:.2f}GB / 总共{total_gb:.2f}GB"
                ))
        except Exception as e:
            checks.append(DependencyCheck(
                name="存储空间",
                status=DependencyStatus.UNKNOWN,
                message=f"无法检查存储空间: {e}"
            ))
        
        # 3. 检查K线数据目录
        if self.kline_dir.exists():
            checks.append(DependencyCheck(
                name="K线数据目录",
                status=DependencyStatus.HEALTHY,
                message=f"目录存在: {self.kline_dir}"
            ))
        else:
            checks.append(DependencyCheck(
                name="K线数据目录",
                status=DependencyStatus.UNHEALTHY,
                message=f"目录不存在: {self.kline_dir}"
            ))
        
        # 4. 检查网络连接
        try:
            import urllib.request
            urllib.request.urlopen('https://www.baidu.com', timeout=5)
            checks.append(DependencyCheck(
                name="网络连接",
                status=DependencyStatus.HEALTHY,
                message="网络连接正常"
            ))
        except Exception as e:
            checks.append(DependencyCheck(
                name="网络连接",
                status=DependencyStatus.UNHEALTHY,
                message=f"网络连接异常: {e}"
            ))
        
        return checks
    
    def auto_fix_dependency(self, dependency: DependencyCheck) -> bool:
        """自动修复依赖问题"""
        self.logger.info(f"尝试自动修复: {dependency.name}")
        
        if dependency.name == "K线数据目录":
            try:
                self.kline_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"✅ 已创建目录: {self.kline_dir}")
                return True
            except Exception as e:
                self.logger.error(f"❌ 创建目录失败: {e}")
                return False
        
        if dependency.name == "数据源连接":
            try:
                # 尝试重新初始化数据服务
                self.data_service = UnifiedDataService()
                stock_list = self.data_service.get_stock_list_sync()
                if len(stock_list) > 0:
                    self.logger.info(f"✅ 数据源连接已恢复")
                    return True
            except Exception as e:
                self.logger.error(f"❌ 数据源连接修复失败: {e}")
                return False
        
        # 其他问题无法自动修复
        return False
    
    def execute(
        self,
        date: Optional[str] = None,
        codes: Optional[List[str]] = None,
        resume: bool = False,
        checkpoint: Optional[Checkpoint] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行数据采集
        
        Args:
            date: 指定日期
            codes: 指定股票代码
            resume: 是否从断点恢复
            checkpoint: 断点信息
        """
        start_time = time.time()
        
        # 确定采集日期
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        self.logger.info(f"\n📅 采集日期: {date}")

        # 升级检测和修复
        upgrade_result = self._upgrade_check_and_fix()
        if upgrade_result['status'] == 'error':
            return {
                'status': 'failed',
                'errors': ['升级检测失败，请修复后再采集'],
                'summary': {'date': date, 'upgrade_check': upgrade_result}
            }

        # 检查点1: 采集前检查
        self.logger.info("\n🔍 检查点1: 采集前检查")
        pre_check = self.checkpoint_validator.pre_collection_check(date=date)
        self.logger.info(f"   结果: {pre_check.status.value} - {pre_check.message}")

        if pre_check.status == CheckStatus.FAILED:
            return {
                'status': 'failed',
                'errors': [f"采集前检查失败: {pre_check.message}"],
                'summary': {'date': date}
            }
        
        # 获取股票列表
        self.logger.info("\n📋 获取股票列表")
        try:
            stock_list_df = self.data_service.get_stock_list_sync()
            self.total_codes = len(stock_list_df)
            self.logger.info(f"   股票总数: {self.total_codes}")
        except Exception as e:
            return {
                'status': 'failed',
                'errors': [f"获取股票列表失败: {e}"],
                'summary': {'date': date}
            }
        
        # 确定要采集的股票
        if codes:
            target_codes = codes
        else:
            target_codes = stock_list_df['code'].tolist()
        
        # 如果从断点恢复，过滤已完成的
        if resume and checkpoint:
            completed = set(checkpoint.completed_items)
            target_codes = [c for c in target_codes if c not in completed]
            self.collected_codes = checkpoint.completed_items.copy()
            self.failed_codes = checkpoint.failed_items.copy()
            self.logger.info(f"   从断点恢复，跳过{len(completed)}只已采集股票")
        
        self.logger.info(f"   本次采集: {len(target_codes)}只股票")
        
        # 批量采集
        self.logger.info("\n📥 开始采集K线数据")
        batch_size = 100
        total_batches = (len(target_codes) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, len(target_codes))
            batch_codes = target_codes[batch_start:batch_end]
            
            self.logger.info(f"\n   批次 {batch_idx + 1}/{total_batches}: {len(batch_codes)}只股票")
            
            for code in batch_codes:
                try:
                    # 采集单只股票数据
                    self._collect_single_stock(code, date)
                    self.collected_codes.append(code)
                except Exception as e:
                    self.logger.error(f"   ❌ {code}: 采集失败 - {e}")
                    self.failed_codes.append(code)
            
            # 保存断点
            checkpoint = Checkpoint(
                workflow_id=self.workflow_id,
                step_name="collect_kline",
                step_index=batch_idx,
                completed_items=self.collected_codes.copy(),
                failed_items=self.failed_codes.copy(),
                metadata={'date': date, 'total': self.total_codes}
            )
            self.save_checkpoint(checkpoint)
            
            self.logger.info(f"   进度: {len(self.collected_codes)}/{self.total_codes} "
                           f"({len(self.collected_codes)/self.total_codes*100:.1f}%)")
        
        # 检查点2: 采集后验证
        self.logger.info("\n🔍 检查点2: 采集后验证")
        
        # 统计采集结果
        collected_count = len(self.collected_codes)
        failed_count = len(self.failed_codes)
        
        # 读取部分数据验证
        sample_data = []
        for code in self.collected_codes[:10]:
            try:
                df = pl.read_parquet(self.kline_dir / f"{code}.parquet")
                sample_data.append(df)
            except:
                pass
        
        if sample_data:
            combined_df = pl.concat(sample_data)
            validation = self.checkpoint_validator.post_collection_validation(
                combined_df, data_type="kline"
            )
            self.logger.info(f"   结果: {validation.status.value} - {validation.message}")
            
            if validation.status == CheckStatus.FAILED:
                return {
                    'status': 'partial',
                    'errors': [f"采集后验证失败: {validation.message}"],
                    'warnings': [f"失败股票: {len(self.failed_codes)}只"],
                    'summary': {
                        'date': date,
                        'total': self.total_codes,
                        'collected': collected_count,
                        'failed': failed_count,
                        'success_rate': f"{collected_count/self.total_codes*100:.1f}%"
                    }
                }
        
        duration = time.time() - start_time
        
        # 确定最终状态
        if failed_count == 0:
            status = 'completed'
        elif collected_count > 0:
            status = 'partial'
        else:
            status = 'failed'
        
        return {
            'status': status,
            'errors': [f"失败股票: {self.failed_codes}"] if self.failed_codes else [],
            'warnings': [f"部分股票采集失败: {failed_count}只"] if failed_count > 0 else [],
            'summary': {
                'date': date,
                'total': self.total_codes,
                'collected': collected_count,
                'failed': failed_count,
                'duration_seconds': f"{duration:.2f}",
                'success_rate': f"{collected_count/self.total_codes*100:.1f}%"
            }
        }
    
    def _collect_single_stock(self, code: str, date: str):
        """采集单只股票数据"""
        try:
            # 使用数据服务获取K线数据
            df = self.data_service.get_kline(code, start_date=None, end_date=date)

            if df is None or len(df) == 0:
                raise ValueError(f"无数据返回")

            # 转换为polars DataFrame
            import polars as pl
            if isinstance(df, pd.DataFrame):
                df = pl.from_pandas(df)

            # 保存到parquet
            output_path = self.kline_dir / f"{code}.parquet"
            df.write_parquet(output_path)

        except Exception as e:
            raise Exception(f"采集失败: {e}")


def run_data_collection(
    date: Optional[str] = None,
    codes: Optional[List[str]] = None,
    resume: bool = False,
    use_rich: bool = False,
    max_workers: int = 10
) -> Dict[str, Any]:
    """
    运行数据采集工作流

    Args:
        date: 指定日期
        codes: 指定股票代码
        resume: 是否从断点恢复
        use_rich: 是否使用Rich进度条
        max_workers: 并发数(Rich模式)

    Returns:
        执行结果
    """
    workflow = EnhancedDataCollectionWorkflow()

    if use_rich:
        # 使用Rich进度条模式
        return workflow.execute_with_rich_progress(
            date=date, codes=codes, resume=resume, max_workers=max_workers
        )
    else:
        # 使用标准模式
        report = workflow.run(date=date, codes=codes, resume=resume)
        return report.to_dict()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="数据采集工作流")
    parser.add_argument("--date", help="指定日期 (YYYY-MM-DD)")
    parser.add_argument("--codes", nargs="+", help="指定股票代码")
    parser.add_argument("--resume", action="store_true", help="从断点恢复")
    parser.add_argument("--rich", action="store_true", help="使用Rich进度条(异步并发)")
    parser.add_argument("--workers", type=int, default=10, help="并发数(Rich模式)")

    args = parser.parse_args()

    result = run_data_collection(
        date=args.date,
        codes=args.codes,
        resume=args.resume,
        use_rich=args.rich,
        max_workers=args.workers
    )

    # 非Rich模式才打印结果(Rich模式已经打印过)
    if not args.rich:
        print("\n" + "=" * 60)
        print("执行结果:")
        print("=" * 60)
        print(f"状态: {result['status']}")
        if 'duration_seconds' in result:
            print(f"时长: {result['duration_seconds']:.2f}秒")

        if result.get('errors'):
            print(f"\n错误:")
            for error in result['errors']:
                print(f"  - {error}")

        if result.get('summary'):
            print(f"\n汇总:")
            for key, value in result['summary'].items():
                print(f"  {key}: {value}")
