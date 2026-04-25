#!/usr/bin/env python3
"""
100% 数据采集完成工具
- 使用多数据源备份策略
- 智能重试机制
- 完整性验证
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Set, List, Dict, Tuple, Optional
from dataclasses import dataclass

import pandas as pd
import polars as pl
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.panel import Panel

from core.logger import setup_logger
from services.data_service.datasource.manager import DataSourceManager

logger = setup_logger("complete_collection")
console = Console()


@dataclass
class CollectionResult:
    """采集结果"""
    code: str
    success: bool
    source: str  # 成功使用的数据源
    records: int
    error: str = ""


class CompleteCollectionManager:
    """100%采集管理器"""

    def __init__(self):
        self.project_root = Path('/Volumes/Xdata/workstation/xxxcnstock')
        self.kline_dir = self.project_root / 'data' / 'kline'
        self.checkpoint_dir = self.project_root / 'data' / 'checkpoints' / 'complete_collection'
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # 数据源优先级：baostock > tencent > akshare
        self.data_sources = ['baostock', 'tencent', 'akshare']

        # 统计
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'by_source': {source: 0 for source in self.data_sources}
        }

    def get_target_codes(self) -> Set[str]:
        """获取需要采集的目标股票代码"""
        # 从有效股票列表加载
        valid_list_file = self.project_root / 'data' / 'valid_stock_list.json'

        if not valid_list_file.exists():
            console.print("[red]错误: 未找到有效股票列表，请先运行 filter_valid_stocks.py[/red]")
            return set()

        with open(valid_list_file, 'r') as f:
            data = json.load(f)
            all_codes = set(data.get('codes', []))

        # 获取已存在的代码
        existing_codes = self.get_existing_codes()

        # 需要采集的代码
        need_codes = all_codes - existing_codes

        console.print(Panel.fit(
            f"[bold blue]数据采集目标[/bold blue]\n"
            f"总目标: {len(all_codes)}\n"
            f"已存在: {len(existing_codes)}\n"
            f"需采集: [bold yellow]{len(need_codes)}[/bold yellow]",
            title="目标统计",
            border_style="blue"
        ))

        self.stats['total'] = len(need_codes)
        return need_codes

    def get_existing_codes(self) -> Set[str]:
        """获取已存在的股票代码"""
        if not self.kline_dir.exists():
            return set()

        existing_files = list(self.kline_dir.glob('*.parquet'))
        return {f.stem for f in existing_files}

    async def collect_single_stock(
        self,
        code: str,
        date: str,
        progress: Progress = None,
        task_id: int = None
    ) -> CollectionResult:
        """
        采集单只股票（多数据源备份）

        策略：
        1. 优先使用 baostock
        2. baostock 失败则尝试 tencent
        3. tencent 失败则尝试 akshare
        4. 全部失败则记录为失败
        """
        from services.data_service.datasource.providers import (
            BaostockProvider, TencentProvider, AKShareProvider
        )

        last_error = ""

        # 设置默认起始日期为3年前
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')

        for source_name in self.data_sources:
            try:
                if progress and task_id:
                    progress.update(task_id, description=f"[cyan]{code} [{source_name}]...")

                # 根据数据源创建provider
                if source_name == 'baostock':
                    provider = BaostockProvider()
                elif source_name == 'tencent':
                    provider = TencentProvider()
                elif source_name == 'akshare':
                    provider = AKShareProvider()
                else:
                    continue

                # 尝试获取数据
                df = await provider.fetch_kline(
                    code=code,
                    start_date=start_date,
                    end_date=date
                )

                if df is not None and len(df) > 0:
                    # 转换为polars并保存
                    if isinstance(df, pd.DataFrame):
                        df = pl.from_pandas(df)

                    output_path = self.kline_dir / f"{code}.parquet"
                    df.write_parquet(output_path)

                    if progress and task_id:
                        progress.update(task_id, advance=1)

                    self.stats['by_source'][source_name] += 1
                    return CollectionResult(
                        code=code,
                        success=True,
                        source=source_name,
                        records=len(df)
                    )

            except Exception as e:
                last_error = str(e)
                # 继续尝试下一个数据源
                continue

        # 所有数据源都失败
        if progress and task_id:
            progress.update(task_id, advance=1)

        return CollectionResult(
            code=code,
            success=False,
            source="",
            records=0,
            error=last_error
        )

    async def collect_batch(
        self,
        codes: List[str],
        date: str,
        progress: Progress,
        main_task_id: int,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[CollectionResult], List[CollectionResult]]:
        """批量采集"""
        success_results = []
        failed_results = []

        async def collect_with_limit(code: str) -> None:
            async with semaphore:
                result = await self.collect_single_stock(
                    code, date, progress, main_task_id
                )
                if result.success:
                    success_results.append(result)
                else:
                    failed_results.append(result)

        # 并发执行
        await asyncio.gather(*[collect_with_limit(code) for code in codes])

        return success_results, failed_results

    async def run_collection(
        self,
        date: Optional[str] = None,
        max_workers: int = 10,
        batch_size: int = 100
    ):
        """运行完整采集"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        # 获取目标代码
        target_codes = self.get_target_codes()
        if not target_codes:
            console.print("[green]所有股票数据已完整，无需采集[/green]")
            return

        target_codes = sorted(list(target_codes))
        total_batches = (len(target_codes) + batch_size - 1) // batch_size

        console.print(f"\n[bold]开始采集，共 {len(target_codes)} 只股票，{total_batches} 批次[/bold]\n")

        # 创建进度条
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            main_task = progress.add_task(
                f"[bold green]采集进度",
                total=len(target_codes)
            )

            # 信号量控制并发
            semaphore = asyncio.Semaphore(max_workers)

            # 分批处理
            all_failed = []

            for batch_idx in range(total_batches):
                batch_start = batch_idx * batch_size
                batch_end = min(batch_start + batch_size, len(target_codes))
                batch_codes = target_codes[batch_start:batch_end]

                console.print(f"\n[cyan]批次 {batch_idx + 1}/{total_batches}: {len(batch_codes)} 只股票[/cyan]")

                # 采集批次
                success_results, failed_results = await self.collect_batch(
                    batch_codes, date, progress, main_task, semaphore
                )

                self.stats['success'] += len(success_results)
                self.stats['failed'] += len(failed_results)
                all_failed.extend(failed_results)

                # 保存断点
                self._save_checkpoint(batch_idx, success_results, failed_results, date)

                # 显示批次结果
                console.print(f"  [green]成功: {len(success_results)}[/green] "
                             f"[red]失败: {len(failed_results)}[/red]")

                # 显示数据源分布
                source_dist = {}
                for r in success_results:
                    source_dist[r.source] = source_dist.get(r.source, 0) + 1

                if source_dist:
                    source_str = ", ".join([f"{s}:{c}" for s, c in source_dist.items()])
                    console.print(f"  [dim]数据源分布: {source_str}[/dim]")

        # 显示最终结果
        self._show_final_result(all_failed)

        # 如果有失败，进行第二轮重试
        if all_failed:
            await self._retry_failed(all_failed, date, max_workers)

    def _save_checkpoint(
        self,
        batch_idx: int,
        success_results: List[CollectionResult],
        failed_results: List[CollectionResult],
        date: str
    ):
        """保存断点"""
        checkpoint_file = self.checkpoint_dir / f"checkpoint_{date}_{batch_idx:04d}.json"

        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'date': date,
            'batch_idx': batch_idx,
            'success': [r.code for r in success_results],
            'failed': [
                {'code': r.code, 'error': r.error}
                for r in failed_results
            ]
        }

        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

    def _show_final_result(self, failed_results: List[CollectionResult]):
        """显示最终结果"""
        success_rate = (self.stats['success'] / self.stats['total'] * 100) if self.stats['total'] > 0 else 0

        table = Table(title="采集结果统计")
        table.add_column("指标", style="cyan")
        table.add_column("数值", style="magenta")

        table.add_row("总目标", str(self.stats['total']))
        table.add_row("成功采集", f"[green]{self.stats['success']}[/green]")
        table.add_row("失败数量", f"[red]{self.stats['failed']}[/red]")
        table.add_row("成功率", f"[bold]{success_rate:.2f}%[/bold]")

        # 数据源分布
        for source, count in self.stats['by_source'].items():
            if count > 0:
                table.add_row(f"  - {source}", str(count))

        console.print("\n")
        console.print(table)

        # 显示失败股票
        if failed_results:
            console.print(f"\n[red]失败股票 ({len(failed_results)} 只):[/red]")
            failed_codes = [r.code for r in failed_results[:20]]
            console.print(f"  {', '.join(failed_codes)}")
            if len(failed_results) > 20:
                console.print(f"  ... 还有 {len(failed_results) - 20} 只")

    async def _retry_failed(
        self,
        failed_results: List[CollectionResult],
        date: str,
        max_workers: int = 5
    ):
        """重试失败的股票"""
        console.print("\n" + "=" * 70)
        console.print("[bold yellow]第二轮重试: 降低并发，增加重试次数[/bold yellow]")
        console.print("=" * 70)

        failed_codes = [r.code for r in failed_results]

        # 降低并发，增加稳定性
        semaphore = asyncio.Semaphore(max_workers)

        retry_success = []
        retry_failed = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold yellow]重试 {task.fields[code]}..."),
            console=console
        ) as progress:
            task_id = progress.add_task("retry", total=len(failed_codes), code="")

            async def retry_single(code: str):
                async with semaphore:
                    progress.update(task_id, code=code)

                    # 重试3次
                    for attempt in range(3):
                        result = await self.collect_single_stock(code, date)
                        if result.success:
                            retry_success.append(result)
                            return
                        await asyncio.sleep(1 * (attempt + 1))

                    retry_failed.append(CollectionResult(code=code, success=False, source="", records=0))
                    progress.update(task_id, advance=1)

            await asyncio.gather(*[retry_single(code) for code in failed_codes])

        # 更新统计
        self.stats['success'] += len(retry_success)
        self.stats['failed'] = len(retry_failed)

        console.print(f"\n[green]重试成功: {len(retry_success)}[/green]")
        console.print(f"[red]仍然失败: {len(retry_failed)}[/red]")

        # 保存最终失败列表
        if retry_failed:
            final_failed_file = self.checkpoint_dir / f"final_failed_{date}.json"
            with open(final_failed_file, 'w') as f:
                json.dump({
                    'date': date,
                    'failed_codes': [r.code for r in retry_failed],
                    'count': len(retry_failed)
                }, f, indent=2, ensure_ascii=False)

            console.print(f"\n[yellow]最终失败列表已保存: {final_failed_file}[/yellow]")

    def verify_completeness(self) -> bool:
        """验证采集完整性"""
        console.print("\n" + "=" * 70)
        console.print("[bold blue]数据完整性验证[/bold blue]")
        console.print("=" * 70)

        # 获取目标代码
        valid_list_file = self.project_root / 'data' / 'valid_stock_list.json'
        with open(valid_list_file, 'r') as f:
            all_codes = set(json.load(f).get('codes', []))

        # 获取已存在的代码
        existing_codes = self.get_existing_codes()

        # 检查缺失
        missing = all_codes - existing_codes

        if not missing:
            console.print("[bold green]✅ 数据完整性验证通过: 100% 采集完成[/bold green]")
            return True
        else:
            console.print(f"[bold red]❌ 数据不完整: 缺失 {len(missing)} 只股票[/bold red]")
            console.print(f"[dim]缺失示例: {', '.join(sorted(list(missing))[:10])}[/dim]")
            return False


async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='100% 数据采集完成工具')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--workers', type=int, default=10, help='并发数 (默认: 10)')
    parser.add_argument('--batch-size', type=int, default=100, help='批次大小 (默认: 100)')
    parser.add_argument('--verify', action='store_true', help='仅验证完整性')

    args = parser.parse_args()

    manager = CompleteCollectionManager()

    if args.verify:
        manager.verify_completeness()
    else:
        await manager.run_collection(
            date=args.date,
            max_workers=args.workers,
            batch_size=args.batch_size
        )

        # 最后验证
        console.print("\n")
        manager.verify_completeness()


if __name__ == "__main__":
    asyncio.run(main())
