#!/usr/bin/env python3
"""
过滤有效股票列表
- 从Baostock获取当前有效股票列表
- 过滤掉已退市/不存在的股票
- 生成有效的股票代码列表用于采集
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Set, List, Dict

import pandas as pd

from services.data_service.unified_data_service import UnifiedDataService
from core.logger import setup_logger

logger = setup_logger("filter_valid_stocks")


class ValidStockFilter:
    """有效股票过滤器"""

    def __init__(self):
        self.data_service = UnifiedDataService()
        self.project_root = Path('/Volumes/Xdata/workstation/xxxcnstock')
        self.kline_dir = self.project_root / 'data' / 'kline'
        self.checkpoint_dir = self.project_root / 'data' / 'checkpoints' / 'data_collection'

    def get_baostock_stock_list(self) -> Set[str]:
        """从Baostock获取当前有效股票列表"""
        print("\n" + "=" * 70)
        print("从Baostock获取有效股票列表")
        print("=" * 70)

        try:
            # 使用同步方法获取股票列表
            import importlib
            bs = importlib.import_module("baostock")

            lg = bs.login()
            if lg.error_code != '0':
                print(f"❌ Baostock登录失败: {lg.error_msg}")
                return set()

            stocks = []
            query_date = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            rs = bs.query_all_stock(day=query_date)

            while rs.next():
                row = rs.get_row_data()
                full_code = row[0]  # e.g., 'sh.000001'
                trade_status = row[1] if len(row) > 1 else '0'
                name = row[2] if len(row) > 2 else ''

                code = full_code.split('.')[-1]
                exchange = 'sh' if full_code.startswith('sh') else 'sz'

                # 过滤指数
                if code.startswith(('000', '880', '999')) and exchange == 'sh':
                    continue
                if code.startswith('399') and exchange == 'sz':
                    continue

                # 过滤非交易状态
                if trade_status != '1':
                    continue

                # 过滤退市股票
                if self._is_delisted(name):
                    continue

                stocks.append(code)

            bs.logout()

            print(f"✅ 获取到 {len(stocks)} 只有效股票")
            return set(stocks)

        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return set()

    def _is_delisted(self, name: str) -> bool:
        """检查是否退市"""
        if not name:
            return False

        delisting_keywords = [
            '退市', '退', '*ST', 'ST', 'PT', '终止上市',
            '摘牌', '作废', '注销', '解散', '破产'
        ]

        name_upper = str(name).upper()
        return any(keyword.upper() in name_upper for keyword in delisting_keywords)

    def get_existing_stock_codes(self) -> Set[str]:
        """获取本地已存在的股票代码"""
        if not self.kline_dir.exists():
            return set()

        existing_files = list(self.kline_dir.glob('*.parquet'))
        return {f.stem for f in existing_files}

    def get_failed_stocks_from_checkpoint(self) -> Set[str]:
        """从断点文件获取失败股票列表"""
        failed_codes = set()

        if not self.checkpoint_dir.exists():
            return failed_codes

        checkpoint_files = sorted(
            self.checkpoint_dir.glob('*.json'),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )

        for cp_file in checkpoint_files[:3]:  # 检查最近3个
            try:
                with open(cp_file, 'r') as f:
                    data = json.load(f)
                    if 'failed_items' in data:
                        failed_codes.update(data['failed_items'])
            except:
                pass

        return failed_codes

    def filter_and_report(self):
        """过滤并生成报告"""
        print("\n" + "=" * 70)
        print("有效股票过滤报告")
        print("=" * 70)

        # 1. 获取Baostock有效股票列表
        baostock_codes = self.get_baostock_stock_list()
        if not baostock_codes:
            print("❌ 无法获取Baostock股票列表")
            return

        # 2. 获取本地已有数据
        existing_codes = self.get_existing_stock_codes()
        print(f"📁 本地已有数据: {len(existing_codes)} 只股票")

        # 3. 获取失败股票
        failed_codes = self.get_failed_stocks_from_checkpoint()
        print(f"❌ 上次采集失败: {len(failed_codes)} 只股票")

        # 4. 分析失败原因
        if failed_codes:
            not_in_baostock = failed_codes - baostock_codes
            in_baostock = failed_codes & baostock_codes

            print(f"\n📊 失败股票分析:")
            print(f"   不在Baostock列表中: {len(not_in_baostock)} 只")
            print(f"   在Baostock列表中: {len(in_baostock)} 只")

            if not_in_baostock:
                print(f"\n   不在列表中的股票示例:")
                for code in sorted(not_in_baostock)[:10]:
                    print(f"      {code}")
                if len(not_in_baostock) > 10:
                    print(f"      ... 还有 {len(not_in_baostock) - 10} 只")

            # 5. 保存不在列表中的股票（这些应该被排除）
            excluded_file = self.project_root / 'data' / 'excluded_stocks.json'
            with open(excluded_file, 'w') as f:
                json.dump({
                    'date': datetime.now().isoformat(),
                    'reason': '不在Baostock当前股票列表中',
                    'count': len(not_in_baostock),
                    'codes': sorted(not_in_baostock)
                }, f, indent=2, ensure_ascii=False)
            print(f"\n💾 已保存排除列表到: {excluded_file}")

        # 6. 计算需要采集的股票
        need_collect = baostock_codes - existing_codes
        print(f"\n📥 需要采集: {len(need_collect)} 只股票")

        # 7. 保存有效股票列表
        valid_list_file = self.project_root / 'data' / 'valid_stock_list.json'
        with open(valid_list_file, 'w') as f:
            json.dump({
                'date': datetime.now().isoformat(),
                'source': 'baostock',
                'total': len(baostock_codes),
                'codes': sorted(baostock_codes)
            }, f, indent=2, ensure_ascii=False)
        print(f"💾 已保存有效股票列表到: {valid_list_file}")

        return {
            'baostock_total': len(baostock_codes),
            'existing': len(existing_codes),
            'failed': len(failed_codes),
            'need_collect': len(need_collect)
        }


def main():
    """主函数"""
    filter_tool = ValidStockFilter()
    result = filter_tool.filter_and_report()

    if result:
        print("\n" + "=" * 70)
        print("总结")
        print("=" * 70)
        print(f"Baostock有效股票: {result['baostock_total']}")
        print(f"本地已有数据: {result['existing']}")
        print(f"需要采集: {result['need_collect']}")


if __name__ == "__main__":
    main()
