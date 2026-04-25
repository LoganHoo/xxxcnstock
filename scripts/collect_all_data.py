#!/usr/bin/env python3
"""
完整数据采集脚本
使用 Baostock 直接采集（无需 Token）
"""
import sys
import os
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import baostock as bs
import logging

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


class DataCollector:
    """数据采集器"""
    
    def __init__(self):
        self.data_dir = PROJECT_ROOT / "data"
        self.kline_dir = self.data_dir / "kline"
        self.data_dir.mkdir(exist_ok=True)
        self.kline_dir.mkdir(exist_ok=True)
        self.lg = None
        
    def login(self):
        """登录 Baostock"""
        self.lg = bs.login()
        if self.lg.error_code != '0':
            logger.error(f"Baostock 登录失败: {self.lg.error_msg}")
            return False
        logger.info("✅ Baostock 登录成功")
        return True
    
    def logout(self):
        """登出"""
        if self.lg:
            bs.logout()
            logger.info("Baostock 登出")
    
    def collect_stock_list(self):
        """采集股票列表"""
        logger.info("=" * 70)
        logger.info("📋 步骤1: 采集股票列表")
        logger.info("=" * 70)

        # 使用最近的交易日（避免非交易日返回空）
        # 2026-04-17 是周五
        trading_day = "2026-04-17"

        # 获取所有A股
        rs = bs.query_all_stock(day=trading_day)
        all_stocks = []
        while (rs.error_code == '0') & rs.next():
            all_stocks.append(rs.get_row_data())

        # 转换为DataFrame
        df = pd.DataFrame(all_stocks, columns=rs.fields)

        # 过滤出个股（排除指数）
        # code_pattern: sh.600000 或 sz.000001 格式
        df = df[df['code'].str.match(r'^(sh|sz)\.(600|601|603|605|000|001|002|003|300|301|688|689)\d{3}$', na=False)]

        # 提取纯数字代码
        df['code'] = df['code'].str.extract(r'\.(\d{6})$')[0]

        # 保存
        output_file = self.data_dir / "stock_list.parquet"
        pl.from_pandas(df).write_parquet(output_file)

        logger.info(f"✅ 股票列表已保存: {len(df)} 只股票")
        return df
    
    def collect_kline(self, code, start_date="2023-01-01", end_date=None):
        """采集单只股票K线数据"""
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        # 转换代码格式
        if code.startswith('6'):
            code = f"sh.{code}"
        else:
            code = f"sz.{code}"
        
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2"  # 前复权
        )
        
        data = []
        while (rs.error_code == '0') & rs.next():
            data.append(rs.get_row_data())
        
        if not data:
            return None
        
        df = pd.DataFrame(data, columns=rs.fields)
        
        # 类型转换
        numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def collect_all_kline(self, max_stocks=None):
        """采集所有股票K线数据"""
        logger.info("=" * 70)
        logger.info("📈 步骤2: 采集K线数据")
        logger.info("=" * 70)
        
        # 读取股票列表
        stock_list_file = self.data_dir / "stock_list.parquet"
        if not stock_list_file.exists():
            logger.error("股票列表不存在，请先运行股票列表采集")
            return
        
        stock_list = pl.read_parquet(stock_list_file)
        codes = stock_list['code'].to_list()
        
        if max_stocks:
            codes = codes[:max_stocks]
        
        logger.info(f"开始采集 {len(codes)} 只股票的K线数据...")
        
        success_count = 0
        fail_count = 0
        
        for i, code in enumerate(codes):
            try:
                # 清理代码
                code = str(code).strip().zfill(6)
                
                df = self.collect_kline(code)
                if df is not None and not df.empty:
                    # 重命名列以符合项目规范
                    df = df.rename(columns={
                        'date': 'trade_date',
                        'turn': 'turnover',
                        'pctChg': 'pct_chg'
                    })
                    
                    # 保存
                    output_file = self.kline_dir / f"{code}.parquet"
                    pl.from_pandas(df).write_parquet(output_file)
                    success_count += 1
                else:
                    fail_count += 1
                
                # 每100只股票报告一次进度
                if (i + 1) % 100 == 0:
                    logger.info(f"进度: {i+1}/{len(codes)} | 成功: {success_count} | 失败: {fail_count}")
                
            except Exception as e:
                fail_count += 1
                logger.warning(f"采集 {code} 失败: {e}")
        
        logger.info(f"✅ K线数据采集完成: 成功 {success_count}, 失败 {fail_count}")
    
    def collect_fundamental(self):
        """采集基本面数据"""
        logger.info("=" * 70)
        logger.info("📊 步骤3: 采集基本面数据")
        logger.info("=" * 70)
        
        year = datetime.now().year
        quarter = (datetime.now().month - 1) // 3 + 1
        
        # 获取季频盈利能力
        rs = bs.query_profit_data(code="sh.600000", year=year, quarter=quarter)
        profit_data = []
        while (rs.error_code == '0') & rs.next():
            profit_data.append(rs.get_row_data())
        
        if profit_data:
            df = pd.DataFrame(profit_data, columns=rs.fields)
            output_file = self.data_dir / f"fundamental_profit_{year}Q{quarter}.parquet"
            pl.from_pandas(df).write_parquet(output_file)
            logger.info(f"✅ 基本面数据已保存: {len(df)} 条")
        else:
            logger.warning("未获取到基本面数据")
    
    def run_all(self, max_stocks=None):
        """运行完整采集流程"""
        try:
            if not self.login():
                return
            
            # 1. 采集股票列表
            self.collect_stock_list()
            
            # 2. 采集K线数据
            self.collect_all_kline(max_stocks=max_stocks)
            
            # 3. 采集基本面数据
            self.collect_fundamental()
            
            logger.info("=" * 70)
            logger.info("🎉 所有数据采集完成!")
            logger.info("=" * 70)
            
        finally:
            self.logout()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='完整数据采集')
    parser.add_argument(
        '--max-stocks',
        type=int,
        default=None,
        help='限制采集股票数量（用于测试）'
    )
    
    args = parser.parse_args()
    
    collector = DataCollector()
    collector.run_all(max_stocks=args.max_stocks)


if __name__ == "__main__":
    main()
