"""
A股历史K线数据采集脚本
功能：
1. 采集所有A股的历史K线数据
2. 支持增量更新
3. 将数据存储到MySQL数据库
4. 支持断点续传
"""
import os
import sys
import time
import json
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()


class HistoryKlineFetcher:
    """A股历史K线数据采集器"""
    
    def __init__(self):
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = int(os.getenv('DB_PORT', 3306))
        self.db_user = os.getenv('DB_USER', 'root')
        self.db_password = os.getenv('DB_PASSWORD', '')
        self.db_name = os.getenv('DB_NAME', 'quantdb')
        self.db_charset = os.getenv('DB_CHARSET', 'utf8mb4')
        
        self.engine = self._create_db_engine()
        self.Session = sessionmaker(bind=self.engine)
        
        self._init_database()
    
    def _create_db_engine(self):
        """创建数据库连接引擎"""
        connection_string = (
            f"mysql+pymysql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
            f"?charset={self.db_charset}"
        )
        return create_engine(
            connection_string,
            pool_size=int(os.getenv('DB_POOL_SIZE', 10)),
            pool_recycle=int(os.getenv('DB_POOL_RECYCLE', 3600)),
            echo=False
        )
    
    def _init_database(self):
        """初始化数据库表结构"""
        create_stock_list_sql = """
        CREATE TABLE IF NOT EXISTS stock_list (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(10) NOT NULL COMMENT '股票代码',
            name VARCHAR(50) NOT NULL COMMENT '股票名称',
            industry VARCHAR(50) DEFAULT '' COMMENT '所属行业',
            market VARCHAR(20) DEFAULT '' COMMENT '所属市场',
            list_date DATE DEFAULT NULL COMMENT '上市日期',
            status VARCHAR(20) DEFAULT 'active' COMMENT '状态: active=正常, delisted=退市, suspended=停牌',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code (code),
            KEY idx_status (status),
            KEY idx_market (market)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票列表';
        """
        
        create_klines_sql = """
        CREATE TABLE IF NOT EXISTS stock_klines (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(10) NOT NULL COMMENT '股票代码',
            trade_date DATE NOT NULL COMMENT '交易日期',
            open DECIMAL(10, 2) NOT NULL COMMENT '开盘价',
            close DECIMAL(10, 2) NOT NULL COMMENT '收盘价',
            high DECIMAL(10, 2) NOT NULL COMMENT '最高价',
            low DECIMAL(10, 2) NOT NULL COMMENT '最低价',
            volume BIGINT NOT NULL COMMENT '成交量',
            amount DECIMAL(20, 2) DEFAULT 0 COMMENT '成交额',
            change_pct DECIMAL(10, 2) DEFAULT 0 COMMENT '涨跌幅%',
            turnover_rate DECIMAL(10, 2) DEFAULT 0 COMMENT '换手率%',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_date (code, trade_date),
            KEY idx_trade_date (trade_date),
            KEY idx_code (code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票历史K线数据';
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_stock_list_sql))
            conn.execute(text(create_klines_sql))
            conn.commit()
        
        print("✅ 数据库表初始化完成")
    
    def update_stock_list(self) -> List[Dict]:
        """更新股票列表（包括新股、退市等）"""
        print("\n正在更新股票列表...")
        
        all_stocks = []
        
        print("从新浪财经获取股票列表...")
        
        markets = [
            ('sh', '上海'),
            ('sz', '深圳')
        ]
        
        for market_code, market_name in markets:
            try:
                if market_code == 'sh':
                    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData'
                    params = {'page': 1, 'num': 5000, 'sort': 'symbol', 'asc': 1, 'node': 'hs_a', 'symbol': '', '_s_r_a': 'page'}
                else:
                    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData'
                    params = {'page': 1, 'num': 5000, 'sort': 'symbol', 'asc': 1, 'node': 'hs_a', 'symbol': '', '_s_r_a': 'page'}
                
                headers = {'Referer': 'http://finance.sina.com.cn/'}
                resp = requests.get(url, params=params, headers=headers, timeout=30)
                data = resp.json()
                
                if data:
                    for stock in data:
                        code = stock.get('code', '')
                        name = stock.get('name', '')
                        
                        if code and name and code.startswith(('6', '0', '3')):
                            all_stocks.append({
                                'code': code,
                                'name': name,
                                'market': market_name,
                                'status': 'active'
                            })
                
                print(f"  {market_name}市场: {len([s for s in all_stocks if s['market'] == market_name])} 只")
                
            except Exception as e:
                print(f"  {market_name}市场获取失败: {e}")
        
        if all_stocks:
            print(f"\n获取到 {len(all_stocks)} 只股票，正在更新数据库...")
            
            df = pd.DataFrame(all_stocks)
            
            with self.engine.connect() as conn:
                existing_codes = conn.execute(text("SELECT code FROM stock_list")).fetchall()
                existing_codes = set([row[0] for row in existing_codes])
                
                new_stocks = df[~df['code'].isin(existing_codes)]
                
                if len(new_stocks) > 0:
                    new_stocks.to_sql('stock_list', self.engine, if_exists='append', index=False)
                    print(f"  新增股票: {len(new_stocks)} 只")
                
                current_codes = set(df['code'].tolist())
                delisted_codes = existing_codes - current_codes
                
                if delisted_codes:
                    conn.execute(
                        text("UPDATE stock_list SET status = 'delisted' WHERE code IN :codes"),
                        {'codes': tuple(delisted_codes)}
                    )
                    conn.commit()
                    print(f"  标记退市: {len(delisted_codes)} 只")
            
            print(f"✅ 股票列表更新完成")
            return all_stocks
        else:
            print("❌ 未获取到股票列表，使用本地数据...")
            return self._get_local_stock_list()
    
    def _get_local_stock_list(self) -> List[Dict]:
        """获取本地股票列表"""
        try:
            stock_list = pd.read_parquet('data/stock_list_20260316.parquet')
            stocks = []
            for _, row in stock_list.iterrows():
                stocks.append({
                    'code': row['code'],
                    'name': row.get('name', ''),
                    'market': '上海' if row['code'].startswith('6') else '深圳',
                    'status': 'active'
                })
            print(f"本地股票列表: {len(stocks)} 只")
            return stocks
        except Exception as e:
            print(f"读取本地股票列表失败: {e}")
            return []
    
    def fetch_stock_list(self) -> List[str]:
        """获取股票代码列表"""
        stocks = self.update_stock_list()
        return [s['code'] for s in stocks if s['status'] == 'active']
    
    def fetch_kline_tencent(self, code: str, days: int = 365) -> Optional[pd.DataFrame]:
        """使用腾讯API获取K线数据"""
        if code.startswith('6'):
            symbol = f'sh{code}'
        else:
            symbol = f'sz{code}'
        
        url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
        params = {
            '_var': f'kline_dayqfq_{symbol}',
            'param': f'{symbol},day,,,{days},qfq',
            'r': str(int(time.time() * 1000))
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://gu.qq.com/'
        }
        
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            
            text = r.text
            match = re.match(r'kline_dayqfq_\w+=(.*)', text)
            if match:
                data = json.loads(match.group(1))
                if data.get('code') == 0:
                    klines = data['data'][symbol].get('qfqday', [])
                    
                    records = []
                    for k in klines:
                        records.append({
                            'code': code,
                            'trade_date': k[0],
                            'open': float(k[1]),
                            'close': float(k[2]),
                            'high': float(k[3]),
                            'low': float(k[4]),
                            'volume': int(float(k[5])),
                        })
                    
                    if records:
                        return pd.DataFrame(records)
        except Exception as e:
            pass
        
        return None
    
    def get_latest_date(self, code: str) -> Optional[str]:
        """获取数据库中某只股票的最新日期"""
        sql = text("""
            SELECT MAX(trade_date) as latest_date 
            FROM stock_klines 
            WHERE code = :code
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(sql, {'code': code}).fetchone()
            if result and result[0]:
                return result[0].strftime('%Y-%m-%d')
        return None
    
    def save_to_database(self, df: pd.DataFrame):
        """将数据保存到数据库"""
        if df is None or len(df) == 0:
            return
        
        df.to_sql(
            'stock_klines',
            self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
    
    def fetch_all_history(self, days: int = 365):
        """采集所有股票的历史数据"""
        print(f"\n{'='*70}")
        print(f"开始采集A股历史K线数据 (最近{days}天)")
        print(f"{'='*70}\n")
        
        codes = self.fetch_stock_list()
        
        if not codes:
            print("❌ 没有获取到股票列表，退出采集")
            return
        
        success_count = 0
        failed_count = 0
        skip_count = 0
        
        progress_file = 'data/.fetch_history_progress.json'
        
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                processed_codes = set(progress.get('processed', []))
                success_count = progress.get('success', 0)
                failed_count = progress.get('failed', 0)
                skip_count = progress.get('skip', 0)
            print(f"续传: 已处理 {len(processed_codes)} 只股票")
        else:
            processed_codes = set()
        
        print(f"待采集股票: {len(codes)} 只\n")
        
        for i, code in enumerate(tqdm(codes, desc="采集进度")):
            if code in processed_codes:
                continue
            
            try:
                latest_date = self.get_latest_date(code)
                
                if latest_date:
                    days_to_fetch = (datetime.now() - datetime.strptime(latest_date, '%Y-%m-%d')).days
                    if days_to_fetch <= 0:
                        skip_count += 1
                        processed_codes.add(code)
                        continue
                    
                    df = self.fetch_kline_tencent(code, days=min(days_to_fetch + 10, days))
                else:
                    df = self.fetch_kline_tencent(code, days=days)
                
                if df is not None and len(df) > 0:
                    self.save_to_database(df)
                    success_count += 1
                else:
                    failed_count += 1
                
                processed_codes.add(code)
                
                if (i + 1) % 50 == 0:
                    progress = {
                        'processed': list(processed_codes),
                        'success': success_count,
                        'failed': failed_count,
                        'skip': skip_count,
                        'timestamp': datetime.now().isoformat()
                    }
                    with open(progress_file, 'w') as f:
                        json.dump(progress, f)
                    
                    print(f"\n进度: {i+1}/{len(codes)} | 成功: {success_count} | 失败: {failed_count} | 跳过: {skip_count}")
                
                time.sleep(0.15)
                
            except Exception as e:
                print(f"\n处理 {code} 时出错: {e}")
                failed_count += 1
        
        if os.path.exists(progress_file):
            os.remove(progress_file)
        
        print(f"\n{'='*70}")
        print(f"采集完成")
        print(f"{'='*70}")
        print(f"总股票数: {len(codes)}")
        print(f"成功采集: {success_count}")
        print(f"失败: {failed_count}")
        print(f"跳过: {skip_count}")
        print(f"成功率: {success_count/(success_count+failed_count)*100:.1f}%")
    
    def fetch_incremental(self):
        """增量更新今天的K线数据"""
        print(f"\n{'='*70}")
        print(f"增量更新今日K线数据")
        print(f"{'='*70}\n")
        
        codes = self.fetch_stock_list()
        
        if not codes:
            print("❌ 没有获取到股票列表，退出采集")
            return
        
        success_count = 0
        failed_count = 0
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        for code in tqdm(codes, desc="更新进度"):
            try:
                df = self.fetch_kline_tencent(code, days=10)
                
                if df is not None and len(df) > 0:
                    today_df = df[df['trade_date'] == today]
                    
                    if len(today_df) > 0:
                        self.save_to_database(today_df)
                        success_count += 1
                
                time.sleep(0.1)
                
            except Exception as e:
                failed_count += 1
        
        print(f"\n{'='*70}")
        print(f"增量更新完成")
        print(f"{'='*70}")
        print(f"成功更新: {success_count}")
        print(f"失败: {failed_count}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='A股历史K线数据采集工具')
    parser.add_argument('--mode', type=str, default='full', choices=['full', 'incremental'],
                        help='采集模式: full=全量采集, incremental=增量更新')
    parser.add_argument('--days', type=int, default=365,
                        help='采集历史天数 (默认365天)')
    
    args = parser.parse_args()
    
    fetcher = HistoryKlineFetcher()
    
    if args.mode == 'full':
        fetcher.fetch_all_history(days=args.days)
    else:
        fetcher.fetch_incremental()


if __name__ == '__main__':
    main()
