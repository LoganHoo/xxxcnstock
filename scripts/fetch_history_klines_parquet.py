"""
A股历史K线数据采集脚本 - Parquet版本
功能：
1. 采集所有A股的历史K线数据（3年）
2. 将数据存储到本地parquet文件
3. 支持增量更新
4. 支持断点续传
5. 采用令牌桶限流控制请求速率
"""
import os
import sys
import time
import json
import re
import requests
import pandas as pd
import threading
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from tqdm import tqdm
from pathlib import Path


class TokenBucket:
    """令牌桶限流器"""
    
    def __init__(self, rate: float = 5.0, capacity: int = 10):
        """
        初始化令牌桶
        
        Args:
            rate: 令牌生成速率（令牌/秒）
            capacity: 桶的最大容量
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_time = time.time()
        self.lock = threading.Lock()
    
    def acquire(self, tokens: int = 1) -> float:
        """
        获取令牌
        
        Args:
            tokens: 需要的令牌数量
            
        Returns:
            等待时间（秒）
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.last_time
            self.last_time = now
            
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            else:
                wait_time = (tokens - self.tokens) / self.rate
                time.sleep(wait_time)
                self.tokens = 0
                return wait_time


class HistoryKlineFetcher:
    """A股历史K线数据采集器 - Parquet存储"""
    
    def __init__(self, data_dir: str = "data/kline", rate_limit: float = 5.0):
        """
        初始化采集器
        
        Args:
            data_dir: 数据存储目录
            rate_limit: 请求速率限制（请求/秒）
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.stock_list_file = self.data_dir.parent / "stock_list.parquet"
        self.progress_file = self.data_dir / ".fetch_progress.json"
        
        self.rate_limiter = TokenBucket(rate=rate_limit, capacity=20)
        
        print(f"数据存储目录: {self.data_dir.absolute()}")
        print(f"请求速率限制: {rate_limit} 请求/秒")
    
    def _identify_stock_status(self, code: str, name: str, volume: float = None) -> tuple:
        """
        识别股票状态
        
        Args:
            code: 股票代码
            name: 股票名称
            volume: 成交量（用于判断停牌）
            
        Returns:
            (status, remark): 状态和备注
        """
        status = 'active'
        remark = ''
        
        if not name:
            return status, remark
        
        if '退市' in name:
            status = 'delisted'
            remark = '已退市'
        elif '*ST' in name:
            status = 'st'
            remark = '*ST股票（退市风险警示）'
        elif 'ST' in name:
            status = 'st'
            remark = 'ST股票（其他风险警示）'
        elif 'S*ST' in name:
            status = 'st'
            remark = 'S*ST股票（退市风险警示）'
        elif 'SST' in name:
            status = 'st'
            remark = 'SST股票（其他风险警示）'
        elif 'S' in name and name.startswith('S') and len(name) > 1 and name[1].isalpha():
            status = 'st'
            remark = 'S股票（未完成股改）'
        
        if volume is not None and volume == 0 and status == 'active':
            status = 'suspended'
            remark = '停牌'
        
        return status, remark
    
    def update_stock_list(self) -> List[Dict]:
        """更新股票列表（包括新股、退市等）"""
        print("\n正在更新股票列表...")
        
        all_stocks = []
        
        local_file = Path("data/stock_list_20260316.parquet")
        if local_file.exists():
            df = pd.read_parquet(local_file)
            for _, row in df.iterrows():
                code = row['code']
                name = row.get('name', '')
                status, remark = self._identify_stock_status(code, name)
                
                all_stocks.append({
                    'code': code,
                    'name': name,
                    'market': '上海' if code.startswith('6') else '深圳',
                    'status': status,
                    'remark': remark
                })
            print(f"使用本地股票列表: {len(all_stocks)} 只")
            
            st_count = sum(1 for s in all_stocks if s['status'] == 'st')
            delisted_count = sum(1 for s in all_stocks if s['status'] == 'delisted')
            suspended_count = sum(1 for s in all_stocks if s['status'] == 'suspended')
            print(f"  - ST股票: {st_count} 只")
            print(f"  - 退市股票: {delisted_count} 只")
            print(f"  - 停牌股票: {suspended_count} 只")
            
            df_save = pd.DataFrame(all_stocks)
            df_save.to_parquet(self.stock_list_file, index=False)
            
            return all_stocks
        
        if self.stock_list_file.exists():
            df = pd.read_parquet(self.stock_list_file)
            all_stocks = df.to_dict('records')
            if len(all_stocks) > 0:
                print(f"使用本地股票列表: {len(all_stocks)} 只")
                
                st_count = sum(1 for s in all_stocks if s.get('status') == 'st')
                delisted_count = sum(1 for s in all_stocks if s.get('status') == 'delisted')
                suspended_count = sum(1 for s in all_stocks if s.get('status') == 'suspended')
                print(f"  - ST股票: {st_count} 只")
                print(f"  - 退市股票: {delisted_count} 只")
                print(f"  - 停牌股票: {suspended_count} 只")
                
                return all_stocks
        
        print("从新浪财经获取股票列表...")
        
        try:
            url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData'
            params = {'page': 1, 'num': 8000, 'sort': 'symbol', 'asc': 1, 'node': 'hs_a', 'symbol': '', '_s_r_a': 'page'}
            headers = {'Referer': 'http://finance.sina.com.cn/'}
            
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            data = resp.json()
            
            if data:
                for stock in data:
                    code = stock.get('code', '')
                    name = stock.get('name', '')
                    volume = stock.get('volume', 0)
                    
                    if code and name and code.startswith(('6', '0', '3')):
                        market = '上海' if code.startswith('6') else '深圳'
                        status, remark = self._identify_stock_status(code, name, volume)
                        
                        all_stocks.append({
                            'code': code,
                            'name': name,
                            'market': market,
                            'status': status,
                            'remark': remark
                        })
                
                print(f"获取到 {len(all_stocks)} 只股票")
                
                st_count = sum(1 for s in all_stocks if s['status'] == 'st')
                delisted_count = sum(1 for s in all_stocks if s['status'] == 'delisted')
                suspended_count = sum(1 for s in all_stocks if s['status'] == 'suspended')
                print(f"  - ST股票: {st_count} 只")
                print(f"  - 退市股票: {delisted_count} 只")
                print(f"  - 停牌股票: {suspended_count} 只")
                
                df = pd.DataFrame(all_stocks)
                df.to_parquet(self.stock_list_file, index=False)
                print(f"股票列表已保存: {self.stock_list_file}")
                
                return all_stocks
        except Exception as e:
            print(f"获取股票列表失败: {e}")
        
        return []
    
    def fetch_kline_tencent(self, code: str, days: int = 730) -> Optional[pd.DataFrame]:
        """使用腾讯API获取K线数据（带限流）"""
        self.rate_limiter.acquire()
        
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
    
    def get_existing_data(self, code: str) -> Optional[pd.DataFrame]:
        """获取已存在的K线数据"""
        file_path = self.data_dir / f"{code}.parquet"
        if file_path.exists():
            return pd.read_parquet(file_path)
        return None
    
    def save_kline_data(self, df: pd.DataFrame, code: str):
        """保存K线数据到parquet文件"""
        if df is None or len(df) == 0:
            return
        
        file_path = self.data_dir / f"{code}.parquet"
        df.to_parquet(file_path, index=False)
    
    def merge_kline_data(self, existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
        """合并新旧K线数据"""
        if existing_df is None:
            return new_df
        
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['code', 'trade_date'], keep='last')
        combined = combined.sort_values('trade_date').reset_index(drop=True)
        
        return combined
    
    def fetch_all_history(self, days: int = 730):
        """采集所有股票的历史数据"""
        start_time = time.time()
        
        print(f"\n{'='*70}")
        print(f"开始采集A股历史K线数据 (最近{days}天 ≈ {days//365}年)")
        print(f"{'='*70}\n")
        
        stocks = self.update_stock_list()
        
        if not stocks:
            print("❌ 没有获取到股票列表，退出采集")
            return
        
        codes = [s['code'] for s in stocks if s['status'] == 'active']
        
        success_count = 0
        failed_count = 0
        skip_count = 0
        total_records = 0
        failed_codes = []
        
        processed_codes = set()
        
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                progress = json.load(f)
                processed_codes = set(progress.get('processed', []))
                success_count = progress.get('success', 0)
                failed_count = progress.get('failed', 0)
                skip_count = progress.get('skip', 0)
                total_records = progress.get('total_records', 0)
                failed_codes = progress.get('failed_codes', [])
            print(f"续传: 已处理 {len(processed_codes)} 只股票")
        
        print(f"待采集股票: {len(codes)} 只\n")
        
        pbar = tqdm(
            total=len(codes),
            desc="采集进度",
            unit="只",
            ncols=100,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )
        
        for i, code in enumerate(codes):
            if code in processed_codes:
                pbar.update(1)
                continue
            
            try:
                existing_df = self.get_existing_data(code)
                
                if existing_df is not None and len(existing_df) > 0:
                    latest_date = existing_df['trade_date'].max()
                    latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                    days_to_fetch = (datetime.now() - latest_dt).days
                    
                    if days_to_fetch <= 0:
                        skip_count += 1
                        processed_codes.add(code)
                        pbar.update(1)
                        continue
                    
                    days_to_fetch = min(days_to_fetch + 10, days)
                else:
                    days_to_fetch = days
                
                new_df = self.fetch_kline_tencent(code, days=days_to_fetch)
                
                if new_df is not None and len(new_df) > 0:
                    merged_df = self.merge_kline_data(existing_df, new_df)
                    self.save_kline_data(merged_df, code)
                    
                    success_count += 1
                    total_records += len(new_df)
                else:
                    failed_count += 1
                    failed_codes.append(code)
                
                processed_codes.add(code)
                
                if (i + 1) % 50 == 0:
                    progress = {
                        'processed': list(processed_codes),
                        'success': success_count,
                        'failed': failed_count,
                        'skip': skip_count,
                        'total_records': total_records,
                        'failed_codes': failed_codes,
                        'timestamp': datetime.now().isoformat()
                    }
                    with open(self.progress_file, 'w') as f:
                        json.dump(progress, f)
                
                pbar.update(1)
                
            except Exception as e:
                print(f"\n处理 {code} 时出错: {e}")
                failed_count += 1
                failed_codes.append(code)
                processed_codes.add(code)
                pbar.update(1)
        
        pbar.close()
        
        if self.progress_file.exists():
            os.remove(self.progress_file)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        self._print_completion_report(
            total_stocks=len(codes),
            success_count=success_count,
            failed_count=failed_count,
            skip_count=skip_count,
            total_records=total_records,
            failed_codes=failed_codes,
            elapsed_time=elapsed_time
        )
    
    def _print_completion_report(
        self,
        total_stocks: int,
        success_count: int,
        failed_count: int,
        skip_count: int,
        total_records: int,
        failed_codes: List[str],
        elapsed_time: float
    ):
        """打印详细的完成报告"""
        print(f"\n{'='*70}")
        print(f"采集完成报告")
        print(f"{'='*70}\n")
        
        print(f"📊 采集统计:")
        print(f"  ├─ 总股票数: {total_stocks:,} 只")
        print(f"  ├─ 成功采集: {success_count:,} 只")
        print(f"  ├─ 失败: {failed_count:,} 只")
        print(f"  ├─ 跳过: {skip_count:,} 只")
        print(f"  └─ 成功率: {success_count/(success_count+failed_count)*100:.2f}%")
        
        print(f"\n📈 数据统计:")
        print(f"  ├─ 总记录数: {total_records:,} 条")
        print(f"  └─ 平均每只股票: {total_records/max(success_count, 1):.0f} 条")
        
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)
        
        print(f"\n⏱️  时间统计:")
        print(f"  ├─ 总耗时: {hours}小时 {minutes}分钟 {seconds}秒")
        print(f"  ├─ 平均每只股票: {elapsed_time/max(success_count, 1):.2f} 秒")
        print(f"  └─ 采集速率: {success_count/elapsed_time:.2f} 只/秒")
        
        print(f"\n💾 数据存储:")
        print(f"  └─ 存储位置: {self.data_dir.absolute()}")
        
        if failed_codes and len(failed_codes) <= 20:
            print(f"\n❌ 失败股票列表 (前20只):")
            for i, code in enumerate(failed_codes[:20], 1):
                print(f"  {i}. {code}")
        
        print(f"\n{'='*70}")
        print(f"采集任务已完成！")
        print(f"{'='*70}\n")
    
    def get_statistics(self) -> Dict:
        """获取数据统计信息"""
        parquet_files = list(self.data_dir.glob("*.parquet"))
        
        total_stocks = len(parquet_files)
        total_records = 0
        date_range = None
        
        for file in parquet_files[:10]:
            df = pd.read_parquet(file)
            total_records += len(df)
            
            if len(df) > 0:
                min_date = df['trade_date'].min()
                max_date = df['trade_date'].max()
                if date_range is None:
                    date_range = (min_date, max_date)
                else:
                    date_range = (
                        min(date_range[0], min_date),
                        max(date_range[1], max_date)
                    )
        
        return {
            'total_stocks': total_stocks,
            'total_records': total_records * (total_stocks // 10),
            'date_range': date_range,
            'data_dir': str(self.data_dir.absolute())
        }


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='A股历史K线数据采集工具 - Parquet版本')
    parser.add_argument('--mode', type=str, default='full', choices=['full', 'stats'],
                        help='采集模式: full=全量采集, stats=统计信息')
    parser.add_argument('--days', type=int, default=1095,
                        help='采集历史天数 (默认1095天=3年)')
    parser.add_argument('--data-dir', type=str, default='data/kline',
                        help='数据存储目录')
    parser.add_argument('--rate-limit', type=float, default=5.0,
                        help='请求速率限制（请求/秒，默认5.0）')
    
    args = parser.parse_args()
    
    fetcher = HistoryKlineFetcher(data_dir=args.data_dir, rate_limit=args.rate_limit)
    
    if args.mode == 'full':
        fetcher.fetch_all_history(days=args.days)
    else:
        stats = fetcher.get_statistics()
        print(f"\n{'='*70}")
        print(f"数据统计")
        print(f"{'='*70}")
        print(f"股票数量: {stats['total_stocks']}")
        print(f"总记录数: {stats['total_records']}")
        if stats['date_range']:
            print(f"日期范围: {stats['date_range'][0]} ~ {stats['date_range'][1]}")
        print(f"存储位置: {stats['data_dir']}")


if __name__ == '__main__':
    main()
