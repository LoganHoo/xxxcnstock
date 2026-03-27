"""股本数据采集脚本"""
import logging
import time
import requests
import polars as pl
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime


class ShareDataFetcher:
    """股本数据采集器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch_from_eastmoney(self, code: str) -> Dict[str, Any]:
        """从东方财富获取股本数据
        
        Args:
            code: 股票代码
            
        Returns:
            股本数据字典
        """
        try:
            url = "http://push2.eastmoney.com/api/qt/stock/get"
            params = {
                'secid': f"{'1' if code.startswith('6') else '0'}.{code}",
                'fields': 'f57,f58,f116,f117,f118,f119,f120,f121,f122,f123'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data and 'data' in data and data['data']:
                stock_data = data['data']
                return {
                    'code': code,
                    'name': stock_data.get('f58', ''),
                    'total_share': stock_data.get('f119', 0) / 10000,
                    'float_share': stock_data.get('f120', 0) / 10000
                }
        except Exception as e:
            self.logger.error(f"获取股本数据失败 {code}: {e}")
        
        return None
    
    def fetch_all_stocks(self, stock_codes: List[str]) -> List[Dict[str, Any]]:
        """获取所有股票的股本数据
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            股本数据列表
        """
        results = []
        total = len(stock_codes)
        
        for i, code in enumerate(stock_codes, 1):
            self.logger.info(f"获取股本数据 [{i}/{total}]: {code}")
            
            data = self.fetch_from_eastmoney(code)
            if data:
                results.append(data)
            
            time.sleep(0.1)
        
        return results
    
    def save_to_parquet(self, data: List[Dict[str, Any]], output_path: str):
        """保存股本数据到 Parquet 文件
        
        Args:
            data: 股本数据列表
            output_path: 输出文件路径
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        df = pl.DataFrame(data)
        
        df = df.with_columns([
            pl.lit(datetime.now().isoformat()).alias('update_time')
        ])
        
        df.write_parquet(output_path)
        self.logger.info(f"股本数据已保存到 {output_path}, 共 {len(df)} 条记录")


def main():
    """主函数"""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from config.config_manager import ConfigManager
    
    config = ConfigManager('config/xcn_comm.yaml')
    
    stock_list_path = config.config['data_paths'].get('stock_list', 'data/stock_list.parquet')
    if Path(stock_list_path).exists():
        stock_list = pl.read_parquet(stock_list_path)
        stock_codes = stock_list['code'].to_list()
    else:
        logging.warning(f"股票列表文件不存在: {stock_list_path}")
        return
    
    fetcher = ShareDataFetcher()
    share_data = fetcher.fetch_all_stocks(stock_codes[:100])  # 限制数量避免长时间运行
    
    output_path = config.config['recommendation']['market_cap']['share_data_path']
    fetcher.save_to_parquet(share_data, output_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
