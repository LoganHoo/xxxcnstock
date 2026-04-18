"""大盘指数数据同步服务 - 同步Parquet数据到MySQL"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import polars as pl
from dotenv import load_dotenv

load_dotenv()


class IndexSyncService:
    """大盘指数数据同步服务"""
    
    # 指数配置
    INDICES = [
        ('000001', 'sh000001', '上证指数'),
        ('399001', 'sz399001', '深证成指'),
        ('399006', 'sz399006', '创业板指'),
        ('000300', 'sh000300', '沪深300'),
        ('000016', 'sh000016', '上证50'),
        ('000905', 'sh000905', '中证500'),
        ('000852', 'sh000852', '中证1000'),
    ]
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.index_dir = self.project_root / "data" / "index"
        self.db_config = self._get_db_config()
        
    def _get_db_config(self) -> Dict:
        """获取数据库配置"""
        return {
            'host': os.getenv('DB_HOST', '49.233.10.199'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'user': os.getenv('DB_USER', 'nextai'),
            'password': os.getenv('DB_PASSWORD', '100200'),
            'database': os.getenv('DB_NAME', 'xcn_db'),
            'charset': 'utf8mb4',
            'cursorclass': DictCursor
        }
    
    def _get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(**self.db_config)
    
    def init_table(self) -> bool:
        """初始化指数数据表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS index_daily (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(20) NOT NULL COMMENT '指数代码',
            symbol VARCHAR(20) NOT NULL COMMENT '指数符号',
            name VARCHAR(50) NOT NULL COMMENT '指数名称',
            trade_date DATE NOT NULL COMMENT '交易日期',
            open DECIMAL(10, 3) COMMENT '开盘价',
            high DECIMAL(10, 3) COMMENT '最高价',
            low DECIMAL(10, 3) COMMENT '最低价',
            close DECIMAL(10, 3) NOT NULL COMMENT '收盘价',
            volume BIGINT COMMENT '成交量',
            amount BIGINT COMMENT '成交额',
            change_pct DECIMAL(6, 3) COMMENT '涨跌幅%',
            change_amount DECIMAL(10, 3) COMMENT '涨跌额',
            ma5 DECIMAL(10, 3) COMMENT '5日均线',
            ma10 DECIMAL(10, 3) COMMENT '10日均线',
            ma20 DECIMAL(10, 3) COMMENT '20日均线',
            ma60 DECIMAL(10, 3) COMMENT '60日均线',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_date (code, trade_date),
            INDEX idx_trade_date (trade_date),
            INDEX idx_code (code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='大盘指数日线数据';
        """
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
            conn.close()
            print("✓ 指数数据表初始化成功")
            return True
        except Exception as e:
            print(f"✗ 表初始化失败: {e}")
            return False
    
    def _calculate_ma(self, df: pl.DataFrame, window: int) -> Optional[float]:
        """计算移动平均线"""
        if len(df) < window:
            return None
        return df.tail(window)['close'].mean()
    
    def _calculate_change_pct(self, current: float, previous: float) -> float:
        """计算涨跌幅"""
        if previous == 0:
            return 0
        return (current - previous) / previous * 100
    
    def sync_index(self, code: str, symbol: str, name: str) -> Tuple[int, int]:
        """同步单个指数数据到MySQL
        
        Returns:
            Tuple[int, int]: (插入记录数, 更新记录数)
        """
        parquet_file = self.index_dir / f"{code}.parquet"
        if not parquet_file.exists():
            print(f"  ✗ 文件不存在: {parquet_file}")
            return 0, 0
        
        try:
            df = pl.read_parquet(parquet_file)
            df = df.sort('trade_date')
            
            inserted = 0
            updated = 0
            
            conn = self._get_connection()
            
            for row in df.iter_rows(named=True):
                trade_date = row['trade_date']
                if isinstance(trade_date, str):
                    trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
                
                # 计算技术指标
                current_idx = df.filter(pl.col('trade_date') == trade_date)
                idx_position = df.get_column('trade_date').to_list().index(trade_date)
                
                # 涨跌幅
                change_pct = None
                change_amount = None
                if idx_position > 0:
                    prev_close = df.row(idx_position - 1, named=True)['close']
                    change_pct = self._calculate_change_pct(row['close'], prev_close)
                    change_amount = row['close'] - prev_close
                
                # 均线
                ma5 = self._calculate_ma(df.head(idx_position + 1), 5)
                ma10 = self._calculate_ma(df.head(idx_position + 1), 10)
                ma20 = self._calculate_ma(df.head(idx_position + 1), 20)
                ma60 = self._calculate_ma(df.head(idx_position + 1), 60)
                
                # 插入或更新数据
                upsert_sql = """
                INSERT INTO index_daily 
                (code, symbol, name, trade_date, open, high, low, close, volume, amount,
                 change_pct, change_amount, ma5, ma10, ma20, ma60)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                amount = VALUES(amount),
                change_pct = VALUES(change_pct),
                change_amount = VALUES(change_amount),
                ma5 = VALUES(ma5),
                ma10 = VALUES(ma10),
                ma20 = VALUES(ma20),
                ma60 = VALUES(ma60),
                updated_at = CURRENT_TIMESTAMP
                """
                
                with conn.cursor() as cursor:
                    cursor.execute(upsert_sql, (
                        code, symbol, name, trade_date,
                        row.get('open'), row.get('high'), row.get('low'), row['close'],
                        row.get('volume'), row.get('amount'),
                        change_pct, change_amount,
                        ma5, ma10, ma20, ma60
                    ))
                    
                    if cursor.rowcount == 1:
                        inserted += 1
                    elif cursor.rowcount == 2:
                        updated += 1
            
            conn.commit()
            conn.close()
            
            return inserted, updated
            
        except Exception as e:
            print(f"  ✗ 同步失败: {e}")
            return 0, 0
    
    def sync_all(self) -> Dict[str, Tuple[int, int]]:
        """同步所有指数数据"""
        print("=" * 60)
        print("开始同步大盘指数数据到MySQL")
        print("=" * 60)
        
        # 初始化表
        self.init_table()
        
        results = {}
        total_inserted = 0
        total_updated = 0
        
        print(f"\n同步 {len(self.INDICES)} 个指数...\n")
        
        for code, symbol, name in self.INDICES:
            print(f"同步 {name} ({code})...")
            inserted, updated = self.sync_index(code, symbol, name)
            results[code] = (inserted, updated)
            total_inserted += inserted
            total_updated += updated
            
            if inserted > 0 or updated > 0:
                print(f"  ✓ 插入: {inserted}, 更新: {updated}")
            else:
                print(f"  - 无变化")
        
        print("\n" + "=" * 60)
        print(f"同步完成: 插入 {total_inserted} 条, 更新 {total_updated} 条")
        print("=" * 60)
        
        return results
    
    def get_latest_date(self, code: str) -> Optional[date]:
        """获取MySQL中最新数据日期"""
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT MAX(trade_date) as latest FROM index_daily WHERE code = %s",
                    (code,)
                )
                result = cursor.fetchone()
                conn.close()
                
                if result and result['latest']:
                    return result['latest']
                return None
        except Exception as e:
            print(f"查询最新日期失败: {e}")
            return None
    
    def sync_incremental(self, days: int = 30) -> Dict[str, Tuple[int, int]]:
        """增量同步最近N天的数据"""
        print(f"=" * 60)
        print(f"开始增量同步（最近{days}天）")
        print(f"=" * 60)
        
        results = {}
        total_inserted = 0
        total_updated = 0
        
        for code, symbol, name in self.INDICES:
            parquet_file = self.index_dir / f"{code}.parquet"
            if not parquet_file.exists():
                continue
            
            try:
                df = pl.read_parquet(parquet_file)
                # 只取最近N天
                df = df.tail(days)
                
                inserted, updated = self.sync_index(code, symbol, name)
                results[code] = (inserted, updated)
                total_inserted += inserted
                total_updated += updated
                
            except Exception as e:
                print(f"  ✗ {name} 同步失败: {e}")
        
        print(f"\n增量同步完成: 插入 {total_inserted} 条, 更新 {total_updated} 条")
        return results


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='大盘指数数据同步到MySQL')
    parser.add_argument('--init', action='store_true', help='初始化表结构')
    parser.add_argument('--incremental', '-i', action='store_true', help='增量同步')
    parser.add_argument('--days', '-d', type=int, default=30, help='增量同步天数')
    
    args = parser.parse_args()
    
    service = IndexSyncService()
    
    if args.init:
        service.init_table()
    elif args.incremental:
        service.sync_incremental(args.days)
    else:
        service.sync_all()


if __name__ == "__main__":
    main()
