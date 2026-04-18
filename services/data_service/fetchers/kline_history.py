#!/usr/bin/env python3
"""
历史K线数据获取器
使用Baostock获取全量历史K线数据
支持增量更新
"""
import importlib
import pandas as pd
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import time

from core.logger import setup_logger

logger = setup_logger("kline_fetcher", log_file="system/kline.log")


def _get_baostock_client():
    """延迟加载baostock"""
    return importlib.import_module("baostock")


@dataclass
class KlineData:
    """K线数据"""
    code: str
    date: str
    open: float
    close: float
    high: float
    low: float
    volume: int
    amount: float
    adjustflag: str  # 1-后复权, 2-前复权, 3-不复权


class KlineHistoryFetcher:
    """历史K线数据获取器"""
    
    def __init__(self):
        self._bs = None
        self._logged_in = False
    
    def _login(self) -> bool:
        """登录Baostock"""
        if self._logged_in:
            return True
        
        try:
            bs = _get_baostock_client()
            lg = bs.login()
            if lg.error_code == '0':
                self._bs = bs
                self._logged_in = True
                logger.info("Baostock登录成功")
                return True
            else:
                logger.error(f"Baostock登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"Baostock登录异常: {e}")
            return False
    
    def _logout(self):
        """登出Baostock"""
        if self._logged_in and self._bs:
            try:
                self._bs.logout()
                logger.info("Baostock登出成功")
            except:
                pass
            finally:
                self._logged_in = False
                self._bs = None
    
    def _convert_code(self, code: str) -> str:
        """转换股票代码格式"""
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            return f"sz.{code}"
        return code
    
    async def fetch_kline(
        self,
        code: str,
        start_date: str = "2020-01-01",
        end_date: str = None,
        adjust: str = "2"  # 默认前复权
    ) -> pd.DataFrame:
        """
        获取单只股票历史K线
        
        Args:
            code: 股票代码(6位数字)
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            adjust: 复权类型 1-后复权, 2-前复权, 3-不复权
        Returns:
            DataFrame
        """
        if not self._login():
            return pd.DataFrame()
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            code_bs = self._convert_code(code)
            
            # 查询K线数据
            rs = self._bs.query_history_k_data_plus(
                code_bs,
                "date,code,open,high,low,close,preclose,volume,amount,adjustflag",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag=adjust
            )
            
            if rs.error_code != '0':
                logger.warning(f"{code} 查询失败: {rs.error_msg}")
                return pd.DataFrame()
            
            # 解析数据
            data = []
            while rs.next():
                row = rs.get_row_data()
                data.append({
                    'code': code,
                    'date': row[0],
                    'open': float(row[2]) if row[2] else 0,
                    'high': float(row[3]) if row[3] else 0,
                    'low': float(row[4]) if row[4] else 0,
                    'close': float(row[5]) if row[5] else 0,
                    'pre_close': float(row[6]) if row[6] else 0,
                    'volume': int(row[7]) if row[7] else 0,
                    'amount': float(row[8]) if row[8] else 0,
                    'adjustflag': row[9]
                })
            
            if data:
                df = pd.DataFrame(data)
                logger.debug(f"{code} 获取K线 {len(df)} 条")
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"{code} 采集异常: {e}")
            return pd.DataFrame()
    
    async def fetch_batch(
        self,
        stock_list: List[str],
        start_date: str = "2020-01-01",
        end_date: str = None,
        output_dir: str = "data/kline"
    ) -> Dict[str, int]:
        """
        批量获取K线数据
        
        Args:
            stock_list: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            output_dir: 输出目录
        Returns:
            统计结果 {成功数, 失败数}
        """
        if not self._login():
            return {"success": 0, "failed": 0}
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        success = 0
        failed = 0
        total = len(stock_list)
        
        logger.info(f"开始批量采集 {total} 只股票的K线数据")
        
        for i, code in enumerate(stock_list, 1):
            try:
                df = await self.fetch_kline(code, start_date, end_date)
                
                if not df.empty:
                    # 保存为parquet
                    file_path = output_path / f"{code}.parquet"
                    df.to_parquet(file_path, index=False)
                    success += 1
                else:
                    failed += 1
                
                # 每100只记录进度
                if i % 100 == 0:
                    logger.info(f"进度: {i}/{total} ({i/total*100:.1f}%), 成功: {success}, 失败: {failed}")
                
                # 限速，避免请求过快
                time.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"{code} 处理失败: {e}")
                failed += 1
                continue
        
        self._logout()
        logger.info(f"批量采集完成: 成功 {success}/{total}, 失败 {failed}")
        
        return {"success": success, "failed": failed}
    
    async def incremental_update(
        self,
        stock_list: List[str],
        output_dir: str = "data/kline"
    ) -> Dict[str, int]:
        """
        增量更新K线数据
        
        Args:
            stock_list: 股票代码列表
            output_dir: 输出目录
        Returns:
            统计结果
        """
        output_path = Path(output_dir)
        
        # 获取最后更新日期
        last_dates = {}
        for code in stock_list:
            file_path = output_path / f"{code}.parquet"
            if file_path.exists():
                try:
                    df = pd.read_parquet(file_path)
                    if not df.empty:
                        last_date = df['date'].max()
                        last_dates[code] = last_date
                except:
                    pass
        
        logger.info(f"需要增量更新的股票: {len(last_dates)}/{len(stock_list)}")
        
        # 对每个股票进行增量更新
        success = 0
        failed = 0
        
        for code in stock_list:
            try:
                if code in last_dates:
                    # 从最后日期+1天开始
                    start_date = (datetime.strptime(last_dates[code], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    # 全量采集
                    start_date = "2020-01-01"
                
                df_new = await self.fetch_kline(code, start_date)
                
                if not df_new.empty:
                    file_path = output_path / f"{code}.parquet"
                    
                    # 合并数据
                    if file_path.exists():
                        df_old = pd.read_parquet(file_path)
                        df_combined = pd.concat([df_old, df_new], ignore_index=True)
                        # 去重
                        df_combined = df_combined.drop_duplicates(subset=['date'], keep='last')
                        df_combined = df_combined.sort_values('date')
                    else:
                        df_combined = df_new
                    
                    df_combined.to_parquet(file_path, index=False)
                    success += 1
                
                time.sleep(0.05)  # 限速
                
            except Exception as e:
                logger.warning(f"{code} 增量更新失败: {e}")
                failed += 1
                continue
        
        logger.info(f"增量更新完成: 成功 {success}, 失败 {failed}")
        return {"success": success, "failed": failed}
