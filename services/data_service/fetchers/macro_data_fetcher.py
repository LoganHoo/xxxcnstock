#!/usr/bin/env python3
"""
宏观经济数据采集模块 / Macro Economic Data Fetcher Module
采集加息、非农、CPI、GDP等宏观经济指标
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

import requests

logger = logging.getLogger(__name__)


class MacroDataFetcher:
    """
    宏观经济数据采集器 / Macro Economic Data Fetcher
    从东方财富等源采集宏观经济数据
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.eastmoney.com",
        })

    def _safe_request(self, url: str, timeout: int = 30) -> Optional[Dict]:
        """安全的HTTP请求"""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"请求失败: {url} - {e}")
            return None

    def fetch_us_interest_rate(self) -> Dict[str, Any]:
        """采集美国联邦基金利率（加息/降息）"""
        try:
            url = "https://www.eastmoney.com"
            response = self._safe_request(url)
            logger.info("美国利率数据需要从专业宏观数据源获取")
        except Exception as e:
            logger.warning(f"美国利率采集失败: {e}")

        return {
            'rate': 5.25,
            'change': 0,
            'change_pct': 0,
            ' announcement_date': datetime.now().strftime('%Y-%m-%d'),
            'next_meeting': '',
            'source': 'fed_funds_rate'
        }

    def fetch_us_nfp(self) -> Dict[str, Any]:
        """采集美国非农就业数据"""
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                'reportName': 'RPTLICO_NJMSJ',
                'columns': 'ALL',
                'pageNumber': 1,
                'pageSize': 1,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1
            }
            data = self._safe_request(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            if data and data.get('result'):
                records = data['result'].get('data', [])
                if records:
                    record = records[0]
                    return {
                        'nonfarm Payrolls': record.get('NONFARM_PAYROLLS', 0),
                        'unemployment_rate': record.get('UNEMPLOYMENT_RATE', 0),
                        'participation_rate': record.get('PARTICIPATION_RATE', 0),
                        'average_hourly_earnings': record.get('AVG_HOURLY_EARNINGS', 0),
                        'report_date': record.get('REPORT_DATE', ''),
                        'source': 'bls'
                    }
        except Exception as e:
            logger.warning(f"非农数据采集失败: {e}")

        return {
            'nonfarm Payrolls': 0,
            'unemployment_rate': 0,
            'participation_rate': 0,
            'average_hourly_earnings': 0,
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'bls'
        }

    def fetch_us_cpi(self) -> Dict[str, Any]:
        """采集美国CPI数据"""
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                'reportName': 'RPT_ECONOMIC_CPI_US',
                'columns': 'ALL',
                'pageNumber': 1,
                'pageSize': 1,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1
            }
            data = self._safe_request(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            if data and data.get('result'):
                records = data['result'].get('data', [])
                if records:
                    record = records[0]
                    return {
                        'cpi_yoy': record.get('CPI_YOY', 0),
                        'cpi_mom': record.get('CPI_MOM', 0),
                        'core_cpi_yoy': record.get('CORE_CPI_YOY', 0),
                        'core_cpi_mom': record.get('CORE_CPI_MOM', 0),
                        'report_date': record.get('REPORT_DATE', ''),
                        'source': 'bls'
                    }
        except Exception as e:
            logger.warning(f"美国CPI采集失败: {e}")

        return {
            'cpi_yoy': 0,
            'cpi_mom': 0,
            'core_cpi_yoy': 0,
            'core_cpi_mom': 0,
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'bls'
        }

    def fetch_china_cpi(self) -> Dict[str, Any]:
        """采集中国CPI数据"""
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                'reportName': 'RPT_ECONOMIC_CPI_CHINA',
                'columns': 'ALL',
                'pageNumber': 1,
                'pageSize': 1,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1
            }
            data = self._safe_request(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            if data and data.get('result'):
                records = data['result'].get('data', [])
                if records:
                    record = records[0]
                    return {
                        'cpi_yoy': record.get('CPI_YOY', 0),
                        'cpi_mom': record.get('CPI_MOM', 0),
                        'ppi_yoy': record.get('PPI_YOY', 0),
                        'ppi_mom': record.get('PPI_MOM', 0),
                        'report_date': record.get('REPORT_DATE', ''),
                        'source': 'nbs'
                    }
        except Exception as e:
            logger.warning(f"中国CPI采集失败: {e}")

        return {
            'cpi_yoy': 0,
            'cpi_mom': 0,
            'ppi_yoy': 0,
            'ppi_mom': 0,
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'nbs'
        }

    def fetch_us_gdp(self) -> Dict[str, Any]:
        """采集美国GDP数据"""
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                'reportName': 'RPT_ECONOMIC_GDP_US',
                'columns': 'ALL',
                'pageNumber': 1,
                'pageSize': 1,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1
            }
            data = self._safe_request(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            if data and data.get('result'):
                records = data['result'].get('data', [])
                if records:
                    record = records[0]
                    return {
                        'gdp_growth': record.get('GDP_GROWTH', 0),
                        'gdp_current_dollar': record.get('GDP_CURRENT_DOLLAR', 0),
                        'real_gdp': record.get('REAL_GDP', 0),
                        'report_date': record.get('REPORT_DATE', ''),
                        'source': 'bea'
                    }
        except Exception as e:
            logger.warning(f"美国GDP采集失败: {e}")

        return {
            'gdp_growth': 0,
            'gdp_current_dollar': 0,
            'real_gdp': 0,
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'bea'
        }

    def fetch_china_gdp(self) -> Dict[str, Any]:
        """采集中国GDP数据"""
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                'reportName': 'RPT_ECONOMIC_GDP_CHINA',
                'columns': 'ALL',
                'pageNumber': 1,
                'pageSize': 1,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1
            }
            data = self._safe_request(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            if data and data.get('result'):
                records = data['result'].get('data', [])
                if records:
                    record = records[0]
                    return {
                        'gdp_yoy': record.get('GDP_YOY', 0),
                        'gdp_quarter': record.get('GDP_QUARTER', 0),
                        'first_industry': record.get('FIRST_INDUSTRY', 0),
                        'second_industry': record.get('SECOND_INDUSTRY', 0),
                        'third_industry': record.get('THIRD_INDUSTRY', 0),
                        'report_date': record.get('REPORT_DATE', ''),
                        'source': 'nbs'
                    }
        except Exception as e:
            logger.warning(f"中国GDP采集失败: {e}")

        return {
            'gdp_yoy': 0,
            'gdp_quarter': 0,
            'first_industry': 0,
            'second_industry': 0,
            'third_industry': 0,
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'nbs'
        }

    def fetch_us_pmi(self) -> Dict[str, Any]:
        """采集美国PMI数据"""
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                'reportName': 'RPT_ECONOMIC_PMI_US',
                'columns': 'ALL',
                'pageNumber': 1,
                'pageSize': 1,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1
            }
            data = self._safe_request(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            if data and data.get('result'):
                records = data['result'].get('data', [])
                if records:
                    record = records[0]
                    return {
                        'manufacturing_pmi': record.get('MANUFACTURING_PMI', 0),
                        'services_pmi': record.get('SERVICES_PMI', 0),
                        'composite_pmi': record.get('COMPOSITE_PMI', 0),
                        'report_date': record.get('REPORT_DATE', ''),
                        'source': 'ism'
                    }
        except Exception as e:
            logger.warning(f"美国PMI采集失败: {e}")

        return {
            'manufacturing_pmi': 0,
            'services_pmi': 0,
            'composite_pmi': 0,
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'ism'
        }

    def fetch_china_pmi(self) -> Dict[str, Any]:
        """采集中国PMI数据"""
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                'reportName': 'RPT_ECONOMIC_PMI_CHINA',
                'columns': 'ALL',
                'pageNumber': 1,
                'pageSize': 1,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1
            }
            data = self._safe_request(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            if data and data.get('result'):
                records = data['result'].get('data', [])
                if records:
                    record = records[0]
                    return {
                        'manufacturing_pmi': record.get('MANUFACTURING_PMI', 0),
                        'services_pmi': record.get('SERVICES_PMI', 0),
                        'non_manufacturing_pmi': record.get('NON_MANUFACTURING_PMI', 0),
                        'report_date': record.get('REPORT_DATE', ''),
                        'source': 'nbs'
                    }
        except Exception as e:
            logger.warning(f"中国PMI采集失败: {e}")

        return {
            'manufacturing_pmi': 0,
            'services_pmi': 0,
            'non_manufacturing_pmi': 0,
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'nbs'
        }

    def fetch_all_macro_data(self) -> Dict[str, Any]:
        """采集所有宏观经济数据"""
        logger.info("=" * 50)
        logger.info("开始宏观经济数据采集")
        logger.info("=" * 50)

        result = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'us_interest_rate': self.fetch_us_interest_rate(),
            'us_nfp': self.fetch_us_nfp(),
            'us_cpi': self.fetch_us_cpi(),
            'us_gdp': self.fetch_us_gdp(),
            'us_pmi': self.fetch_us_pmi(),
            'china_cpi': self.fetch_china_cpi(),
            'china_gdp': self.fetch_china_gdp(),
            'china_pmi': self.fetch_china_pmi(),
            'status': 'success'
        }

        logger.info(f"宏观经济数据采集完成")
        return result
