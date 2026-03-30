"""历史行情数据审计和验证"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
import numpy as np
import requests
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

class DataAuditor:
    """数据审计器"""
    
    def __init__(self, data_dir='data/kline'):
        """
        初始化数据审计器
        
        Args:
            data_dir: 数据存储目录
        """
        self.data_dir = Path(data_dir)
    
    def audit(self):
        """
        执行数据质量审计
        
        Returns:
            审计报告
        """
        report = {
            'total_stocks': 0,
            'total_records': 0,
            'date_range': None,
            'quality_issues': []
        }
        
        # 获取所有Parquet文件
        parquet_files = list(self.data_dir.glob('*.parquet'))
        report['total_stocks'] = len(parquet_files)
        
        min_date = None
        max_date = None
        
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                report['total_records'] += len(df)
                
                # 检查日期范围
                if len(df) > 0:
                    file_min_date = df['trade_date'].min()
                    file_max_date = df['trade_date'].max()
                    
                    if min_date is None or file_min_date < min_date:
                        min_date = file_min_date
                    if max_date is None or file_max_date > max_date:
                        max_date = file_max_date
                
                # 检查数据质量
                self._check_data_quality(df, file.stem, report['quality_issues'])
                
            except Exception as e:
                report['quality_issues'].append(f"{file.stem}: 读取文件失败 - {str(e)}")
        
        if min_date and max_date:
            report['date_range'] = (min_date, max_date)
        
        return report
    
    def _check_data_quality(self, df, code, issues):
        """
        检查单只股票的数据质量
        
        Args:
            df: 股票数据
            code: 股票代码
            issues: 问题列表
        """
        # 检查必要列是否存在
        required_columns = ['trade_date', 'open', 'close', 'high', 'low', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            issues.append(f"{code}: 缺少列 - {missing_columns}")
        
        # 检查数据完整性
        if len(df) == 0:
            issues.append(f"{code}: 数据为空")
        
        # 检查价格数据是否合理
        if 'close' in df.columns:
            if (df['close'] <= 0).any():
                issues.append(f"{code}: 收盘价包含负值或零")
        
        # 检查成交量数据是否合理
        if 'volume' in df.columns:
            if (df['volume'] < 0).any():
                issues.append(f"{code}: 成交量包含负值")


def fetch_kline_sample(code, days=90):
    """获取K线样本数据进行验证"""
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
                return klines
    except:
        pass
    return None

def audit_analysis_data():
    """审计分析结果数据"""
    print('='*70)
    print('历史行情数据审计报告')
    print(f'审计时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*70)
    
    # 读取分析结果
    df = pd.read_parquet('data/enhanced_scores_full.parquet')
    
    print('\n[1] 基础数据检查')
    print('-'*50)
    print(f'总记录数: {len(df)}')
    print(f'列数: {len(df.columns)}')
    print(f'列名: {list(df.columns)}')
    
    # 缺失值检查
    print('\n[2] 缺失值检查')
    print('-'*50)
    missing = df.isnull().sum()
    for col, count in missing.items():
        if count > 0:
            print(f'  {col}: {count} 个缺失值 ({count/len(df)*100:.2f}%)')
    if missing.sum() == 0:
        print('  无缺失值')
    
    # 评分分布
    print('\n[3] 评分分布')
    print('-'*50)
    for grade in ['S', 'A', 'B', 'C']:
        count = len(df[df['grade'] == grade])
        avg = df[df['grade'] == grade]['enhanced_score'].mean()
        min_s = df[df['grade'] == grade]['enhanced_score'].min()
        max_s = df[df['grade'] == grade]['enhanced_score'].max()
        print(f'  {grade}级: {count}只, 均值:{avg:.1f}, 范围:[{min_s:.1f}, {max_s:.1f}]')
    
    # 异常值检查
    print('\n[4] 异常值检查')
    print('-'*50)
    
    # RSI异常
    rsi_invalid = df[(df['rsi'] < 0) | (df['rsi'] > 100)]
    if len(rsi_invalid) > 0:
        print(f'  RSI异常(不在0-100范围): {len(rsi_invalid)}只')
    else:
        print('  RSI范围正常 (0-100)')
    
    # 评分异常
    score_invalid = df[(df['enhanced_score'] < 0) | (df['enhanced_score'] > 100)]
    if len(score_invalid) > 0:
        print(f'  评分异常(不在0-100范围): {len(score_invalid)}只')
    else:
        print('  评分范围正常 (0-100)')
    
    # 价格异常
    price_invalid = df[df['price'] <= 0]
    if len(price_invalid) > 0:
        print(f'  价格异常(<=0): {len(price_invalid)}只')
    else:
        print('  价格范围正常')
    
    return df

def verify_kline_data():
    """验证K线数据质量"""
    print('\n[5] K线数据验证 (抽样检查)')
    print('-'*50)
    
    df = pd.read_parquet('data/enhanced_scores_full.parquet')
    
    # 随机抽样20只股票验证
    samples = df.sample(n=min(20, len(df)), random_state=42)
    
    issues = []
    verified = 0
    
    for _, row in samples.iterrows():
        code = row['code']
        name = row['name']
        
        klines = fetch_kline_sample(code, days=90)
        
        if klines is None:
            issues.append(f'{code} {name}: 无法获取K线数据')
            continue
        
        # 验证数据量
        if len(klines) < 60:
            issues.append(f'{code} {name}: K线数据不足({len(klines)}天)')
            continue
        
        # 验证最新日期
        latest_date = klines[-1][0]
        expected_date = datetime.now().strftime('%Y-%m-%d')
        
        # 验证价格一致性
        latest_close = float(klines[-1][2])
        stored_price = row['price']
        price_diff = abs(latest_close - stored_price) / stored_price * 100
        
        if price_diff > 5:
            issues.append(f'{code} {name}: 价格差异{price_diff:.1f}% (K线:{latest_close}, 存储:{stored_price})')
        else:
            verified += 1
        
        time.sleep(0.2)
    
    print(f'  抽样数量: {len(samples)}只')
    print(f'  验证通过: {verified}只')
    print(f'  发现问题: {len(issues)}只')
    
    if issues:
        print('\n  问题详情:')
        for issue in issues[:10]:
            print(f'    - {issue}')
        if len(issues) > 10:
            print(f'    ... 还有{len(issues)-10}个问题')
    
    return issues

def check_data_freshness():
    """检查数据新鲜度"""
    print('\n[6] 数据新鲜度检查')
    print('-'*50)
    
    df = pd.read_parquet('data/enhanced_scores_full.parquet')
    
    # 抽样检查最新K线日期
    samples = df.sample(n=min(10, len(df)), random_state=123)
    
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    fresh_count = 0
    stale_count = 0
    
    for _, row in samples.iterrows():
        code = row['code']
        klines = fetch_kline_sample(code, days=5)
        
        if klines:
            latest = klines[-1][0]
            if latest == today or latest == yesterday:
                fresh_count += 1
            else:
                stale_count += 1
                print(f'  {code}: 最新日期 {latest} (可能过期)')
        
        time.sleep(0.2)
    
    print(f'\n  数据新鲜: {fresh_count}只')
    print(f'  数据可能过期: {stale_count}只')
    
    return fresh_count, stale_count

def check_data_completeness():
    """检查数据完整性"""
    print('\n[7] 历史数据完整性检查')
    print('-'*50)
    
    df = pd.read_parquet('data/enhanced_scores_full.parquet')
    
    # 检查各指标是否都有值
    required_cols = ['enhanced_score', 'grade', 'trend', 'momentum', 'tech', 
                     'rsi', 'momentum_3d', 'momentum_10d', 'momentum_20d']
    
    incomplete = 0
    for col in required_cols:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                print(f'  {col}: {null_count}个空值')
                incomplete += null_count
    
    if incomplete == 0:
        print('  所有必需字段都有值')
    
    # 检查等级分布合理性
    print('\n[8] 等级分布合理性')
    print('-'*50)
    
    grade_dist = df['grade'].value_counts()
    total = len(df)
    
    for grade in ['S', 'A', 'B', 'C']:
        count = grade_dist.get(grade, 0)
        pct = count / total * 100
        print(f'  {grade}级: {count}只 ({pct:.1f}%)')
    
    # 正态分布检查 - S级和A级应该较少
    s_pct = grade_dist.get('S', 0) / total * 100
    a_pct = grade_dist.get('A', 0) / total * 100
    
    if s_pct > 20:
        print(f'\n  警告: S级占比{s_pct:.1f}%过高,可能评分标准偏宽松')
    elif s_pct < 3:
        print(f'\n  注意: S级占比{s_pct:.1f}%较低,评分标准可能偏严格')
    else:
        print(f'\n  S级占比{s_pct:.1f}%在合理范围内')

def main():
    audit_analysis_data()
    verify_kline_data()
    check_data_freshness()
    check_data_completeness()
    
    print('\n' + '='*70)
    print('审计完成')
    print('='*70)

if __name__ == '__main__':
    main()
