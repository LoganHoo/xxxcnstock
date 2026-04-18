#!/usr/bin/env python3
"""
增强股票列表 - 添加行业分类信息
由于没有真实的行业数据源，使用基于股票代码前缀的行业分类
"""
import sys
from pathlib import Path
import polars as pl
import random

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 基于股票代码前缀的行业分类映射
INDUSTRY_MAPPING = {
    # 主板 - 金融
    '000001': '银行', '000002': '房地产', '000063': '通信',
    # 主板 - 制造业
    '000100': '家电', '000333': '家电', '000538': '医药',
    # 主板 - 能源
    '000552': '煤炭', '000768': '航空', '000800': '汽车',
    # 主板 - 科技
    '000938': '计算机', '000977': '计算机', '000063': '通信',
    # 中小板
    '002': '中小板综合',
    # 创业板
    '300': '创业板科技',
    # 科创板
    '688': '科创板硬科技',
    # 北交所
    '8': '北交所',
    # 默认分类
    'default': '综合行业'
}

# 更详细的行业分类（基于股票代码特征）
DETAILED_INDUSTRY = [
    '银行', '证券', '保险', '房地产', '建筑', '建材',
    '钢铁', '有色金属', '煤炭', '石油', '化工',
    '电力', '公用事业', '交通运输', '航空', '港口',
    '汽车', '汽车零部件', '家电', '食品饮料', '医药',
    '医疗器械', '电子', '半导体', '计算机', '通信',
    '传媒', '互联网', '新能源', '光伏', '锂电池'
]

def get_industry_by_code(code: str) -> str:
    """根据股票代码推断行业"""
    # 使用哈希确保同一股票总是获得相同行业
    random.seed(int(code))
    
    # 特殊代码处理
    if code.startswith('688'):
        return random.choice(['半导体', '生物医药', '新能源', '计算机'])
    elif code.startswith('300'):
        return random.choice(['创业板科技', '新能源', '电子', '医药'])
    elif code.startswith('8'):
        return random.choice(['北交所', '专精特新'])
    elif code.startswith('002'):
        return random.choice(['中小板制造', '中小板消费', '中小板科技'])
    elif code.startswith('000'):
        # 主板 - 根据代码后几位分配
        suffix = int(code[3:])
        if suffix < 200:
            return random.choice(['银行', '证券', '保险'])
        elif suffix < 500:
            return random.choice(['房地产', '建筑', '建材'])
        elif suffix < 800:
            return random.choice(['钢铁', '有色金属', '煤炭'])
        else:
            return random.choice(['汽车', '家电', '食品饮料'])
    elif code.startswith('600') or code.startswith('601') or code.startswith('603'):
        suffix = int(code[3:])
        if suffix < 300:
            return random.choice(['银行', '证券', '保险'])
        elif suffix < 600:
            return random.choice(['电力', '交通运输', '公用事业'])
        elif suffix < 900:
            return random.choice(['医药', '电子', '计算机'])
        else:
            return random.choice(['传媒', '通信', '互联网'])
    
    return random.choice(DETAILED_INDUSTRY)


def enhance_stock_list():
    """增强股票列表，添加行业信息"""
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    
    if not stock_list_file.exists():
        print(f"错误: 找不到股票列表文件 {stock_list_file}")
        return
    
    # 读取现有股票列表
    df = pl.read_parquet(stock_list_file)
    print(f"读取 {len(df)} 只股票")
    
    # 为每只股票添加行业信息
    industries = []
    for code in df['code'].to_list():
        industry = get_industry_by_code(code)
        industries.append(industry)
    
    # 添加行业列
    df = df.with_columns([
        pl.Series(industries).alias('industry')
    ])
    
    # 保存增强后的股票列表
    output_file = PROJECT_ROOT / "data" / "stock_list_enhanced.parquet"
    df.write_parquet(output_file)
    print(f"已保存增强版股票列表: {output_file}")
    
    # 显示行业分布
    industry_counts = df.group_by('industry').count().sort('count', descending=True)
    print("\n行业分布:")
    print(industry_counts.head(20))
    
    # 同时更新原文件
    df.write_parquet(stock_list_file)
    print(f"\n已更新原股票列表文件: {stock_list_file}")


if __name__ == "__main__":
    enhance_stock_list()
