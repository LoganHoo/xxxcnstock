#!/usr/bin/env python3
"""
批量分析CCTV新闻联播
为所有缺失AI分析的日期生成分析结果
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime
from scripts.pipeline.cctv_analyzer import CCTVNewsProvider, AIAnalyzer


def get_dates_need_analysis():
    """获取需要AI分析的日期列表"""
    conn = pymysql.connect(
        host='49.233.10.199', port=3306, user='nextai', password='100200',
        database='xcn_db', charset='utf8mb4', cursorclass=DictCursor
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT news_date, full_content, main_topics
        FROM cctv_news_broadcast 
        WHERE ai_summary IS NULL 
          AND full_content IS NOT NULL
          AND news_date >= '2026-04-01'
        ORDER BY news_date DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


def analyze_and_save(date_str, content, provider, analyzer):
    """分析单条新闻并保存"""
    print(f"\n{'='*60}")
    print(f"分析日期: {date_str}")
    print(f"内容长度: {len(content)} 字符")
    print('='*60)
    
    try:
        analysis = analyzer.analyze_news(content, date_str)
        if analysis:
            print(f"✅ 分析完成")
            print(f"   摘要: {analysis.summary[:60]}...")
            print(f"   情绪: {analysis.overall_sentiment}")
            print(f"   热门板块: {analysis.hot_sectors[:50]}...")
            
            # 保存到数据库
            result = provider.save_ai_analysis(date_str, analysis, "批量自动分析")
            if result:
                print(f"✅ 保存成功")
                return True
            else:
                print(f"❌ 保存失败")
                return False
        else:
            print(f"❌ 分析返回空结果")
            return False
    except Exception as e:
        print(f"❌ 分析异常: {e}")
        return False


def main():
    print("="*60)
    print("CCTV新闻联播批量AI分析")
    print("="*60)
    
    # 获取需要分析的日期
    rows = get_dates_need_analysis()
    print(f"\n找到 {len(rows)} 条需要分析的新闻")
    
    if not rows:
        print("✅ 所有新闻已完成AI分析")
        return
    
    # 初始化分析器
    provider = CCTVNewsProvider()
    analyzer = AIAnalyzer()
    
    # 批量分析
    success_count = 0
    fail_count = 0
    
    for i, row in enumerate(rows, 1):
        date_str = row['news_date'].strftime('%Y-%m-%d') if hasattr(row['news_date'], 'strftime') else str(row['news_date'])
        print(f"\n[{i}/{len(rows)}] ", end="")
        
        if analyze_and_save(date_str, row['full_content'], provider, analyzer):
            success_count += 1
        else:
            fail_count += 1
    
    # 总结
    print(f"\n{'='*60}")
    print("分析完成!")
    print(f"成功: {success_count} 条")
    print(f"失败: {fail_count} 条")
    print(f"总计: {len(rows)} 条")
    print("="*60)


if __name__ == "__main__":
    main()
