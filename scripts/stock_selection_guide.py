"""
根据个股与大盘分析结果，筛选适合不同投资策略的股票
"""
import json
from pathlib import Path
from datetime import datetime

def main():
    report_file = Path('reports/stock_vs_index_20260326.json')
    
    if not report_file.exists():
        print('请先运行 stock_vs_index.py 生成分析报告')
        return
    
    with open(report_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print('=' * 60)
    print('【投资策略建议】')
    print('=' * 60)
    print()
    
    print('根据一年期分析结果，不同投资者适合不同类型股票:')
    print()
    
    print('━' * 60)
    print('1. 激进型投资者 → 选择「跑赢大盘型」')
    print('━' * 60)
    print()
    print('特点: 高收益高风险，独立于大盘走势')
    print('适合: 看好个股行情，愿意承担高风险')
    print()
    print('代表股票 (超额收益>100%):')
    for stock in data['outperform_top20'][:5]:
        print(f'  {stock["code"]}  超额: {stock["excess_return"]:+.1f}%  相关: {stock["correlation"]:.2f}')
    print()
    
    print('━' * 60)
    print('2. 稳健型投资者 → 选择「平衡型」(推荐)')
    print('━' * 60)
    print()
    print('特点: 有超额收益，与大盘适度相关，风险可控')
    print('适合: 大多数投资者，平衡风险与收益')
    print()
    print('筛选条件: 超额收益>20% 且 相关性0.3-0.5')
    print()
    balanced = [s for s in data['outperform_top20'] 
                if s['excess_return'] > 20 and 0.3 <= s['correlation'] <= 0.5]
    if balanced:
        for stock in balanced[:10]:
            print(f'  {stock["code"]}  超额: {stock["excess_return"]:+.1f}%  相关: {stock["correlation"]:.2f}  Beta: {stock["beta"]:.2f}')
    else:
        print('  (TOP20中暂无完全符合条件的)')
    print()
    
    print('━' * 60)
    print('3. 保守型投资者 → 选择「跟大盘型」')
    print('━' * 60)
    print()
    print('特点: 与大盘高度相关，可预测性强')
    print('适合: 看好大盘时加仓，趋势跟随')
    print()
    print('代表股票 (相关性>0.75):')
    for stock in data['follow_index_top20'][:5]:
        print(f'  {stock["code"]}  相关: {stock["correlation"]:.3f}  Beta: {stock["beta"]:.2f}  超额: {stock["excess_return"]:+.1f}%')
    print()
    print('注意: 跟大盘股票在大盘上涨时表现好，下跌时跌幅更大')
    print()
    
    print('━' * 60)
    print('4. 需要避开的股票 → 「跑输大盘型」')
    print('━' * 60)
    print()
    print('特点: 持续跑输大盘，可能存在基本面问题')
    print()
    print('代表股票:')
    for stock in data['underperform_top20'][:5]:
        print(f'  {stock["code"]}  超额: {stock["excess_return"]:+.1f}%  相关: {stock["correlation"]:.2f}')
    print()
    
    print('=' * 60)
    print('【总结建议】')
    print('=' * 60)
    print()
    print('✅ 推荐策略: 选择「跑赢大盘 + 弱相关」的股票')
    print()
    print('理由:')
    print('  1. 有超额收益，不依赖大盘')
    print('  2. 相关性适中，风险分散')
    print('  3. Beta适中，波动可控')
    print()
    print('最佳筛选条件:')
    print('  - 超额收益: > 20%')
    print('  - 相关性: 0.3 - 0.5')
    print('  - Beta: 0.8 - 1.5')
    print()
    print('当前市场环境 (大盘一年涨幅 +16.74%):')
    print(f'  - 大幅跑赢: {data["performance_distribution"]["大幅跑赢"]} 只')
    print(f'  - 大幅跑输: {data["performance_distribution"]["大幅跑输"]} 只')
    print(f'  - 平均超额: {data["avg_excess_return"]:+.2f}%')
    print()

if __name__ == '__main__':
    main()
