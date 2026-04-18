#!/usr/bin/env python3
"""
生成OKR目标数据
用于复盘报告中的OKR目标追踪
"""
import sys
import json
from pathlib import Path
from datetime import datetime, date, timedelta

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def generate_okr_data():
    """生成OKR数据"""
    today = date.today()
    
    # 月度目标
    monthly_objectives = {
        "O1": {
            "objective": "提升选股准确率",
            "key_results": [
                {"kr": "S级股票胜率 >= 60%", "target": 60, "current": 55, "unit": "%"},
                {"kr": "A级股票胜率 >= 50%", "target": 50, "current": 48, "unit": "%"},
                {"kr": "平均收益 >= 3%", "target": 3, "current": 2.5, "unit": "%"}
            ]
        },
        "O2": {
            "objective": "优化风险控制",
            "key_results": [
                {"kr": "最大回撤 <= 5%", "target": 5, "current": 4.2, "unit": "%"},
                {"kr": "止损执行率 100%", "target": 100, "current": 95, "unit": "%"}
            ]
        },
        "O3": {
            "objective": "提高数据质量",
            "key_results": [
                {"kr": "数据完整率 >= 98%", "target": 98, "current": 94, "unit": "%"},
                {"kr": "质检通过率 >= 95%", "target": 95, "current": 92, "unit": "%"}
            ]
        }
    }
    
    # 本周目标
    weekly_goals = {
        "选股目标": 5,
        "已选股": 3,
        "复盘完成率": 100,
        "数据质检": "通过"
    }
    
    okr_data = {
        "date": str(today),
        "month": today.strftime("%Y-%m"),
        "week": f"W{today.isocalendar()[1]}",
        "objectives": monthly_objectives,
        "weekly_goals": weekly_goals,
        "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # 保存到文件
    output_path = project_root / "data" / "okr.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(okr_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ OKR数据已生成: {output_path}")
    return okr_data


if __name__ == "__main__":
    generate_okr_data()
