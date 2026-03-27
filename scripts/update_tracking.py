"""
更新推荐股票跟踪数据
"""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.stock_pick_verification_service import StockPickVerificationService


def update_tracking(track_date: str = None):
    """更新跟踪数据"""
    if track_date is None:
        track_date = date.today()
    else:
        track_date = date.fromisoformat(track_date)
    
    print(f"📅 更新日期: {track_date}")
    
    service = StockPickVerificationService()
    stats = service.update_tracking(track_date)
    
    print(f"✅ 更新完成: {stats}")
    
    return stats


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='更新推荐股票跟踪数据')
    parser.add_argument('--date', help='跟踪日期 (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    update_tracking(args.date)
