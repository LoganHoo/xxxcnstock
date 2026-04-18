"""推荐股票跟踪更新 - 21:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime, date, timedelta
from core.logger import get_logger


class TrackingUpdater:
    """跟踪更新器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)
        self.holidays = {
            '2026-01-01', '2026-01-26', '2026-01-27', '2026-01-28', '2026-01-29',
            '2026-01-30', '2026-01-31', '2026-02-01', '2026-02-02',
            '2026-04-03', '2026-04-04', '2026-04-05',
            '2026-05-01', '2026-05-02', '2026-05-03',
            '2026-06-01', '2026-06-02', '2026-06-03',
            '2026-09-20', '2026-09-21', '2026-09-22',
            '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07'
        }

    def _is_trading_day(self, d: date) -> bool:
        """判断是否为交易日"""
        if d.weekday() >= 5:
            return False
        if d.strftime('%Y-%m-%d') in self.holidays:
            return False
        return True

    def _get_previous_trading_day(self) -> date:
        """获取前一个交易日"""
        d = date.today() - timedelta(days=1)
        while d >= date.today() - timedelta(days=7):
            if self._is_trading_day(d):
                return d
            d -= timedelta(days=1)
        return date.today()

    def _update_tracking(self, track_date: date):
        """执行跟踪更新核心逻辑"""
        from services.stock_pick_verification_service import StockPickVerificationService

        self.logger.info(f"📅 更新日期: {track_date}")

        service = StockPickVerificationService()
        stats = service.update_tracking(track_date)

        self.logger.info(f"✅ 更新完成: {stats}")
        return stats

    def run(self) -> bool:
        """执行跟踪更新"""
        self.logger.info("开始更新推荐股票跟踪...")

        try:
            now = datetime.now()
            if now.hour < 15:
                track_date = self._get_previous_trading_day()
                self.logger.info(f"盘前运行，使用前一交易日: {track_date}")
            else:
                track_date = date.today()
                self.logger.info(f"盘后运行，使用今日: {track_date}")

            self._update_tracking(track_date)
            self.logger.info("跟踪更新成功")
            return True

        except Exception as e:
            self.logger.error(f"跟踪更新失败: {e}")
            return False


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description='更新推荐股票跟踪数据')
    parser.add_argument('--date', help='跟踪日期 (YYYY-MM-DD)，默认自动判断')

    args = parser.parse_args()

    updater = TrackingUpdater()

    if args.date:
        track_date = date.fromisoformat(args.date)
        updater.logger.info(f"📅 手动指定日期: {track_date}")
        updater._update_tracking(track_date)
    else:
        result = updater.run()
        sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
