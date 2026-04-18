#!/usr/bin/env python3
"""
晨间报告推送 - 使用 BaseReporter 重构
【08:45执行】
"""
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.base_reporter import BaseReporter
from core.paths import ReportPaths
from services.notify_service.templates import get_template
from services.report_db_service import ReportDBService


class MorningReporter(BaseReporter):
    """晨间报告推送器"""

    @property
    def report_type(self) -> str:
        return "morning_report"

    @property
    def required_data_sources(self) -> List[str]:
        return ['foreign_data', 'market_data', 'picks_data']

    @property
    def optional_data_sources(self) -> List[str]:
        return ['strategy_data', 'fund_behavior_data']

    def load_data(self) -> Dict[str, Any]:
        """加载晨间报告所需数据"""
        return {
            'foreign_data': self._load_json_file(ReportPaths.foreign_index()),
            'market_data': self._load_json_file(
                ReportPaths.market_analysis(fallback_to_yesterday=True)
            ),
            'picks_data': self._load_json_file(
                ReportPaths.daily_picks(fallback_to_yesterday=True)
            ),
            'strategy_data': self._load_json_file(ReportPaths.strategy_result()),
            'fund_behavior_data': self._load_json_file(ReportPaths.fund_behavior_result())
        }

    # 保持向后兼容的方法名
    def load_foreign_data(self) -> dict:
        """加载外盘数据（兼容旧接口）"""
        return self._load_json_file(ReportPaths.foreign_index())

    def load_market_analysis(self) -> dict:
        """加载大盘分析数据（兼容旧接口）"""
        file_path = ReportPaths.market_analysis(fallback_to_yesterday=True)
        return self._load_json_file(file_path) if file_path else None

    def load_daily_picks(self) -> dict:
        """加载每日选股（兼容旧接口）"""
        file_path = ReportPaths.daily_picks(fallback_to_yesterday=True)
        return self._load_json_file(file_path) if file_path else None

    def load_strategy_result(self) -> dict:
        """加载策略结果（兼容旧接口）"""
        return self._load_json_file(ReportPaths.strategy_result())

    def load_fund_behavior_result(self) -> dict:
        """加载资金行为结果（兼容旧接口）"""
        return self._load_json_file(ReportPaths.fund_behavior_result())

    def generate(self, data: Dict[str, Any]) -> str:
        """生成晨间报告"""
        template = get_template('morning_report')
        return template.generate(
            market_data=data.get('market_data'),
            picks_data=data.get('picks_data'),
            foreign_data=data.get('foreign_data'),
            strategy_data=data.get('strategy_data'),
            fb_result=data.get('fund_behavior_data')
        )

    # 保持向后兼容的方法名
    def generate_report(self) -> str:
        """生成报告（兼容旧接口）"""
        data = self.load_data()
        return self.generate(data)

    def _send(self, content: str) -> bool:
        """发送报告并保存到数据库"""
        # 调用基类发送
        success = super()._send(content)

        if success:
            # 保存到数据库
            try:
                db_service = ReportDBService()
                today = datetime.now().strftime('%Y-%m-%d')
                db_service.save_report(
                    report_type='morning',
                    report_date=today,
                    content=content
                )
                self.logger.info("报告已保存到数据库")
            except Exception as e:
                self.logger.warning(f"保存到数据库失败: {e}")

        return success

    def run(self) -> bool:
        """执行晨间报告推送（兼容旧接口）"""
        return super().run()


def main():
    """主函数"""
    reporter = MorningReporter()
    success = reporter.run()

    result = reporter.get_last_result()
    if result:
        print(f"\n执行结果: {result.status.value}")
        if result.error_message:
            print(f"错误: {result.error_message}")
        if result.validation_issues:
            print(f"验证问题: {len(result.validation_issues)} 个")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
