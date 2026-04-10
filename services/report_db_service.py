#!/usr/bin/env python3
"""报告存储服务 - 将报告保存到MySQL"""
import os
import sys
from pathlib import Path
from datetime import datetime, date
from typing import Optional, List, Dict
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text, Column, String, DateTime, Text, Float, Integer, Date
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT

Base = declarative_base()


class DailyReport(Base):
    __tablename__ = 'xcn_daily_report'

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_type = Column(String(20), nullable=False, comment='报告类型: morning/review/fund_behavior')
    report_date = Column(Date, nullable=False, comment='报告日期')
    subject = Column(String(200), comment='邮件主题')
    text_content = Column(LONGTEXT, comment='文本内容')
    json_data = Column(LONGTEXT, comment='JSON数据')
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<DailyReport(type={self.report_type}, date={self.report_date})>"


from services.db_pool import get_db_pool, DatabasePoolManager


class ReportDBService:
    """报告数据库服务 - 使用连接池"""

    def __init__(self, pool_manager: DatabasePoolManager = None):
        self.pool_manager = pool_manager or get_db_pool()
        
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '3306')
        db_user = os.getenv('DB_USER', 'root')
        db_password = os.getenv('DB_PASSWORD', '')
        db_name = os.getenv('DB_NAME', 'quantdb')

        conn_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4'
        
        pool_info = self.pool_manager.get_pool('report_db', conn_str)
        self.engine = pool_info['engine']
        self.Session = pool_info['Session']

    def init_tables(self):
        """初始化表"""
        Base.metadata.create_all(self.engine)

    def save_report(
        self,
        report_type: str,
        report_date: str,
        subject: str,
        text_content: str,
        json_data: Optional[Dict] = None
    ) -> bool:
        """保存报告"""
        try:
            with self.Session() as session:
                report = DailyReport(
                    report_type=report_type,
                    report_date=datetime.strptime(report_date, '%Y-%m-%d').date(),
                    subject=subject,
                    text_content=text_content,
                    json_data=json.dumps(json_data, ensure_ascii=False) if json_data else None
                )
                session.add(report)
                session.commit()
                print(f"✅ 报告已保存: {report_type} - {report_date}")
                return True
        except Exception as e:
            print(f"❌ 保存报告失败: {e}")
            return False

    def get_report(self, report_type: str, report_date: str) -> Optional[Dict]:
        """获取报告"""
        try:
            with self.Session() as session:
                report = session.query(DailyReport).filter(
                    DailyReport.report_type == report_type,
                    DailyReport.report_date == datetime.strptime(report_date, '%Y-%m-%d').date()
                ).first()

                if report:
                    return {
                        'subject': report.subject,
                        'text_content': report.text_content,
                        'json_data': json.loads(report.json_data) if report.json_data else None,
                        'created_at': report.created_at
                    }
                return None
        except Exception as e:
            print(f"❌ 获取报告失败: {e}")
            return None

    def get_recent_reports(self, report_type: str, limit: int = 7) -> List[Dict]:
        """获取最近报告"""
        try:
            with self.Session() as session:
                reports = session.query(DailyReport).filter(
                    DailyReport.report_type == report_type
                ).order_by(DailyReport.report_date.desc()).limit(limit).all()

                return [{
                    'report_date': r.report_date,
                    'subject': r.subject,
                    'created_at': r.created_at
                } for r in reports]
        except Exception as e:
            print(f"❌ 获取最近报告失败: {e}")
            return []

    def save_txt_file(self, report_type: str, report_date: str, text_content: str) -> str:
        """保存TXT文件"""
        report_dir = Path('data/reports')
        report_dir.mkdir(parents=True, exist_ok=True)

        txt_file = report_dir / f"{report_type}_{report_date}.txt"
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(text_content)

        return str(txt_file)


def main():
    service = ReportDBService()
    service.init_tables()

    print("✅ 报告数据库服务初始化完成")
    print(f"   连接: {service.engine.url}")


if __name__ == '__main__':
    main()
