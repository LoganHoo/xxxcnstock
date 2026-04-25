#!/usr/bin/env python3
"""
选股报告服务 - SQLite版本
- 记录评分数据最后更新时间
- 保存选股结果到本地SQLite
- 发送邮件报告
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import polars as pl
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer, Date, Text, UniqueConstraint, Index
from sqlalchemy.orm import declarative_base, sessionmaker
from core.logger import setup_logger
from services.email_sender import EmailSender

logger = setup_logger("selection_report")

Base = declarative_base()


class StockSelectionResult(Base):
    """选股结果表"""
    __tablename__ = 'stock_selection_results'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(Date, nullable=False, comment='报告日期')
    code = Column(String(20), nullable=False, comment='股票代码')
    name = Column(String(100), comment='股票名称')
    rank = Column(Integer, comment='排名')
    score = Column(Float, comment='综合评分')
    price = Column(Float, comment='收盘价')
    volume = Column(Float, comment='成交量')
    change_pct = Column(Float, comment='涨跌幅%')
    trade_date = Column(String(10), comment='数据日期')
    created_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        UniqueConstraint('report_date', 'code', name='uix_selection_date_code'),
        Index('idx_report_date', 'report_date'),
    )


class DataUpdateLog(Base):
    """数据更新日志表"""
    __tablename__ = 'data_update_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    data_type = Column(String(50), nullable=False, comment='数据类型')
    data_date = Column(String(10), comment='数据日期')
    last_update_time = Column(DateTime, default=datetime.now, comment='最后更新时间')
    record_count = Column(Integer, comment='记录数')
    details = Column(Text, comment='详情JSON')
    
    __table_args__ = (
        Index('idx_data_type_date', 'data_type', 'data_date'),
    )


class SelectionReportService:
    """选股报告服务 - SQLite版本"""
    
    def __init__(self, db_path: str = None):
        """
        初始化服务
        
        Args:
            db_path: SQLite数据库路径，默认使用data/selection_report.db
        """
        if db_path is None:
            data_dir = Path(__file__).parent.parent / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            db_path = data_dir / 'selection_report.db'
        
        self.db_path = str(db_path)
        self.db_url = f"sqlite:///{self.db_path}"
        
        try:
            self.engine = create_engine(self.db_url, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            
            # 创建表
            Base.metadata.create_all(self.engine)
            logger.info(f"SQLite数据库初始化成功: {self.db_path}")
        except Exception as e:
            logger.error(f"SQLite数据库初始化失败: {e}")
            raise
        
        self.email_sender = EmailSender()
    
    def record_data_update(self, data_type: str, data_date: str, record_count: int, details: Dict = None):
        """记录数据更新时间"""
        session = self.Session()
        try:
            log = DataUpdateLog(
                data_type=data_type,
                data_date=data_date,
                last_update_time=datetime.now(),
                record_count=record_count,
                details=json.dumps(details) if details else None
            )
            session.add(log)
            session.commit()
            logger.info(f"记录数据更新: {data_type} - {data_date} - {record_count}条")
        except Exception as e:
            session.rollback()
            logger.error(f"记录数据更新失败: {e}")
        finally:
            session.close()
    
    def get_last_update_time(self, data_type: str, data_date: str = None) -> Optional[datetime]:
        """获取最后更新时间"""
        session = self.Session()
        try:
            query = session.query(DataUpdateLog).filter(DataUpdateLog.data_type == data_type)
            if data_date:
                query = query.filter(DataUpdateLog.data_date == data_date)
            log = query.order_by(DataUpdateLog.last_update_time.desc()).first()
            return log.last_update_time if log else None
        finally:
            session.close()
    
    def save_selection_results(self, report_date: str, results: List[Dict]):
        """保存选股结果到SQLite"""
        from datetime import date as date_type
        
        session = self.Session()
        try:
            # 转换report_date为date对象
            if isinstance(report_date, str):
                report_date_obj = date_type.fromisoformat(report_date)
            else:
                report_date_obj = report_date
            
            # 先删除该日期的旧数据
            session.query(StockSelectionResult).filter(
                StockSelectionResult.report_date == report_date_obj
            ).delete()
            
            # 插入新数据
            for item in results:
                result = StockSelectionResult(
                    report_date=report_date_obj,
                    code=item['code'],
                    name=item.get('name', ''),
                    rank=item.get('rank', 0),
                    score=item.get('enhanced_score', 0),
                    price=item.get('price', 0),
                    volume=item.get('volume', 0),
                    change_pct=item.get('change_pct', 0),
                    trade_date=item.get('trade_date', report_date)
                )
                session.add(result)
            
            session.commit()
            logger.info(f"保存选股结果: {report_date} - {len(results)}条")
        except Exception as e:
            session.rollback()
            logger.error(f"保存选股结果失败: {e}")
            raise
        finally:
            session.close()
    
    def get_selection_results(self, report_date: str) -> List[Dict]:
        """获取选股结果"""
        session = self.Session()
        try:
            results = session.query(StockSelectionResult).filter(
                StockSelectionResult.report_date == report_date
            ).order_by(StockSelectionResult.rank).all()
            
            return [
                {
                    'code': r.code,
                    'name': r.name,
                    'rank': r.rank,
                    'score': r.score,
                    'price': r.price,
                    'volume': r.volume,
                    'change_pct': r.change_pct,
                    'trade_date': r.trade_date,
                }
                for r in results
            ]
        finally:
            session.close()
    
    def send_email_report(self, report_date: str, results: List[Dict], recipient: str = "287363@qq.com"):
        """发送邮件报告"""
        try:
            # 生成HTML报告
            html_content = self._generate_html_report(report_date, results)
            
            # 生成文本报告
            text_content = self._generate_text_report(report_date, results)
            
            # 发送邮件
            subject = f"选股报告 - {report_date} - Top{len(results)}"
            
            success = self.email_sender.send(
                to_addrs=[recipient],
                subject=subject,
                content=text_content,
                html_content=html_content
            )
            
            if success:
                logger.info(f"邮件报告已发送: {recipient}")
            else:
                logger.error(f"邮件发送失败")
            
            return success
        except Exception as e:
            logger.error(f"发送邮件报告异常: {e}")
            return False
    
    def _generate_html_report(self, report_date: str, results: List[Dict]) -> str:
        """生成HTML格式报告"""
        rows = ""
        for i, item in enumerate(results, 1):
            rows += f"""
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;">{i}</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{item['code']}</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{item.get('name', '')}</td>
                    <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{item.get('enhanced_score', 0):.1f}</td>
                    <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{item.get('price', 0):.2f}</td>
                    <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{item.get('volume', 0):,.0f}</td>
                </tr>
            """
        
        # 获取最后更新时间
        last_update = self.get_last_update_time("enhanced_scores")
        last_update_str = last_update.strftime('%Y-%m-%d %H:%M:%S') if last_update else '未知'
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2>选股报告 - {report_date}</h2>
            <p>选取数量: 前{len(results)}名</p>
            <p>评分数据更新时间: {last_update_str}</p>
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
                        <th style="padding: 8px; border: 1px solid #ddd;">排名</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">代码</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">名称</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">评分</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">收盘价</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">成交量</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
            <p style="margin-top: 20px; color: #666; font-size: 12px;">
                生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </body>
        </html>
        """
    
    def _generate_text_report(self, report_date: str, results: List[Dict]) -> str:
        """生成文本格式报告"""
        # 获取最后更新时间
        last_update = self.get_last_update_time("enhanced_scores")
        last_update_str = last_update.strftime('%Y-%m-%d %H:%M:%S') if last_update else '未知'
        
        lines = [
            f"选股报告 - {report_date}",
            "=" * 60,
            f"选取数量: 前{len(results)}名",
            f"评分数据更新时间: {last_update_str}",
            "",
            "排名 | 代码   | 名称     | 评分 | 收盘价 | 成交量",
            "-" * 60
        ]
        
        for i, item in enumerate(results, 1):
            lines.append(
                f"{i:4d} | {item['code']} | {item.get('name', ''):8s} | "
                f"{item.get('enhanced_score', 0):4.1f} | {item.get('price', 0):6.2f} | {item.get('volume', 0):10,.0f}"
            )
        
        lines.extend([
            "",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(lines)


if __name__ == "__main__":
    # 测试
    service = SelectionReportService()
    
    # 记录数据更新
    service.record_data_update("enhanced_scores", "2026-04-22", 4447)
    
    # 获取最后更新时间
    last_update = service.get_last_update_time("enhanced_scores")
    print(f"最后更新时间: {last_update}")
    
    # 保存测试数据
    test_results = [
        {'code': '000001', 'name': '平安银行', 'rank': 1, 'enhanced_score': 95.5, 'price': 12.34, 'volume': 1000000, 'change_pct': 2.5, 'trade_date': '2026-04-22'},
        {'code': '000002', 'name': '万科A', 'rank': 2, 'enhanced_score': 92.0, 'price': 15.67, 'volume': 2000000, 'change_pct': 1.8, 'trade_date': '2026-04-22'},
    ]
    service.save_selection_results("2026-04-23", test_results)
    
    # 查询数据
    results = service.get_selection_results("2026-04-23")
    print(f"查询结果: {len(results)}条")
    for r in results:
        print(f"  {r}")
