#!/usr/bin/env python3
"""
数据完整性检查脚本
功能：
1. 检查核心数据表的记录数
2. 验证今日数据是否已生成
3. 检查数据新鲜度
4. 发送异常告警

使用方法:
    python scripts/data_integrity_check.py
    python scripts/data_integrity_check.py --date 2026-04-15  # 检查指定日期
    python scripts/data_integrity_check.py --verbose           # 详细输出
"""
import sys
import os
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

os.environ['PYTHONUNBUFFERED'] = '1'

from core.logger import setup_logger

logger = setup_logger(
    name="data_integrity",
    level="INFO",
    log_file="system/data_integrity.log",
    rotation="1 day",
    retention="30 days"
)


@dataclass
class CheckResult:
    """检查结果数据类"""
    table: str
    description: str
    status: str  # 'pass', 'fail', 'warning'
    count: int
    min_required: int
    message: str
    details: Optional[Dict] = None


class DataIntegrityChecker:
    """数据完整性检查器"""
    
    def __init__(self, check_date: Optional[str] = None):
        self.check_date = check_date or datetime.now().strftime('%Y-%m-%d')
        self.results: List[CheckResult] = []
        self.db_config = self._get_db_config()
        
    def _get_db_config(self) -> Dict:
        """获取数据库配置"""
        return {
            'host': os.getenv('DB_HOST', '49.233.10.199'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'user': os.getenv('DB_USER', 'nextai'),
            'password': os.getenv('DB_PASSWORD', '100200'),
            'database': os.getenv('DB_NAME', 'xcn_db'),
            'charset': 'utf8mb4'
        }
    
    def _get_connection(self):
        """获取数据库连接"""
        import pymysql
        return pymysql.connect(**self.db_config)
    
    def check_table_count(self, table: str, description: str, 
                          min_required: int = 1,
                          date_column: str = 'created_at') -> CheckResult:
        """检查表记录数"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 查询今日记录数
            sql = f"""
                SELECT COUNT(*) FROM {table} 
                WHERE DATE({date_column}) = %s
            """
            cursor.execute(sql, (self.check_date,))
            count = cursor.fetchone()[0]
            
            conn.close()
            
            # 判断状态
            if count >= min_required:
                status = 'pass'
                message = f"✅ {description}: {count}条 (要求≥{min_required})"
            elif count > 0:
                status = 'warning'
                message = f"⚠️ {description}: {count}条 (要求≥{min_required})"
            else:
                status = 'fail'
                message = f"❌ {description}: 无数据 (要求≥{min_required})"
            
            return CheckResult(
                table=table,
                description=description,
                status=status,
                count=count,
                min_required=min_required,
                message=message
            )
            
        except Exception as e:
            logger.error(f"检查表 {table} 失败: {e}")
            return CheckResult(
                table=table,
                description=description,
                status='fail',
                count=0,
                min_required=min_required,
                message=f"❌ {description}: 检查失败 - {str(e)[:50]}"
            )
    
    def check_cctv_news(self) -> CheckResult:
        """检查新闻联播数据
        
        注意：新闻联播数据通常在节目结束后1-2小时才会发布到第三方网站
        因此允许数据延迟到晚上21:00之前
        """
        from datetime import datetime, time
        
        now = datetime.now()
        current_time = now.time()
        
        # 如果当前时间早于21:00，检查昨天的数据
        # 如果当前时间晚于21:00，检查今天的数据
        if current_time < time(21, 0):
            # 21:00前，检查昨天是否有数据
            check_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            min_required = 1
        else:
            # 21:00后，检查今天是否有数据
            check_date = self.check_date
            min_required = 1
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT COUNT(*) FROM cctv_news_broadcast WHERE DATE(news_date) = %s",
                (check_date,)
            )
            count = cursor.fetchone()[0]
            conn.close()
            
            if count >= min_required:
                status = 'pass'
                message = f"✅ 新闻联播数据: {count}条 ({check_date})"
            else:
                # 如果21:00前没有昨天数据，或者21:00后没有今天数据，给警告而非失败
                if current_time < time(21, 0):
                    status = 'warning'
                    message = f"⚠️ 新闻联播数据: {check_date}无数据 (21:00前允许延迟)"
                else:
                    status = 'warning'
                    message = f"⚠️ 新闻联播数据: {check_date}无数据 (可能尚未发布)"
            
            return CheckResult(
                table='cctv_news_broadcast',
                description='新闻联播数据',
                status=status,
                count=count,
                min_required=min_required,
                message=message
            )
            
        except Exception as e:
            logger.error(f"检查新闻联播数据失败: {e}")
            return CheckResult(
                table='cctv_news_broadcast',
                description='新闻联播数据',
                status='warning',
                count=0,
                min_required=1,
                message=f"⚠️ 新闻联播数据: 检查失败 - {str(e)[:50]}"
            )
    
    def check_stock_selections(self) -> CheckResult:
        """检查选股推荐数据"""
        return self.check_table_count(
            table='stock_selections',
            description='选股推荐',
            min_required=5,
            date_column='report_date'
        )

    def check_daily_reports(self) -> CheckResult:
        """检查日报数据"""
        return self.check_table_count(
            table='xcn_daily_report',
            description='日报',
            min_required=1,
            date_column='report_date'
        )
    
    def check_kline_data(self) -> CheckResult:
        """检查K线数据文件"""
        try:
            kline_dir = project_root / 'data' / 'kline'
            if not kline_dir.exists():
                return CheckResult(
                    table='kline_files',
                    description='K线数据文件',
                    status='fail',
                    count=0,
                    min_required=100,
                    message='❌ K线目录不存在'
                )
            
            # 统计parquet文件数
            parquet_files = list(kline_dir.glob('*.parquet'))
            count = len(parquet_files)
            
            # 检查是否有今日更新的文件
            today = datetime.now().strftime('%Y-%m-%d')
            today_files = [
                f for f in parquet_files 
                if datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d') == today
            ]
            
            if count >= 4000 and len(today_files) > 100:
                status = 'pass'
                message = f'✅ K线数据: {count}个文件，今日更新{len(today_files)}个'
            elif count >= 4000:
                status = 'warning'
                message = f'⚠️ K线数据: {count}个文件，但今日仅更新{len(today_files)}个'
            else:
                status = 'fail'
                message = f'❌ K线数据: 仅{count}个文件 (要求≥4000)'
            
            return CheckResult(
                table='kline_files',
                description='K线数据文件',
                status=status,
                count=count,
                min_required=4000,
                message=message,
                details={'today_updated': len(today_files)}
            )
            
        except Exception as e:
            return CheckResult(
                table='kline_files',
                description='K线数据文件',
                status='fail',
                count=0,
                min_required=4000,
                message=f'❌ 检查失败: {str(e)[:50]}'
            )
    
    def check_task_states(self) -> CheckResult:
        """检查任务执行状态"""
        try:
            state_file = project_root / 'logs' / 'task_states.json'
            if not state_file.exists():
                return CheckResult(
                    table='task_states',
                    description='任务状态',
                    status='fail',
                    count=0,
                    min_required=1,
                    message='❌ 任务状态文件不存在'
                )
            
            import json
            with open(state_file, 'r', encoding='utf-8') as f:
                states = json.load(f)
            
            today = datetime.now().strftime('%Y%m%d')
            today_tasks = {k: v for k, v in states.items() if k.endswith(today)}
            
            failed_tasks = [
                k for k, v in today_tasks.items() 
                if v.get('status') == 'failed'
            ]
            
            if failed_tasks:
                status = 'fail'
                message = f"❌ 今日有{len(failed_tasks)}个任务失败"
            elif len(today_tasks) >= 3:
                status = 'pass'
                message = f"✅ 今日{len(today_tasks)}个任务已执行"
            else:
                status = 'warning'
                message = f"⚠️ 今日仅{len(today_tasks)}个任务记录"
            
            return CheckResult(
                table='task_states',
                description='任务执行状态',
                status=status,
                count=len(today_tasks),
                min_required=3,
                message=message,
                details={'failed_tasks': failed_tasks}
            )
            
        except Exception as e:
            return CheckResult(
                table='task_states',
                description='任务执行状态',
                status='fail',
                count=0,
                min_required=3,
                message=f'❌ 检查失败: {str(e)[:50]}'
            )
    
    def run_all_checks(self) -> List[CheckResult]:
        """运行所有检查"""
        logger.info(f"开始数据完整性检查: {self.check_date}")
        
        checks = [
            self.check_cctv_news(),
            self.check_stock_selections(),
            self.check_daily_reports(),
            self.check_kline_data(),
            self.check_task_states(),
        ]
        
        self.results = checks
        return checks
    
    def generate_report(self, verbose: bool = False) -> str:
        """生成检查报告"""
        lines = []
        lines.append("=" * 60)
        lines.append(f"数据完整性检查报告 - {self.check_date}")
        lines.append("=" * 60)
        
        fail_count = sum(1 for r in self.results if r.status == 'fail')
        warning_count = sum(1 for r in self.results if r.status == 'warning')
        pass_count = sum(1 for r in self.results if r.status == 'pass')
        
        lines.append(f"\n总计: {len(self.results)}项检查")
        lines.append(f"  ✅ 通过: {pass_count}")
        lines.append(f"  ⚠️ 警告: {warning_count}")
        lines.append(f"  ❌ 失败: {fail_count}")
        lines.append("")
        
        for result in self.results:
            lines.append(result.message)
            if verbose and result.details:
                for key, value in result.details.items():
                    lines.append(f"    {key}: {value}")
        
        lines.append("=" * 60)
        
        return "\n".join(lines)
    
    def send_alert(self):
        """发送告警邮件"""
        fail_count = sum(1 for r in self.results if r.status == 'fail')
        
        if fail_count == 0:
            logger.info("所有检查通过，无需发送告警")
            return
        
        try:
            from services.email_sender import send_report_email

            subject = f"❌ 数据完整性检查异常 - {self.check_date}"
            content = self.generate_report(verbose=True)

            send_report_email(subject=subject, content=content)

            logger.info("告警邮件已发送")
            
        except Exception as e:
            logger.error(f"发送告警邮件失败: {e}")


def main():
    parser = argparse.ArgumentParser(description='数据完整性检查工具')
    parser.add_argument('--date', type=str, help='检查指定日期 (YYYY-MM-DD)')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    parser.add_argument('--no-alert', action='store_true', help='不发送告警邮件')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("数据完整性检查启动")
    logger.info("=" * 60)
    
    checker = DataIntegrityChecker(check_date=args.date)
    checker.run_all_checks()
    
    report = checker.generate_report(verbose=args.verbose)
    print(report)
    
    # 记录到日志
    logger.info("检查完成")
    for result in checker.results:
        logger.info(result.message)
    
    # 发送告警
    if not args.no_alert:
        checker.send_alert()
    
    # 如果有失败项，返回非0退出码
    fail_count = sum(1 for r in checker.results if r.status == 'fail')
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
