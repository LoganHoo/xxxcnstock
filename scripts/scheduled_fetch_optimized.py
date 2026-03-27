"""
优化版定时任务脚本
功能：
1. 判断是否为交易日和收盘后
2. 检查断点续传
3. 执行数据采集
4. 数据验证
5. 完整性检查和报告
6. 发送邮件通知
"""
import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime
import logging
import subprocess

PROJECT_ROOT = Path(__file__).parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'scheduled_fetch_optimized.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from trading_calendar import TradingCalendar, check_market_status
from data_validator import DataValidator
from email_sender import EmailSender


class ScheduledTaskManager:
    """定时任务管理器"""
    
    def __init__(self):
        self.calendar = TradingCalendar()
        self.validator = DataValidator(str(PROJECT_ROOT / "data" / "kline"))
        self.progress_file = PROJECT_ROOT / "data" / "kline" / ".fetch_progress.json"
        self.report_file = LOG_DIR / "daily_report.json"
        
        self.smtp_config = {
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.qq.com'),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'sender_email': os.getenv('SENDER_EMAIL', ''),
            'sender_password': os.getenv('SENDER_PASSWORD', ''),
            'use_tls': os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
        }
        
        self.notification_emails = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com').split(',')
        
        if self.smtp_config['sender_email'] and self.smtp_config['sender_password']:
            self.email_sender = EmailSender(**self.smtp_config)
        else:
            self.email_sender = None
            logger.warning("邮件发送功能未配置，请设置 SENDER_EMAIL 和 SENDER_PASSWORD 环境变量")
    
    def check_preconditions(self) -> tuple:
        """
        检查执行前置条件
        
        Returns:
            tuple: (can_run: bool, reason: str)
        """
        logger.info("="*70)
        logger.info("检查执行前置条件")
        logger.info("="*70)
        
        market_status = check_market_status()
        
        logger.info(f"当前时间: {market_status['current_time']} {market_status['weekday']}")
        logger.info(f"是否交易日: {'是' if market_status['is_trading_day'] else '否'}")
        logger.info(f"是否收盘后: {'是' if market_status['is_after_market_close'] else '否'}")
        logger.info(f"上一交易日: {market_status['last_trading_day']}")
        
        if not market_status['should_run_task']:
            logger.warning(f"❌ 不满足执行条件: {market_status['reason']}")
            return False, market_status['reason']
        
        logger.info("✅ 满足执行条件")
        return True, market_status['reason']
    
    def check_resume_point(self) -> Dict:
        """
        检查断点续传
        
        Returns:
            Dict: 断点信息
        """
        logger.info("")
        logger.info("="*70)
        logger.info("检查断点续传")
        logger.info("="*70)
        
        resume_info = {
            'has_progress': False,
            'processed_count': 0,
            'last_update': None,
            'should_resume': False
        }
        
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                
                resume_info['has_progress'] = True
                resume_info['processed_count'] = len(progress.get('processed', []))
                resume_info['last_update'] = progress.get('timestamp')
                
                last_update_time = datetime.fromisoformat(progress['timestamp'])
                hours_elapsed = (datetime.now() - last_update_time).total_seconds() / 3600
                
                if hours_elapsed < 24:
                    resume_info['should_resume'] = True
                    logger.info(f"✅ 发现断点文件")
                    logger.info(f"  已处理: {resume_info['processed_count']} 只股票")
                    logger.info(f"  最后更新: {progress['timestamp']}")
                    logger.info(f"  将从断点继续")
                else:
                    logger.info(f"⚠️  断点文件已过期（{hours_elapsed:.1f}小时前）")
                    logger.info(f"  将重新开始采集")
                    os.remove(self.progress_file)
            except Exception as e:
                logger.error(f"❌ 读取断点文件失败: {e}")
        else:
            logger.info("ℹ️  无断点文件，将从头开始采集")
        
        return resume_info
    
    def run_data_fetch(self) -> bool:
        """
        执行数据采集
        
        Returns:
            bool: 是否成功
        """
        logger.info("")
        logger.info("="*70)
        logger.info("开始执行数据采集")
        logger.info("="*70)
        
        try:
            script_path = PROJECT_ROOT / "scripts" / "fetch_history_klines_parquet.py"
            
            if not script_path.exists():
                logger.error(f"❌ 脚本不存在: {script_path}")
                return False
            
            logger.info(f"执行脚本: {script_path}")
            logger.info(f"工作目录: {PROJECT_ROOT}")
            
            result = subprocess.run(
                [sys.executable, str(script_path), "--mode", "full", "--days", "1095", "--rate-limit", "5.0"],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=3600
            )
            
            if result.returncode == 0:
                logger.info("✅ 数据采集成功完成")
                logger.info(f"输出:\n{result.stdout}")
                return True
            else:
                logger.error(f"❌ 数据采集失败，返回码: {result.returncode}")
                logger.error(f"错误输出:\n{result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ 数据采集超时（超过1小时）")
            return False
        except Exception as e:
            logger.error(f"❌ 执行数据采集时发生错误: {e}")
            return False
    
    def validate_data(self) -> Dict:
        """
        验证采集的数据
        
        Returns:
            Dict: 验证结果
        """
        logger.info("")
        logger.info("="*70)
        logger.info("开始数据验证")
        logger.info("="*70)
        
        stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
        
        if not stock_list_file.exists():
            logger.error(f"❌ 股票列表文件不存在: {stock_list_file}")
            return {'valid': False, 'error': 'stock_list_not_found'}
        
        try:
            import pandas as pd
            df = pd.read_parquet(stock_list_file)
            stock_list = df['code'].tolist()
            
            logger.info(f"验证 {len(stock_list)} 只股票数据...")
            
            validation_results = self.validator.validate_all_stocks(stock_list)
            
            logger.info(f"✅ 数据验证完成")
            logger.info(f"  有效: {validation_results['valid']}")
            logger.info(f"  无效: {validation_results['invalid']}")
            logger.info(f"  警告: {validation_results['warnings']}")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"❌ 数据验证失败: {e}")
            return {'valid': False, 'error': str(e)}
    
    def check_completeness(self) -> Dict:
        """
        检查数据完整性
        
        Returns:
            Dict: 完整性检查结果
        """
        logger.info("")
        logger.info("="*70)
        logger.info("检查数据完整性")
        logger.info("="*70)
        
        last_trading_day = self.calendar.get_last_trading_day()
        
        logger.info(f"检查日期: {last_trading_day}")
        
        completeness_results = self.validator.check_data_completeness(last_trading_day)
        
        logger.info(f"✅ 完整性检查完成")
        logger.info(f"  总股票数: {completeness_results['total_stocks']}")
        logger.info(f"  包含数据: {completeness_results['stocks_with_data']}")
        logger.info(f"  缺失数据: {completeness_results['stocks_missing_data']}")
        
        if completeness_results['total_stocks'] > 0:
            rate = completeness_results['stocks_with_data'] / completeness_results['total_stocks'] * 100
            logger.info(f"  完整率: {rate:.2f}%")
        
        return completeness_results
    
    def generate_daily_report(self, validation_results: Dict, completeness_results: Dict):
        """
        生成每日报告
        
        Args:
            validation_results: 数据验证结果
            completeness_results: 完整性检查结果
        """
        logger.info("")
        logger.info("="*70)
        logger.info("生成每日报告")
        logger.info("="*70)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'market_status': check_market_status(),
            'validation': {
                'total': validation_results.get('total', 0),
                'valid': validation_results.get('valid', 0),
                'invalid': validation_results.get('invalid', 0),
                'warnings': validation_results.get('warnings', 0)
            },
            'completeness': {
                'expected_date': completeness_results.get('expected_date'),
                'total_stocks': completeness_results.get('total_stocks', 0),
                'stocks_with_data': completeness_results.get('stocks_with_data', 0),
                'stocks_missing_data': completeness_results.get('stocks_missing_data', 0),
                'missing_stocks': completeness_results.get('missing_stocks', [])[:20]
            }
        }
        
        with open(self.report_file, 'w') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ 报告已保存: {self.report_file}")
        
        report_text = self.validator.generate_validation_report(validation_results, completeness_results)
        logger.info("\n" + report_text)
        
        self.send_notification_email(report)
    
    def send_notification_email(self, report: Dict):
        """
        发送通知邮件
        
        Args:
            report: 报告数据
        """
        if not self.email_sender:
            logger.warning("⚠️  邮件发送功能未配置，跳过邮件通知")
            return
        
        logger.info("")
        logger.info("="*70)
        logger.info("发送通知邮件")
        logger.info("="*70)
        
        try:
            success = self.email_sender.send_daily_report(
                to_emails=self.notification_emails,
                report_data=report,
                report_file=str(self.report_file)
            )
            
            if success:
                logger.info(f"✅ 邮件发送成功: {', '.join(self.notification_emails)}")
            else:
                logger.error("❌ 邮件发送失败")
                
        except Exception as e:
            logger.error(f"❌ 发送邮件时发生错误: {e}")
    
    def run(self):
        """执行完整的定时任务流程"""
        start_time = time.time()
        
        logger.info("")
        logger.info("🚀 定时任务启动")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        can_run, reason = self.check_preconditions()
        
        if not can_run:
            logger.info(f"⏭️  跳过执行: {reason}")
            return 0
        
        resume_info = self.check_resume_point()
        
        fetch_success = self.run_data_fetch()
        
        if not fetch_success:
            logger.error("❌ 数据采集失败，终止流程")
            return 1
        
        validation_results = self.validate_data()
        
        completeness_results = self.check_completeness()
        
        self.generate_daily_report(validation_results, completeness_results)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        logger.info("")
        logger.info("="*70)
        logger.info("✅ 定时任务执行完成")
        logger.info(f"总耗时: {elapsed_time:.2f} 秒")
        logger.info("="*70)
        
        return 0


def main():
    """主函数"""
    manager = ScheduledTaskManager()
    return manager.run()


if __name__ == '__main__':
    sys.exit(main())
