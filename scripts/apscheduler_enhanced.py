#!/usr/bin/env python3
"""
APScheduler 增强版调度器 - 独立容器运行
功能：
1. 严格按 cron_tasks.yaml 执行任务
2. 任务状态持久化（状态文件）
3. 断点续传（重启后继续执行未完成任务）
4. 自动重试（失败任务自动重试）
5. 邮件通知（每项任务执行结果）
"""
import sys
import os
import json
import time
import threading
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.logger import get_logger

os.environ['PYTHONUNBUFFERED'] = '1'
logger = get_logger(__name__)

HEARTBEAT_FILE = '/app/logs/scheduler_heartbeat'
STATE_FILE = '/app/logs/task_states.json'
HEARTBEAT_INTERVAL = 60
RETRY_DELAY = 300
MAX_RETRIES = 3


def write_heartbeat():
    try:
        with open(HEARTBEAT_FILE, 'w') as f:
            f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        logger.error(f"心跳写入失败: {e}")


def heartbeat_loop():
    write_heartbeat()
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        write_heartbeat()


class TaskStateManager:
    """任务状态管理器"""

    def __init__(self, state_file=STATE_FILE):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.states = self._load_states()

    def _load_states(self) -> dict:
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_states(self):
        with open(self.state_file, 'w') as f:
            json.dump(self.states, f, indent=2, ensure_ascii=False)

    def get_state(self, job_id: str, date: str = None) -> dict:
        key = f"{job_id}_{date or datetime.now().strftime('%Y%m%d')}"
        return self.states.get(key, {'status': 'pending', 'retries': 0, 'last_run': None, 'result': None})

    def set_state(self, job_id: str, status: str, result: str = None, date: str = None):
        key = f"{job_id}_{date or datetime.now().strftime('%Y%m%d')}"
        self.states[key] = {
            'status': status,
            'retries': self.states.get(key, {}).get('retries', 0),
            'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'result': result
        }
        self._save_states()
        logger.info(f"📝 任务状态已保存: {job_id} -> {status}")

    def increment_retry(self, job_id: str, date: str = None) -> int:
        key = f"{job_id}_{date or datetime.now().strftime('%Y%m%d')}"
        if key not in self.states:
            self.states[key] = {'retries': 0}
        self.states[key]['retries'] = self.states.get(key, {}).get('retries', 0) + 1
        self._save_states()
        return self.states[key]['retries']

    def get_retry_count(self, job_id: str, date: str = None) -> int:
        return self.get_state(job_id, date).get('retries', 0)

    def is_completed_today(self, job_id: str) -> bool:
        today = datetime.now().strftime('%Y%m%d')
        state = self.get_state(job_id, today)
        return state.get('status') == 'completed'

    def get_pending_tasks(self) -> list:
        """获取今日待执行的任务"""
        today = datetime.now().strftime('%Y%m%d')
        pending = []
        for key, state in self.states.items():
            if key.endswith(today) and state.get('status') == 'pending':
                job_id = key.replace(f'_{today}', '')
                pending.append(job_id)
        return pending


class EmailNotifier:
    """邮件通知器"""

    def __init__(self):
        self.receivers = ['287363@qq.com']
        self.enabled = True

    def send(self, subject: str, content: str):
        if not self.enabled:
            return
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            smtp_server = os.getenv('EMAIL_SMTP_SERVER', 'smtp.qq.com')
            smtp_port = int(os.getenv('EMAIL_SMTP_PORT', 465))
            smtp_user = os.getenv('EMAIL_USERNAME', '287363@qq.com')
            smtp_password = os.getenv('EMAIL_PASSWORD', 'acearvjndkaxcaad')

            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = ','.join(self.receivers)
            msg['Subject'] = subject
            msg.attach(MIMEText(content, 'plain', 'utf-8'))

            if smtp_port == 465:
                with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                    server.login(smtp_user, smtp_password)
                    server.sendmail(smtp_user, self.receivers, msg.as_string())
            else:
                with smtplib.SMTP(smtp_server, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_password)
                    server.sendmail(smtp_user, self.receivers, msg.as_string())

            logger.info(f"✅ 邮件已发送: {subject}")
        except Exception as e:
            logger.error(f"❌ 邮件发送异常: {e}")


class EnhancedAPScheduler:
    """增强版 APScheduler"""

    def __init__(self):
        self.scheduler = BlockingScheduler(timezone='Asia/Shanghai')
        self.state_mgr = TaskStateManager()
        self.notifier = EmailNotifier()
        self.redis_client = self._init_redis()
        self.setup_listeners()

    def _init_redis(self):
        try:
            import redis
            client = redis.Redis(
                host=os.getenv('REDIS_HOST', '49.233.10.199'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                password=os.getenv('REDIS_PASSWORD', '100200'),
                decode_responses=True
            )
            client.ping()
            logger.info("✅ Redis 连接成功")
            return client
        except Exception as e:
            logger.warning(f"⚠️ Redis 连接失败: {e}")
            return None

    def acquire_lock(self, job_id: str, timeout: int = 600) -> bool:
        if not self.redis_client:
            return True
        lock_key = f"scheduler:lock:{job_id}"
        try:
            result = self.redis_client.set(lock_key, "1", nx=True, ex=timeout)
            if result:
                logger.info(f"🔐 获得任务锁: {job_id}")
                return True
            logger.warning(f"⏳ 任务正在执行中: {job_id}")
            return False
        except Exception as e:
            logger.error(f"❌ 获取锁失败: {e}")
            return True

    def release_lock(self, job_id: str):
        if not self.redis_client:
            return
        lock_key = f"scheduler:lock:{job_id}"
        try:
            self.redis_client.delete(lock_key)
            logger.info(f"🔓 释放任务锁: {job_id}")
        except Exception as e:
            logger.error(f"❌ 释放锁失败: {e}")

    def setup_listeners(self):
        self.scheduler.add_listener(
            self.job_executed,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        )

    def job_executed(self, event):
        if event.exception:
            logger.error(f"任务 {event.job_id} 执行失败: {event.exception}")
        else:
            logger.info(f"任务 {event.job_id} 执行成功")

    def log_job(self, job_name: str, job_id: str = None):
        logger.info(f"{'='*60}")
        logger.info(f"▶️  开始执行任务: {job_name}")
        logger.info(f"    时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"    任务ID: {job_id}")
        logger.info(f"{'='*60}")

    def run_script(self, script_path: Path, job_name: str, job_id: str = None,
                   timeout: int = 600, date: str = None, args: list = None) -> bool:
        if job_id and not self.acquire_lock(job_id, timeout):
            logger.warning(f"⏭️  跳过任务（已有实例运行）: {job_name}")
            return False

        self.log_job(job_name, job_id)
        self.state_mgr.set_state(job_id, 'running', date=date)

        output_content = ""
        error_content = ""

        try:
            cmd = [sys.executable, str(script_path)]
            if args:
                cmd.extend(args)
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(project_root)
            )

            output_content = result.stdout if result.stdout else ""
            error_content = result.stderr if result.stderr else ""
            success = result is None or result.returncode == 0

            if job_id:
                self.release_lock(job_id)

            if success:
                self.state_mgr.set_state(job_id, 'completed', '成功', date)
                report_details = self._generate_report(job_name, job_id, output_content, error_content, success=True)
                self.notifier.send(
                    f"✅ 任务执行成功: {job_name}",
                    report_details
                )
                logger.info(f"✅ 任务成功: {job_name}")
                return True
            else:
                error_msg = f'返回码: {result.returncode}' if result else '未知错误'
                self.handle_failure(job_id, job_name, error_msg, date, output_content, error_content)
                return False

        except subprocess.TimeoutExpired:
            self.handle_failure(job_id, job_name, '任务超时', date)
            return False
        except Exception as e:
            self.handle_failure(job_id, job_name, str(e), date)
            return False

    def _generate_report(self, job_name: str, job_id: str, stdout: str, stderr: str, success: bool = True) -> str:
        """生成任务执行报告（包含详情）"""
        lines = []
        lines.append(f"任务: {job_name}")
        lines.append(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"状态: {'✅ 成功' if success else '❌ 失败'}")
        lines.append(f"任务ID: {job_id}")
        lines.append("")
        lines.append("=" * 50)

        if job_id == 'data_fetch':
            details = self._parse_data_collect_output(stdout, stderr)
            lines.append("【数据采集详情】")
            lines.append(f"  股票总数: {details.get('total', 'N/A')}")
            lines.append(f"  新增采集: {details.get('success', 0)}")
            lines.append(f"  数据更新: {details.get('updated', 0)}")
            lines.append(f"  已是最新: {details.get('skipped', 0)}")
            lines.append(f"  采集失败: {details.get('failed', 0)}")
            if details.get('latest_date'):
                lines.append(f"  最新日期: {details.get('latest_date')}")
        elif job_id == 'data_quality_check':
            details = self._parse_data_audit_output(stdout, stderr)
            lines.append("【数据质检详情】")
            lines.append(f"  股票总数: {details.get('total', 'N/A')}")
            lines.append(f"  合格: {details.get('qualified', 0)}")
            lines.append(f"  不合格: {details.get('unqualified', 0)}")
            lines.append(f"  问题股票: {details.get('issues', '无')}")
        elif job_id == 'morning_report':
            details = self._parse_morning_report_output(stdout, stderr)
            lines.append("【晨间报告详情】")
            lines.append(f"  上证指数: {details.get('sh_index', 'N/A')}")
            lines.append(f"  涨跌: {details.get('sh_change', 'N/A')}")
            lines.append(f"  深证成指: {details.get('sz_index', 'N/A')}")
            lines.append(f"  涨跌: {details.get('sz_change', 'N/A')}")
        elif job_id in ['market_review', 'picks_review']:
            details = self._parse_review_output(stdout, stderr)
            lines.append("【复盘详情】")
            lines.append(f"  上涨股票: {details.get('up_count', 'N/A')}")
            lines.append(f"  下跌股票: {details.get('down_count', 'N/A')}")
            lines.append(f"  涨停股票: {details.get('limit_up_count', 'N/A')}")
            lines.append(f"  跌停股票: {details.get('limit_down_count', 'N/A')}")
        elif job_id == 'fund_behavior_report':
            details = self._parse_fund_behavior_output(stdout, stderr)
            lines.append("【量化决策详情】")
            lines.append(f"  周期定位: {details.get('market_state', 'N/A')}")
            lines.append(f"  市场基调: {details.get('market_tone', 'N/A')}")
            lines.append(f"  总成交额: {details.get('v_total', 'N/A')}")
            lines.append(f"  情绪温度: {details.get('sentiment_temp', 'N/A')}")
            lines.append(f"  10点变盘: {details.get('upward_pivot', 'N/A')}")
            lines.append(f"  对冲效果: {details.get('hedge_effect', 'N/A')}")
            lines.append(f"  波段股票: {details.get('trend_stocks', 0)}只")
            lines.append(f"  短线股票: {details.get('short_term_stocks', 0)}只")
            lines.append(f"  总仓位: {details.get('total_position', 'N/A')}")
            if details.get('warnings'):
                lines.append("  风险提示:")
                for w in details.get('warnings', []):
                    lines.append(f"    {w}")
        else:
            lines.append("【执行日志】")
            if stdout:
                stdout_lines = stdout.strip().split('\n')
                for line in stdout_lines[-20:]:
                    lines.append(f"  {line}")
            else:
                lines.append("  (无输出)")

        lines.append("")
        lines.append("=" * 50)

        if stderr and stderr.strip():
            lines.append("【错误信息】")
            stderr_lines = stderr.strip().split('\n')
            for line in stderr_lines[-10:]:
                lines.append(f"  {line}")

        return '\n'.join(lines)

    def _parse_data_collect_output(self, stdout: str, stderr: str) -> dict:
        """解析数据采集脚本输出"""
        result = {
            'total': 'N/A', 'success': 0, 'updated': 0, 'skipped': 0, 'failed': 0,
            'latest_date': None
        }
        combined = stdout + "\n" + stderr

        import re
        m = re.search(r'共\s*(\d+)\s*只股票', combined)
        if m:
            result['total'] = m.group(1)

        m = re.search(r'新增\s*(\d+)', combined)
        if m:
            result['success'] = int(m.group(1))

        m = re.search(r'更新\s*(\d+)', combined)
        if m:
            result['updated'] = int(m.group(1))

        m = re.search(r'已是最新\s*(\d+)', combined)
        if m:
            result['skipped'] = int(m.group(1))

        m = re.search(r'失败\s*(\d+)', combined)
        if m:
            result['failed'] = int(m.group(1))

        date_patterns = [
            r'最新日期[：:]\s*(\d{4}-\d{2}-\d{2})',
            r'检查缺失最新日期\s*(\d{4}-\d{2}-\d{2})',
        ]
        for pattern in date_patterns:
            m = re.search(pattern, combined)
            if m:
                result['latest_date'] = m.group(1)
                break

        return result

    def _parse_data_audit_output(self, stdout: str, stderr: str) -> dict:
        """解析数据质检脚本输出"""
        result = {'total': 'N/A', 'qualified': 0, 'unqualified': 0, 'issues': '无'}
        combined = stdout + "\n" + stderr

        import re
        m = re.search(r'共\s*(\d+)\s*只股票', combined)
        if m:
            result['total'] = m.group(1)

        m = re.search(r'合格[:\s]*(\d+)', combined)
        if m:
            result['qualified'] = int(m.group(1))

        m = re.search(r'不合格[:\s]*(\d+)', combined)
        if m:
            result['unqualified'] = int(m.group(1))

        issues = re.findall(r'问题[:\s]*(.+?)(?:\n|$)', combined)
        if issues:
            result['issues'] = ', '.join(issues[:5])

        return result

    def _parse_morning_report_output(self, stdout: str, stderr: str) -> dict:
        """解析晨间报告输出"""
        result = {'sh_index': 'N/A', 'sh_change': 'N/A', 'sz_index': 'N/A', 'sz_change': 'N/A'}
        combined = stdout + "\n" + stderr

        import re
        m = re.search(r'上证[指:]\s*([\d.]+)', combined)
        if m:
            result['sh_index'] = m.group(1)

        m = re.search(r'上证[涨跌:]\s*([-+]?[\d.]+)%?', combined)
        if m:
            result['sh_change'] = m.group(1)

        m = re.search(r'深证[指:]\s*([\d.]+)', combined)
        if m:
            result['sz_index'] = m.group(1)

        m = re.search(r'深证[涨跌:]\s*([-+]?[\d.]+)%?', combined)
        if m:
            result['sz_change'] = m.group(1)

        return result

    def _parse_fund_behavior_output(self, stdout: str, stderr: str) -> dict:
        """解析量化决策报告输出"""
        result = {
            'market_state': 'N/A',
            'market_tone': 'N/A',
            'v_total': 'N/A',
            'sentiment_temp': 'N/A',
            'upward_pivot': 'N/A',
            'hedge_effect': 'N/A',
            'trend_stocks': 0,
            'short_term_stocks': 0,
            'total_position': 'N/A',
            'warnings': []
        }
        combined = stdout + "\n" + stderr

        import re

        m = re.search(r'周期定位[：:]\s*(\w+)', combined)
        if m:
            result['market_state'] = m.group(1)

        m = re.search(r'市场基调[：:]\s*(\S+)', combined)
        if m:
            result['market_tone'] = m.group(1)

        m = re.search(r'当前总成交额\s*([\d.]+)\s*万亿', combined)
        if m:
            result['v_total'] = f"{float(m.group(1)):.2f}万亿"
        else:
            m = re.search(r'当前总成交额\s*([\d.]+)\s*亿', combined)
            if m:
                result['v_total'] = f"{float(m.group(1)):.1f}亿"

        m = re.search(r'情绪温度[：:].*?当前([\d.]+)[°]', combined)
        if m:
            result['sentiment_temp'] = f"{m.group(1)}°"

        m = re.search(r'10:00变盘判定[：:]\s*(\S+)', combined)
        if m:
            pivot_text = m.group(1)
            result['upward_pivot'] = '向上变盘' if '向上' in pivot_text else '未触发'

        m = re.search(r'量能充沛', combined)
        result['hedge_effect'] = '是' if m else '否'

        m = re.search(r'波段仓位[：:].*?([\d.]+)万', combined)
        if m:
            trend_pos = float(m.group(1))
            m2 = re.search(r'短线仓位[：:].*?([\d.]+)万', combined)
            short_pos = float(m2.group(1)) if m2 else 0
            m3 = re.search(r'现金储备[：:].*?([\d.]+)万', combined)
            cash_pos = float(m3.group(1)) if m3 else 0
            result['total_position'] = f"{trend_pos + short_pos + cash_pos:.0f}万"

        m = re.search(r'波段模式[）]（(\d+)只）', combined)
        if m:
            result['trend_stocks'] = int(m.group(1))

        m = re.search(r'短线/打板模式[）]（(\d+)只）', combined)
        if m:
            result['short_term_stocks'] = int(m.group(1))

        warnings = re.findall(r'[⚠✓]\s*([^\n]+)', combined)
        result['warnings'] = [w.strip() for w in warnings[:5]]

        return result

    def _parse_review_output(self, stdout: str, stderr: str) -> dict:
        """解析复盘输出"""
        result = {'up_count': 'N/A', 'down_count': 'N/A', 'limit_up_count': 'N/A', 'limit_down_count': 'N/A'}
        combined = stdout + "\n" + stderr

        import re
        m = re.search(r'上涨\s*(\d+)', combined)
        if m:
            result['up_count'] = m.group(1)

        m = re.search(r'下跌\s*(\d+)', combined)
        if m:
            result['down_count'] = m.group(1)

        m = re.search(r'涨停\s*(\d+)', combined)
        if m:
            result['limit_up_count'] = m.group(1)

        m = re.search(r'跌停\s*(\d+)', combined)
        if m:
            result['limit_down_count'] = m.group(1)

        return result

    def handle_failure(self, job_id: str, job_name: str, error: str,
                       date: str = None, stdout: str = "", stderr: str = ""):
        retry_count = self.state_mgr.increment_retry(job_id, date)
        logger.error(f"❌ 任务失败: {job_name}, 错误: {error}, 重试次数: {retry_count}/{MAX_RETRIES}")

        if retry_count < MAX_RETRIES:
            self.state_mgr.set_state(job_id, 'retry_scheduled', f'失败,{retry_count}次重试', date)
            threading.Thread(target=self.delayed_retry, args=(job_id, job_name, date), daemon=True).start()
        else:
            self.state_mgr.set_state(job_id, 'failed', error, date)
            report_details = self._generate_report(job_name, job_id, stdout, stderr, success=False)
            self.notifier.send(
                f"❌ 任务执行失败: {job_name}",
                report_details
            )

    def delayed_retry(self, job_id: str, job_name: str, date: str):
        logger.info(f"⏳ 计划 {RETRY_DELAY}秒 后重试任务: {job_name}")
        time.sleep(RETRY_DELAY)
        job = self.scheduler.get_job(job_id)
        if job:
            logger.info(f"🔄 执行重试: {job_name}")
            job.func()

    def add_jobs(self):
        """添加所有任务 - 与 cron_tasks.yaml 保持一致"""
        jobs = [
            {
                'id': 'morning_data',
                'script': 'scripts/pipeline/morning_update.py',
                'name': '晨间更新',
                'cron': '30 8 * * 1-5'
            },
            {
                'id': 'morning_report',
                'script': 'scripts/pipeline/send_morning.py',
                'name': '晨间报告',
                'cron': '45 8 * * 1-5',
                'depends_on': 'morning_data'
            },
            {
                'id': 'fund_behavior_report',
                'script': 'scripts/run_fund_behavior_strategy.py',
                'name': '量化决策报告',
                'cron': '9 9 * * 1-5'
            },
            {
                'id': 'data_fetch',
                'script': 'scripts/pipeline/data_collect.py',
                'name': '数据采集',
                'cron': '0 16 * * 1-5'
            },
            {
                'id': 'data_fetch_retry',
                'script': 'scripts/pipeline/data_collect.py',
                'name': '断点续传',
                'cron': '30 16 * * 1-5',
                'args': ['--retry']
            },
            {
                'id': 'data_quality_check',
                'script': 'scripts/pipeline/data_audit.py',
                'name': '数据质检',
                'cron': '0 17 * * 1-5'
            },
            {
                'id': 'market_review',
                'script': 'scripts/pipeline/daily_review.py',
                'name': '复盘分析',
                'cron': '30 17 * * 1-5'
            },
            {
                'id': 'picks_review',
                'script': 'scripts/pipeline/stock_pick.py',
                'name': '选股复盘',
                'cron': '45 17 * * 1-5'
            },
            {
                'id': 'review_report',
                'script': 'scripts/send_review_report.py',
                'name': '复盘报告',
                'cron': '0 18 * * 1-5'
            },
            {
                'id': 'review_brief',
                'script': 'scripts/pipeline/morning_push.py',
                'name': '复盘快报',
                'cron': '0 19 * * 1-5'
            },
            {
                'id': 'update_tracking',
                'script': 'scripts/pipeline/update_tracking.py',
                'name': '跟踪更新',
                'cron': '30 19 * * 1-5'
            },
            {
                'id': 'precompute',
                'script': 'scripts/pipeline/precompute.py',
                'name': '预计算评分',
                'cron': '0 20 * * 1-5'
            },
            {
                'id': 'night_analysis',
                'script': 'scripts/pipeline/night_picks.py',
                'name': '晚间分析',
                'cron': '30 20 * * 1-5'
            },
        ]

        for job in jobs:
            timeout = 600
            if job['id'] in ['data_fetch', 'data_fetch_retry']:
                timeout = 6600

            def make_func(j):
                def func():
                    today = datetime.now().strftime('%Y%m%d')
                    if self.state_mgr.is_completed_today(j['id']):
                        logger.info(f"⏭️  任务已完成，跳过: {j['name']}")
                        return True
                    args = j.get('args', [])
                    script_path = project_root / j['script']
                    return self.run_script(
                        script_path,
                        j['name'],
                        job_id=j['id'],
                        timeout=timeout,
                        date=today,
                        args=args
                    )
                return func

            trigger = CronTrigger.from_crontab(job['cron'])
            self.scheduler.add_job(
                make_func(job),
                trigger=trigger,
                id=job['id'],
                name=job['name'],
                replace_existing=True
            )
            logger.info(f"✅ 已添加任务: {job['id']} ({job['name']}) - {job['cron']}")

    def check_missed_tasks(self):
        """检查今日未执行的任务并尝试补执行"""
        today = datetime.now().strftime('%Y%m%d')
        now = datetime.now()

        missed_tasks = [
            {'id': 'data_fetch', 'name': '数据采集', 'hour': 16, 'minute': 0},
            {'id': 'data_fetch_retry', 'name': '断点续传', 'hour': 16, 'minute': 30},
            {'id': 'data_quality_check', 'name': '数据质检', 'hour': 17, 'minute': 0},
        ]

        for task in missed_tasks:
            task_time = now.replace(hour=task['hour'], minute=task['minute'], second=0, microsecond=0)
            if now > task_time + timedelta(minutes=10):
                state = self.state_mgr.get_state(task['id'], today)
                if state['status'] in ['pending', 'failed']:
                    logger.warning(f"⚠️ 检测到漏执行任务: {task['name']}, 尝试补执行...")
                    job = self.scheduler.get_job(task['id'])
                    if job:
                        job.func()

    def start(self):
        logger.info("=" * 70)
        logger.info("APScheduler 增强版启动")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        self.add_jobs()

        threading.Thread(target=heartbeat_loop, daemon=True).start()
        logger.info("✅ 心跳线程已启动")

        threading.Thread(target=self.check_missed_tasks, daemon=True).start()

        logger.info("\n调度器状态:")
        for job in self.scheduler.get_jobs():
            logger.info(f"  - {job.id}: {job.name} ({job.trigger})")

        logger.info("\n开始监听任务...")
        self.scheduler.start()


if __name__ == "__main__":
    scheduler = EnhancedAPScheduler()
    scheduler.start()
