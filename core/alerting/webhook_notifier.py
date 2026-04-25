"""
Webhook 告警通知器

提供告警通知、抑制和聚合功能。
支持多种通知渠道：Webhook、Slack、邮件、PagerDuty。
"""
import json
import hashlib
import time
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
import threading

import requests
import yaml
from loguru import logger


@dataclass
class Alert:
    """告警对象"""
    name: str
    status: str  # firing, resolved
    severity: str  # critical, warning, info
    summary: str
    description: str
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    fingerprint: str = ""

    def __post_init__(self):
        if not self.fingerprint:
            self.fingerprint = self._generate_fingerprint()
        if not self.starts_at:
            self.starts_at = datetime.now()

    def _generate_fingerprint(self) -> str:
        """生成告警指纹"""
        content = f"{self.name}:{json.dumps(self.labels, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]


@dataclass
class AlertGroup:
    """告警组"""
    name: str
    alerts: List[Alert] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class AlertInhibitor:
    """
    告警抑制器

    根据规则抑制低级别告警。
    """

    def __init__(self, rules: List[Dict[str, Any]]):
        self.rules = rules
        self._inhibited_alerts: Dict[str, datetime] = {}
        self._lock = threading.Lock()

    def check_inhibition(self, alert: Alert, active_alerts: List[Alert]) -> bool:
        """
        检查告警是否应被抑制

        Args:
            alert: 待检查告警
            active_alerts: 当前活动告警列表

        Returns:
            bool: 是否被抑制
        """
        with self._lock:
            # 检查是否已在抑制列表中
            if alert.fingerprint in self._inhibited_alerts:
                inhibit_until = self._inhibited_alerts[alert.fingerprint]
                if datetime.now() < inhibit_until:
                    return True
                else:
                    # 抑制过期，移除
                    del self._inhibited_alerts[alert.fingerprint]

            # 检查抑制规则
            for rule in self.rules:
                source_match = rule.get('source_match', {})
                target_match = rule.get('target_match', {})
                equal_labels = rule.get('equal', [])

                # 查找匹配的源告警
                for source_alert in active_alerts:
                    if self._match_labels(source_alert, source_match):
                        # 检查当前告警是否匹配目标
                        if self._match_labels(alert, target_match):
                            # 检查 equal 条件
                            if self._check_equal_labels(source_alert, alert, equal_labels):
                                # 抑制当前告警
                                inhibit_duration = rule.get('duration', 1800)  # 默认30分钟
                                self._inhibited_alerts[alert.fingerprint] = \
                                    datetime.now() + timedelta(seconds=inhibit_duration)
                                logger.info(f"告警 {alert.name} 被抑制，源告警: {source_alert.name}")
                                return True

            return False

    def _match_labels(self, alert: Alert, match_dict: Dict[str, str]) -> bool:
        """检查告警标签是否匹配"""
        for key, value in match_dict.items():
            if alert.labels.get(key) != value:
                return False
        return True

    def _check_equal_labels(self, alert1: Alert, alert2: Alert, labels: List[str]) -> bool:
        """检查两个告警的指定标签是否相等"""
        for label in labels:
            if alert1.labels.get(label) != alert2.labels.get(label):
                return False
        return True

    def cleanup_expired(self):
        """清理过期的抑制记录"""
        with self._lock:
            now = datetime.now()
            expired = [
                fp for fp, until in self._inhibited_alerts.items()
                if now >= until
            ]
            for fp in expired:
                del self._inhibited_alerts[fp]


class AlertAggregator:
    """
    告警聚合器

    将相似告警聚合为组，减少通知噪音。
    """

    def __init__(self, window_minutes: int = 5):
        self.window = timedelta(minutes=window_minutes)
        self._groups: Dict[str, AlertGroup] = {}
        self._lock = threading.Lock()

    def add_alert(self, alert: Alert, group_by: List[str]) -> Optional[AlertGroup]:
        """
        添加告警到聚合组

        Args:
            alert: 告警对象
            group_by: 聚合标签列表

        Returns:
            AlertGroup: 如果形成新的聚合组则返回
        """
        with self._lock:
            # 生成组键
            group_key = self._generate_group_key(alert, group_by)

            if group_key not in self._groups:
                # 创建新组
                self._groups[group_key] = AlertGroup(
                    name=f"group_{group_key}",
                    alerts=[alert]
                )
                return self._groups[group_key]
            else:
                # 添加到现有组
                group = self._groups[group_key]
                group.alerts.append(alert)
                group.updated_at = datetime.now()
                return None

    def get_ready_groups(self) -> List[AlertGroup]:
        """
        获取已准备好发送的聚合组

        Returns:
            List[AlertGroup]: 聚合组列表
        """
        with self._lock:
            now = datetime.now()
            ready_groups = []

            for key, group in list(self._groups.items()):
                if now - group.created_at >= self.window:
                    ready_groups.append(group)
                    del self._groups[key]

            return ready_groups

    def _generate_group_key(self, alert: Alert, group_by: List[str]) -> str:
        """生成聚合组键"""
        key_parts = [alert.name]
        for label in group_by:
            key_parts.append(f"{label}={alert.labels.get(label, '')}")
        return hashlib.md5("|".join(key_parts).encode()).hexdigest()[:12]

    def cleanup_old_groups(self, max_age_minutes: int = 30):
        """清理过期的聚合组"""
        with self._lock:
            now = datetime.now()
            expired = [
                key for key, group in self._groups.items()
                if now - group.created_at > timedelta(minutes=max_age_minutes)
            ]
            for key in expired:
                del self._groups[key]


class WebhookNotifier:
    """
    Webhook 告警通知器

    支持多种通知渠道，包括抑制和聚合功能。
    """

    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path("config/alerts.yaml")
        self.config = self._load_config()

        # 初始化抑制器
        inhibit_rules = self.config.get('inhibit_rules', [])
        self.inhibitor = AlertInhibitor(inhibit_rules)

        # 初始化聚合器
        aggregation_window = self.config.get('global', {}).get('aggregation_window', 5)
        self.aggregator = AlertAggregator(window_minutes=aggregation_window)

        # 活动告警
        self._active_alerts: Dict[str, Alert] = {}
        self._lock = threading.Lock()

        # 启动后台任务
        self._start_background_tasks()

    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"加载告警配置失败: {e}")
            return {}

    def _start_background_tasks(self):
        """启动后台清理任务"""
        def cleanup_task():
            while True:
                time.sleep(300)  # 每5分钟清理一次
                self.inhibitor.cleanup_expired()
                self.aggregator.cleanup_old_groups()

        thread = threading.Thread(target=cleanup_task, daemon=True)
        thread.start()

    def send_alert(self, alert: Alert) -> bool:
        """
        发送告警通知

        Args:
            alert: 告警对象

        Returns:
            bool: 是否发送成功
        """
        with self._lock:
            # 更新活动告警列表
            if alert.status == 'firing':
                self._active_alerts[alert.fingerprint] = alert
            else:
                self._active_alerts.pop(alert.fingerprint, None)

            # 检查抑制
            active_list = list(self._active_alerts.values())
            if self.inhibitor.check_inhibition(alert, active_list):
                logger.info(f"告警 {alert.name} 被抑制，跳过通知")
                return False

            # 获取路由配置
            routes = self.config.get('routing', {})
            default_route = routes.get('default', {})
            route_list = routes.get('routes', [])

            # 确定接收器
            receiver = self._resolve_receiver(alert, route_list, default_route)

            # 聚合告警
            group_by = default_route.get('group_by', ['alertname'])
            group = self.aggregator.add_alert(alert, group_by)

            # 如果是新组，立即发送；否则等待聚合窗口
            if group:
                return self._send_group_notification(group, receiver)

            return True

    def _resolve_receiver(
        self,
        alert: Alert,
        routes: List[Dict],
        default: Dict
    ) -> str:
        """解析告警接收器"""
        for route in routes:
            match = route.get('match', {})
            matched = True
            for key, value in match.items():
                if alert.labels.get(key) != value:
                    matched = False
                    break
            if matched:
                return route.get('receiver', default.get('receiver', 'default'))

        return default.get('receiver', 'default')

    def _send_group_notification(self, group: AlertGroup, receiver: str) -> bool:
        """发送聚合组通知"""
        receivers = self.config.get('receivers', [])
        receiver_config = None

        for r in receivers:
            if r.get('name') == receiver:
                receiver_config = r
                break

        if not receiver_config:
            logger.error(f"未找到接收器配置: {receiver}")
            return False

        success = True

        # Webhook 通知
        for webhook in receiver_config.get('webhook_configs', []):
            if not self._send_webhook(group, webhook):
                success = False

        # Slack 通知
        for slack in receiver_config.get('slack_configs', []):
            if not self._send_slack(group, slack):
                success = False

        # 邮件通知
        for email in receiver_config.get('email_configs', []):
            if not self._send_email(group, email):
                success = False

        # PagerDuty 通知
        for pd in receiver_config.get('pagerduty_configs', []):
            if not self._send_pagerduty(group, pd):
                success = False

        return success

    def _send_webhook(self, group: AlertGroup, config: Dict) -> bool:
        """发送 Webhook 通知"""
        try:
            url = config.get('url', '')
            if not url:
                return False

            payload = {
                'receiver': 'webhook',
                'status': 'firing',
                'alerts': [
                    {
                        'name': alert.name,
                        'status': alert.status,
                        'severity': alert.severity,
                        'summary': alert.summary,
                        'description': alert.description,
                        'labels': alert.labels,
                        'starts_at': alert.starts_at.isoformat() if alert.starts_at else None
                    }
                    for alert in group.alerts
                ],
                'groupKey': group.name,
                'commonLabels': self._extract_common_labels(group.alerts),
                'commonAnnotations': self._extract_common_annotations(group.alerts)
            }

            response = requests.post(
                url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            if response.status_code == 200:
                logger.info(f"Webhook 通知发送成功: {url}")
                return True
            else:
                logger.warning(f"Webhook 通知失败: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"发送 Webhook 通知失败: {e}")
            return False

    def _send_slack(self, group: AlertGroup, config: Dict) -> bool:
        """发送 Slack 通知"""
        try:
            url = config.get('api_url', '')
            if not url:
                return False

            # 构建 Slack 消息
            severity_emoji = {
                'critical': ':fire:',
                'warning': ':warning:',
                'info': ':information_source:'
            }

            alerts_text = "\n".join([
                f"• *{alert.name}*: {alert.summary}"
                for alert in group.alerts[:5]  # 最多显示5个
            ])

            if len(group.alerts) > 5:
                alerts_text += f"\n_...还有 {len(group.alerts) - 5} 个告警_"

            payload = {
                'channel': config.get('channel', '#alerts'),
                'username': 'XCNStock Alert',
                'icon_emoji': ':chart_with_upwards_trend:',
                'attachments': [{
                    'color': 'danger' if any(a.severity == 'critical' for a in group.alerts) else 'warning',
                    'title': config.get('title', 'XCNStock 告警'),
                    'text': alerts_text,
                    'footer': f"共 {len(group.alerts)} 个告警",
                    'ts': int(time.time())
                }]
            }

            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200

        except Exception as e:
            logger.error(f"发送 Slack 通知失败: {e}")
            return False

    def _send_email(self, group: AlertGroup, config: Dict) -> bool:
        """发送邮件通知"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            msg = MIMEMultipart()
            msg['From'] = config.get('from', 'alerts@xcnstock.com')
            msg['To'] = config.get('to', '')
            msg['Subject'] = f"[XCNStock] {len(group.alerts)} 个告警"

            body = f"""
            <h2>XCNStock 告警通知</h2>
            <p>共 {len(group.alerts)} 个告警</p>
            <hr>
            """

            for alert in group.alerts:
                body += f"""
                <h3>{alert.name} [{alert.severity}]</h3>
                <p><strong>{alert.summary}</strong></p>
                <p>{alert.description}</p>
                <hr>
                """

            msg.attach(MIMEText(body, 'html'))

            server = smtplib.SMTP(config.get('smarthost', 'localhost'))
            server.starttls()
            if config.get('auth_username'):
                server.login(
                    config.get('auth_username'),
                    config.get('auth_password', '')
                )

            server.send_message(msg)
            server.quit()

            logger.info(f"邮件通知发送成功: {config.get('to')}")
            return True

        except Exception as e:
            logger.error(f"发送邮件通知失败: {e}")
            return False

    def _send_pagerduty(self, group: AlertGroup, config: Dict) -> bool:
        """发送 PagerDuty 通知"""
        try:
            service_key = config.get('service_key', '')
            if not service_key:
                return False

            # 只发送严重告警到 PagerDuty
            critical_alerts = [a for a in group.alerts if a.severity == 'critical']
            if not critical_alerts:
                return True

            for alert in critical_alerts:
                payload = {
                    'service_key': service_key,
                    'event_type': 'trigger',
                    'description': alert.summary,
                    'details': {
                        'description': alert.description,
                        'severity': alert.severity,
                        'labels': alert.labels
                    }
                }

                response = requests.post(
                    'https://events.pagerduty.com/generic/2010-04-15/create_event.json',
                    json=payload,
                    timeout=10
                )

                if response.status_code != 200:
                    logger.warning(f"PagerDuty 通知失败: {response.status_code}")

            return True

        except Exception as e:
            logger.error(f"发送 PagerDuty 通知失败: {e}")
            return False

    def _extract_common_labels(self, alerts: List[Alert]) -> Dict[str, str]:
        """提取共同标签"""
        if not alerts:
            return {}

        common = dict(alerts[0].labels)
        for alert in alerts[1:]:
            for key in list(common.keys()):
                if alert.labels.get(key) != common[key]:
                    del common[key]

        return common

    def _extract_common_annotations(self, alerts: List[Alert]) -> Dict[str, str]:
        """提取共同注解"""
        if not alerts:
            return {}

        common = dict(alerts[0].annotations)
        for alert in alerts[1:]:
            for key in list(common.keys()):
                if alert.annotations.get(key) != common[key]:
                    del common[key]

        return common

    def process_aggregated_alerts(self):
        """处理聚合的告警组"""
        groups = self.aggregator.get_ready_groups()

        for group in groups:
            # 使用默认路由
            default_route = self.config.get('routing', {}).get('default', {})
            receiver = default_route.get('receiver', 'default')
            self._send_group_notification(group, receiver)


# 便捷函数
def create_alert(
    name: str,
    severity: str,
    summary: str,
    description: str,
    labels: Optional[Dict] = None
) -> Alert:
    """创建告警对象"""
    return Alert(
        name=name,
        status='firing',
        severity=severity,
        summary=summary,
        description=description,
        labels=labels or {}
    )


def send_alert_notification(alert: Alert, config_path: Optional[Path] = None) -> bool:
    """发送告警通知"""
    notifier = WebhookNotifier(config_path)
    return notifier.send_alert(alert)
