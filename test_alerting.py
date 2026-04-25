"""
告警通知器测试

测试 Webhook 告警通知功能。
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
from datetime import datetime


def test_alert_import():
    """测试告警模块导入"""
    logger.info("=" * 50)
    logger.info("测试告警模块导入")
    logger.info("=" * 50)
    
    try:
        from core.alerting.webhook_notifier import WebhookNotifier, Alert, AlertInhibitor, AlertAggregator
        logger.info("✅ 告警模块导入成功")
        return True
    except Exception as e:
        logger.error(f"❌ 告警模块导入失败: {e}")
        return False


def test_alert_creation():
    """测试告警创建"""
    logger.info("\n" + "=" * 50)
    logger.info("测试告警创建")
    logger.info("=" * 50)
    
    try:
        from core.alerting.webhook_notifier import Alert
        
        alert = Alert(
            name="TestAlert",
            status="firing",
            severity="warning",
            summary="测试告警",
            description="这是一个测试告警",
            labels={"team": "test", "service": "api"},
            annotations={"runbook": "https://example.com"}
        )
        
        assert alert.name == "TestAlert"
        assert alert.status == "firing"
        assert alert.severity == "warning"
        assert alert.fingerprint is not None
        
        logger.info("✅ 告警创建测试通过")
        logger.info(f"   告警名称: {alert.name}")
        logger.info(f"   指纹: {alert.fingerprint}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 告警创建测试失败: {e}")
        return False


def test_alert_inhibitor():
    """测试告警抑制器"""
    logger.info("\n" + "=" * 50)
    logger.info("测试告警抑制器")
    logger.info("=" * 50)
    
    try:
        from core.alerting.webhook_notifier import Alert, AlertInhibitor
        
        inhibit_rules = [
            {
                "source_match": {"alertname": "HighLatency"},
                "target_match": {"alertname": "SlowQuery"},
                "equal": ["instance"]
            }
        ]
        
        inhibitor = AlertInhibitor(inhibit_rules)
        
        # 创建源告警
        source_alert = Alert(
            name="HighLatency",
            status="firing",
            severity="critical",
            summary="高延迟",
            description="服务延迟过高",
            labels={"instance": "server1", "alertname": "HighLatency"}
        )
        
        # 创建目标告警
        target_alert = Alert(
            name="SlowQuery",
            status="firing",
            severity="warning",
            summary="慢查询",
            description="查询执行缓慢",
            labels={"instance": "server1", "alertname": "SlowQuery"}
        )
        
        active_alerts = [source_alert]
        
        # 测试抑制
        is_inhibited = inhibitor.check_inhibition(target_alert, active_alerts)
        assert is_inhibited is True, "告警应该被抑制"
        
        logger.info("✅ 告警抑制器测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 告警抑制器测试失败: {e}")
        return False


def test_alert_aggregator():
    """测试告警聚合器"""
    logger.info("\n" + "=" * 50)
    logger.info("测试告警聚合器")
    logger.info("=" * 50)
    
    try:
        from core.alerting.webhook_notifier import Alert, AlertAggregator
        
        aggregator = AlertAggregator(window_minutes=5)
        
        # 创建多个相似告警
        alerts = [
            Alert(
                name="HighCPU",
                status="firing",
                severity="warning",
                summary="CPU 使用率高",
                description=f"服务器 {i} CPU 使用率超过阈值",
                labels={"alertname": "HighCPU", "instance": f"server{i}"}
            )
            for i in range(3)
        ]
        
        # 添加告警到聚合器
        for alert in alerts:
            group = aggregator.add_alert(alert, group_by=["alertname"])
            if group:
                logger.info(f"   聚合组: {len(group.alerts)} 个告警")
        
        logger.info("✅ 告警聚合器测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 告警聚合器测试失败: {e}")
        return False


def test_webhook_notifier():
    """测试 Webhook 通知器"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 Webhook 通知器")
    logger.info("=" * 50)
    
    try:
        from core.alerting.webhook_notifier import WebhookNotifier, Alert
        
        # 创建通知器（使用默认配置）
        notifier = WebhookNotifier()
        
        # 创建测试告警
        alert = Alert(
            name="TestAlert",
            status="firing",
            severity="info",
            summary="测试告警",
            description="这是一个测试告警",
            labels={"team": "test"}
        )
        
        # 发送告警（不会实际发送，因为没有配置 webhook）
        result = notifier.send_alert(alert)
        
        logger.info("✅ Webhook 通知器测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ Webhook 通知器测试失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    logger.info("\n" + "=" * 60)
    logger.info("告警通知器测试套件")
    logger.info("=" * 60)
    
    tests = [
        ("模块导入", test_alert_import),
        ("告警创建", test_alert_creation),
        ("告警抑制器", test_alert_inhibitor),
        ("告警聚合器", test_alert_aggregator),
        ("Webhook 通知器", test_webhook_notifier),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            logger.error(f"❌ {name} 测试失败: {e}")
            failed += 1
    
    logger.info("\n" + "=" * 60)
    logger.info("测试总结")
    logger.info("=" * 60)
    logger.info(f"✅ 通过: {passed}")
    logger.info(f"❌ 失败: {failed}")
    logger.info(f"📊 总计: {passed + failed}")
    
    if failed == 0:
        logger.info("\n🎉 所有测试通过！")
    else:
        logger.info(f"\n⚠️ {failed} 个测试失败")
    
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
