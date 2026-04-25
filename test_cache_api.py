"""
缓存 API 测试

测试缓存管理接口功能。
"""
import sys
import json
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger


def test_cache_api_import():
    """测试缓存 API 模块导入"""
    logger.info("=" * 50)
    logger.info("测试缓存 API 模块导入")
    logger.info("=" * 50)
    
    try:
        from api.cache_api import app, get_cache, refresh_cache, cache_stats
        logger.info("✅ 缓存 API 模块导入成功")
        return True
    except Exception as e:
        logger.error(f"❌ 缓存 API 模块导入失败: {e}")
        return False


def test_cache_classes():
    """测试缓存类"""
    logger.info("\n" + "=" * 50)
    logger.info("测试缓存类")
    logger.info("=" * 50)
    
    try:
        from core.cache.multi_level_cache import MultiLevelCache
        from core.cache.memory_cache import MemoryCache
        
        # 测试内存缓存
        mem_cache = MemoryCache(maxsize=100, ttl=60)
        mem_cache.set("test_key", "test_value")
        value = mem_cache.get("test_key")
        assert value == "test_value", "内存缓存读写失败"
        
        logger.info("✅ 内存缓存测试通过")
        
        # 测试多级缓存（无 Redis）
        cache = MultiLevelCache(
            l1_maxsize=100,
            l1_ttl=60,
            use_redis=False
        )
        cache.set("key1", "value1")
        result = cache.get("key1")
        assert result == "value1", "多级缓存读写失败"
        
        logger.info("✅ 多级缓存测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 缓存类测试失败: {e}")
        return False


def test_cached_decorator():
    """测试缓存装饰器"""
    logger.info("\n" + "=" * 50)
    logger.info("测试缓存装饰器")
    logger.info("=" * 50)
    
    try:
        from core.cache.multi_level_cache import cached
        
        call_count = 0
        
        @cached(ttl=60, level='l1')
        def expensive_function(x):
            nonlocal call_count
            call_count += 1
            return x * x
        
        # 第一次调用
        result1 = expensive_function(5)
        assert result1 == 25, "计算结果错误"
        assert call_count == 1, "第一次调用计数错误"
        
        # 第二次调用（应该命中缓存）
        result2 = expensive_function(5)
        assert result2 == 25, "缓存结果错误"
        assert call_count == 1, "缓存未命中，函数被重复调用"
        
        logger.info("✅ 缓存装饰器测试通过")
        logger.info(f"   函数调用次数: {call_count}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 缓存装饰器测试失败: {e}")
        return False


def test_cache_api_routes():
    """测试缓存 API 路由"""
    logger.info("\n" + "=" * 50)
    logger.info("测试缓存 API 路由")
    logger.info("=" * 50)
    
    try:
        from api.cache_api import app
        
        # 使用 Flask 测试客户端
        client = app.test_client()
        
        # 测试健康检查
        response = client.get('/health')
        assert response.status_code == 200, "健康检查失败"
        data = json.loads(response.data)
        assert data['status'] == 'healthy', "健康状态错误"
        logger.info("✅ 健康检查接口正常")
        
        # 测试缓存统计
        response = client.get('/api/cache/stats')
        assert response.status_code == 200, "缓存统计接口失败"
        data = json.loads(response.data)
        assert data['success'] is True, "缓存统计返回错误"
        logger.info("✅ 缓存统计接口正常")
        
        # 测试缓存刷新
        response = client.post('/api/cache/refresh',
                              data=json.dumps({'pattern': '*', 'level': 'l1'}),
                              content_type='application/json')
        assert response.status_code == 200, "缓存刷新接口失败"
        data = json.loads(response.data)
        assert data['success'] is True, "缓存刷新返回错误"
        logger.info("✅ 缓存刷新接口正常")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 缓存 API 路由测试失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    logger.info("\n" + "=" * 60)
    logger.info("缓存 API 测试套件")
    logger.info("=" * 60)
    
    tests = [
        ("模块导入", test_cache_api_import),
        ("缓存类", test_cache_classes),
        ("缓存装饰器", test_cached_decorator),
        ("API 路由", test_cache_api_routes),
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
