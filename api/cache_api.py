"""
缓存管理 API

提供缓存刷新、统计和清理接口。
"""
import os
from typing import Dict, Any, Optional
from pathlib import Path

from flask import Flask, jsonify, request
from loguru import logger

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
import sys
sys.path.insert(0, str(project_root))

from core.cache.multi_level_cache import MultiLevelCache
from core.cache.memory_cache import MemoryCache
from core.cache.redis_cache import RedisCache

app = Flask(__name__)

# 全局缓存实例
cache_instance: Optional[MultiLevelCache] = None


def get_cache() -> MultiLevelCache:
    """获取或创建缓存实例"""
    global cache_instance
    if cache_instance is None:
        cache_instance = MultiLevelCache(
            l1_maxsize=1000,
            l1_ttl=3600,
            redis_host=os.getenv('REDIS_HOST', 'localhost'),
            redis_port=int(os.getenv('REDIS_PORT', 6379)),
            l2_ttl=86400
        )
    return cache_instance


@app.route('/api/cache/refresh', methods=['POST'])
def refresh_cache():
    """
    刷新缓存接口

    请求体:
        {
            "pattern": "*",        # 可选，匹配模式
            "level": "both"        # 可选，刷新级别 (l1, l2, both)
        }

    返回:
        {
            "success": true,
            "message": "缓存刷新成功",
            "cleared_keys": 100
        }
    """
    try:
        data = request.get_json() or {}
        pattern = data.get('pattern', '*')
        level = data.get('level', 'both')

        cache = get_cache()

        # 统计清理前状态
        stats_before = cache.get_stats()

        # 清理缓存
        cleared_count = 0

        if level in ('l1', 'both'):
            # 清理 L1 内存缓存
            if pattern == '*':
                cache.l1_cache.clear()
                cleared_count += stats_before.get('l1_keys', 0)
            else:
                # 根据模式匹配清理
                keys_to_remove = [
                    k for k in cache.l1_cache._cache.keys()
                    if pattern in k
                ]
                for key in keys_to_remove:
                    cache.l1_cache.delete(key)
                cleared_count += len(keys_to_remove)

        if level in ('l2', 'both') and cache.l2_cache:
            # 清理 L2 Redis 缓存
            try:
                if pattern == '*':
                    # 获取所有缓存键
                    all_keys = cache.l2_cache.redis.keys(f"{cache.l2_cache.key_prefix}*")
                    if all_keys:
                        cache.l2_cache.redis.delete(*all_keys)
                        cleared_count += len(all_keys)
                else:
                    # 根据模式匹配清理
                    matched_keys = cache.l2_cache.redis.keys(f"{cache.l2_cache.key_prefix}*{pattern}*")
                    if matched_keys:
                        cache.l2_cache.redis.delete(*matched_keys)
                        cleared_count += len(matched_keys)
            except Exception as e:
                logger.warning(f"清理 L2 缓存失败: {e}")

        # 统计清理后状态
        stats_after = cache.get_stats()

        logger.info(f"缓存刷新完成: 清理 {cleared_count} 个键")

        return jsonify({
            "success": True,
            "message": "缓存刷新成功",
            "cleared_keys": cleared_count,
            "stats_before": stats_before,
            "stats_after": stats_after
        })

    except Exception as e:
        logger.error(f"缓存刷新失败: {e}")
        return jsonify({
            "success": False,
            "message": f"缓存刷新失败: {str(e)}"
        }), 500


@app.route('/api/cache/stats', methods=['GET'])
def cache_stats():
    """
    获取缓存统计信息

    返回:
        {
            "l1": {
                "hits": 100,
                "misses": 20,
                "hit_ratio": 0.83,
                "keys": 50
            },
            "l2": {
                "hits": 50,
                "misses": 10,
                "hit_ratio": 0.83,
                "keys": 100
            }
        }
    """
    try:
        cache = get_cache()
        stats = cache.get_stats()

        # 计算命中率
        l1_total = stats.get('l1_hits', 0) + stats.get('l1_misses', 0)
        l2_total = stats.get('l2_hits', 0) + stats.get('l2_misses', 0)

        return jsonify({
            "success": True,
            "l1": {
                "hits": stats.get('l1_hits', 0),
                "misses": stats.get('l1_misses', 0),
                "hit_ratio": stats.get('l1_hits', 0) / l1_total if l1_total > 0 else 0,
                "keys": stats.get('l1_keys', 0)
            },
            "l2": {
                "hits": stats.get('l2_hits', 0),
                "misses": stats.get('l2_misses', 0),
                "hit_ratio": stats.get('l2_hits', 0) / l2_total if l2_total > 0 else 0,
                "keys": stats.get('l2_keys', 0)
            },
            "overall": {
                "total_hits": stats.get('l1_hits', 0) + stats.get('l2_hits', 0),
                "total_misses": stats.get('misses', 0)
            }
        })

    except Exception as e:
        logger.error(f"获取缓存统计失败: {e}")
        return jsonify({
            "success": False,
            "message": f"获取缓存统计失败: {str(e)}"
        }), 500


@app.route('/api/cache/keys', methods=['GET'])
def list_cache_keys():
    """
    列出缓存键

    查询参数:
        level: l1 或 l2
        pattern: 匹配模式
        limit: 最大返回数量

    返回:
        {
            "keys": ["key1", "key2", ...]
        }
    """
    try:
        level = request.args.get('level', 'l1')
        pattern = request.args.get('pattern', '*')
        limit = int(request.args.get('limit', 100))

        cache = get_cache()
        keys = []

        if level == 'l1':
            # 从内存缓存获取
            all_keys = list(cache.l1_cache._cache.keys())
            if pattern != '*':
                all_keys = [k for k in all_keys if pattern in k]
            keys = all_keys[:limit]

        elif level == 'l2' and cache.l2_cache:
            # 从 Redis 获取
            try:
                redis_keys = cache.l2_cache.redis.keys(f"{cache.l2_cache.key_prefix}{pattern}")
                keys = [k.decode('utf-8') if isinstance(k, bytes) else k
                        for k in redis_keys[:limit]]
            except Exception as e:
                logger.warning(f"获取 Redis 键失败: {e}")

        return jsonify({
            "success": True,
            "level": level,
            "pattern": pattern,
            "count": len(keys),
            "keys": keys
        })

    except Exception as e:
        logger.error(f"列出缓存键失败: {e}")
        return jsonify({
            "success": False,
            "message": f"列出缓存键失败: {str(e)}"
        }), 500


@app.route('/api/cache/get/<key>', methods=['GET'])
def get_cache_value(key: str):
    """
    获取缓存值

    路径参数:
        key: 缓存键

    返回:
        {
            "success": true,
            "key": "key1",
            "value": {...},
            "level": "l1"
        }
    """
    try:
        cache = get_cache()

        # 尝试从 L1 获取
        value = cache.l1_cache.get(key)
        if value is not None:
            return jsonify({
                "success": True,
                "key": key,
                "value": str(value)[:1000],  # 限制返回大小
                "level": "l1"
            })

        # 尝试从 L2 获取
        if cache.l2_cache:
            value = cache.l2_cache.get(key)
            if value is not None:
                return jsonify({
                    "success": True,
                    "key": key,
                    "value": str(value)[:1000],
                    "level": "l2"
                })

        return jsonify({
            "success": False,
            "message": "键不存在"
        }), 404

    except Exception as e:
        logger.error(f"获取缓存值失败: {e}")
        return jsonify({
            "success": False,
            "message": f"获取缓存值失败: {str(e)}"
        }), 500


@app.route('/api/cache/delete/<key>', methods=['DELETE'])
def delete_cache_key(key: str):
    """
    删除缓存键

    路径参数:
        key: 缓存键

    返回:
        {
            "success": true,
            "message": "删除成功"
        }
    """
    try:
        cache = get_cache()

        # 从 L1 删除
        cache.l1_cache.delete(key)

        # 从 L2 删除
        if cache.l2_cache:
            cache.l2_cache.delete(key)

        logger.info(f"删除缓存键: {key}")

        return jsonify({
            "success": True,
            "message": "删除成功"
        })

    except Exception as e:
        logger.error(f"删除缓存键失败: {e}")
        return jsonify({
            "success": False,
            "message": f"删除缓存键失败: {str(e)}"
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    return jsonify({
        "status": "healthy",
        "service": "cache-api"
    })


def run_api_server(host='0.0.0.0', port=5001, debug=False):
    """
    运行 API 服务器

    Args:
        host: 监听地址
        port: 监听端口
        debug: 是否调试模式
    """
    logger.info(f"启动缓存管理 API 服务器: {host}:{port}")
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='缓存管理 API 服务器')
    parser.add_argument('--host', default='0.0.0.0', help='监听地址')
    parser.add_argument('--port', type=int, default=5001, help='监听端口')
    parser.add_argument('--debug', action='store_true', help='调试模式')

    args = parser.parse_args()

    run_api_server(host=args.host, port=args.port, debug=args.debug)
