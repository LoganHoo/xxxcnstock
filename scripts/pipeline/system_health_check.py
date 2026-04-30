#!/usr/bin/env python3
"""
系统健康检查脚本
在数据采集前执行前置检查，确保环境就绪

检查分类：
1. 系统级检查 - 硬件、网络、操作系统资源
2. 程序级检查 - 依赖、配置、权限、环境

改进内容：
1. 严格的错误处理 - 关键检查失败返回 False
2. 支持命令行参数 - --min-disk, --timeout, --json
3. 新增内存/CPU/Kafka/数据目录权限检查
4. 添加重试机制 - 网络检查重试 3 次
5. 性能统计 - 记录每个检查的耗时
6. 前置条件检测 - Python版本、依赖包、环境变量、配置
"""
import sys
import os
import socket
import subprocess
import argparse
import json
import time
import psutil
import platform
import importlib.util
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 加载环境变量（可选依赖）
try:
    from dotenv import load_dotenv
    load_dotenv(project_root / '.env')
except ImportError:
    pass  # python-dotenv 是可选依赖，未安装时跳过


class HealthCheckResult:
    """健康检查结果类"""
    def __init__(self, name: str, passed: bool, message: str, duration_ms: float, 
                 details: Dict = None, level: str = "system"):
        self.name = name
        self.passed = passed
        self.message = message
        self.duration_ms = duration_ms
        self.details = details or {}
        self.timestamp = datetime.now().isoformat()
        self.level = level  # "system" 或 "program"
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "duration_ms": round(self.duration_ms, 2),
            "details": self.details,
            "timestamp": self.timestamp,
            "level": self.level
        }


class SystemHealthChecker:
    """系统健康检查器"""
    
    # 必需的依赖包
    REQUIRED_PACKAGES = [
        ("redis", "redis"),
        ("pymysql", "PyMySQL"),
        ("kafka", "kafka-python"),
        ("psutil", "psutil"),
        ("yaml", "PyYAML"),
        ("requests", "requests"),
        ("pandas", "pandas"),
        ("numpy", "numpy"),
    ]
    
    # 必需的环境变量
    REQUIRED_ENV_VARS = {
        "database": ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"],
        "redis": ["REDIS_HOST"],
        "kafka": ["KAFKA_BOOTSTRAP_SERVERS"],
        "api": ["TUSHARE_TOKEN"],
    }
    
    # 必需的配置文件
    REQUIRED_CONFIGS = [
        "config/cron_tasks.yaml",
        "config/database.yaml",
        "config/redis.yaml",
    ]
    
    def __init__(self, min_disk_gb: float = 10.0, timeout: int = 5, retry_count: int = 3, 
                 json_output: bool = False, skip_system: bool = False, skip_program: bool = False):
        self.min_disk_gb = min_disk_gb
        self.timeout = timeout
        self.retry_count = retry_count
        self.json_output = json_output
        self.skip_system = skip_system
        self.skip_program = skip_program
        self.results: List[HealthCheckResult] = []
        self.start_time = None
    
    def _retry_check(self, check_func, *args, **kwargs) -> Tuple[bool, str, Dict]:
        """带重试的检查"""
        last_error = None
        for attempt in range(self.retry_count):
            try:
                return check_func(*args, **kwargs)
            except Exception as e:
                last_error = str(e)
                if attempt < self.retry_count - 1:
                    time.sleep(0.5 * (attempt + 1))
        return False, f"重试{self.retry_count}次后失败: {last_error}", {}
    
    # ==================== 系统级检查 ====================
    
    def check_python_version(self) -> HealthCheckResult:
        """检查 Python 版本"""
        start = time.time()
        try:
            version = sys.version_info
            version_str = f"{version.major}.{version.minor}.{version.micro}"
            
            details = {
                "version": version_str,
                "major": version.major,
                "minor": version.minor,
                "micro": version.micro,
                "platform": platform.python_implementation()
            }
            
            # 要求 Python 3.8+
            if version.major < 3 or (version.major == 3 and version.minor < 8):
                return HealthCheckResult(
                    "Python版本", False,
                    f"Python版本过低: {version_str} (需要 3.8+)",
                    (time.time() - start) * 1000, details, "system"
                )
            
            return HealthCheckResult(
                "Python版本", True,
                f"Python版本正常: {version_str}",
                (time.time() - start) * 1000, details, "system"
            )
        except Exception as e:
            return HealthCheckResult(
                "Python版本", False,
                f"Python版本检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "system"
            )
    
    def check_disk_space(self) -> HealthCheckResult:
        """检查磁盘空间"""
        start = time.time()
        try:
            stat = os.statvfs(project_root)
            free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
            total_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)
            used_percent = ((total_gb - free_gb) / total_gb) * 100
            
            details = {
                "free_gb": round(free_gb, 2),
                "total_gb": round(total_gb, 2),
                "used_percent": round(used_percent, 1),
                "min_required_gb": self.min_disk_gb
            }
            
            if free_gb < self.min_disk_gb:
                return HealthCheckResult(
                    "磁盘空间", False,
                    f"磁盘空间不足: {free_gb:.1f}GB (需要 {self.min_disk_gb}GB)",
                    (time.time() - start) * 1000, details, "system"
                )
            
            if used_percent > 90:
                return HealthCheckResult(
                    "磁盘空间", False,
                    f"磁盘使用率过高: {used_percent:.1f}%",
                    (time.time() - start) * 1000, details, "system"
                )
            
            return HealthCheckResult(
                "磁盘空间", True,
                f"磁盘空间充足: {free_gb:.1f}GB / {total_gb:.1f}GB ({used_percent:.1f}% 已用)",
                (time.time() - start) * 1000, details, "system"
            )
        except Exception as e:
            return HealthCheckResult(
                "磁盘空间", False,
                f"磁盘空间检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "system"
            )
    
    def check_temp_space(self) -> HealthCheckResult:
        """检查临时目录空间"""
        start = time.time()
        try:
            temp_dir = Path(os.getenv('TEMP', '/tmp'))
            stat = os.statvfs(temp_dir)
            free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
            
            details = {
                "temp_dir": str(temp_dir),
                "free_gb": round(free_gb, 2),
                "min_required_gb": 1.0
            }
            
            if free_gb < 1.0:
                return HealthCheckResult(
                    "临时目录空间", False,
                    f"临时目录空间不足: {free_gb:.1f}GB (需要 1GB)",
                    (time.time() - start) * 1000, details, "system"
                )
            
            return HealthCheckResult(
                "临时目录空间", True,
                f"临时目录空间充足: {free_gb:.1f}GB",
                (time.time() - start) * 1000, details, "system"
            )
        except Exception as e:
            return HealthCheckResult(
                "临时目录空间", False,
                f"临时目录空间检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "system"
            )
    
    def check_memory(self) -> HealthCheckResult:
        """检查内存"""
        start = time.time()
        try:
            mem = psutil.virtual_memory()
            details = {
                "total_gb": round(mem.total / (1024**3), 2),
                "available_gb": round(mem.available / (1024**3), 2),
                "used_percent": mem.percent,
                "free_gb": round(mem.free / (1024**3), 2)
            }
            
            if mem.available < 2 * 1024**3 or mem.percent > 95:
                return HealthCheckResult(
                    "内存", False,
                    f"内存不足: {mem.percent}% 已用, 可用 {mem.available/(1024**3):.1f}GB",
                    (time.time() - start) * 1000, details, "system"
                )
            
            return HealthCheckResult(
                "内存", True,
                f"内存正常: {mem.percent}% 已用, 可用 {mem.available/(1024**3):.1f}GB / {mem.total/(1024**3):.1f}GB",
                (time.time() - start) * 1000, details, "system"
            )
        except Exception as e:
            return HealthCheckResult(
                "内存", False,
                f"内存检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "system"
            )
    
    def check_cpu(self) -> HealthCheckResult:
        """检查 CPU 负载"""
        start = time.time()
        try:
            load1, load5, load15 = os.getloadavg()
            cpu_count = os.cpu_count()
            cpu_percent = psutil.cpu_percent(interval=1)
            
            details = {
                "load_1min": round(load1, 2),
                "load_5min": round(load5, 2),
                "load_15min": round(load15, 2),
                "cpu_count": cpu_count,
                "cpu_percent": cpu_percent
            }
            
            if load1 > cpu_count or cpu_percent > 95:
                return HealthCheckResult(
                    "CPU", False,
                    f"CPU 负载过高: {cpu_percent}% 使用率, 负载 {load1:.2f} (核心数: {cpu_count})",
                    (time.time() - start) * 1000, details, "system"
                )
            
            return HealthCheckResult(
                "CPU", True,
                f"CPU 正常: {cpu_percent}% 使用率, 负载 {load1:.2f} / {cpu_count}",
                (time.time() - start) * 1000, details, "system"
            )
        except Exception as e:
            return HealthCheckResult(
                "CPU", False,
                f"CPU 检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "system"
            )
    
    def check_network(self) -> HealthCheckResult:
        """检查网络连接"""
        start = time.time()
        
        def _check() -> Tuple[bool, str, Dict]:
            socket.create_connection(("8.8.8.8", 53), timeout=self.timeout)
            return True, "网络连接正常", {}
        
        passed, message, details = self._retry_check(_check)
        return HealthCheckResult(
            "网络连接", passed, message,
            (time.time() - start) * 1000, details, "system"
        )
    
    # ==================== 程序级检查 ====================
    
    def check_dependencies(self) -> HealthCheckResult:
        """检查必需的依赖包"""
        start = time.time()
        missing = []
        installed = []
        
        for module_name, package_name in self.REQUIRED_PACKAGES:
            try:
                spec = importlib.util.find_spec(module_name)
                if spec is None:
                    missing.append(package_name)
                else:
                    # 尝试导入获取版本
                    try:
                        module = importlib.import_module(module_name)
                        version = getattr(module, '__version__', 'unknown')
                        installed.append(f"{package_name}({version})")
                    except Exception:
                        installed.append(package_name)
            except Exception:
                missing.append(package_name)
        
        details = {
            "installed": installed,
            "missing": missing
        }
        
        if missing:
            return HealthCheckResult(
                "依赖包", False,
                f"缺少依赖包: {', '.join(missing)}",
                (time.time() - start) * 1000, details, "program"
            )
        
        return HealthCheckResult(
            "依赖包", True,
            f"所有依赖包已安装 ({len(installed)}个)",
            (time.time() - start) * 1000, details, "program"
        )
    
    def check_env_variables(self) -> HealthCheckResult:
        """检查环境变量完整性"""
        start = time.time()
        missing_by_category = {}
        
        for category, vars_list in self.REQUIRED_ENV_VARS.items():
            missing = [var for var in vars_list if not os.getenv(var)]
            if missing:
                missing_by_category[category] = missing
        
        details = {
            "missing_by_category": missing_by_category,
            "configured": []
        }
        
        # 记录已配置的环境变量
        for category, vars_list in self.REQUIRED_ENV_VARS.items():
            for var in vars_list:
                if os.getenv(var):
                    # 隐藏敏感信息
                    val = os.getenv(var)
                    masked = val[:4] + "****" if len(val) > 4 else "****"
                    details["configured"].append(f"{var}={masked}")
        
        if missing_by_category:
            categories = ", ".join(missing_by_category.keys())
            return HealthCheckResult(
                "环境变量", False,
                f"环境变量不完整: {categories} 相关配置缺失",
                (time.time() - start) * 1000, details, "program"
            )
        
        return HealthCheckResult(
            "环境变量", True,
            f"环境变量配置完整 ({len(details['configured'])}个)",
            (time.time() - start) * 1000, details, "program"
        )
    
    def check_config_files(self) -> HealthCheckResult:
        """检查配置文件"""
        start = time.time()
        missing = []
        invalid = []
        valid = []
        
        for config_path in self.REQUIRED_CONFIGS:
            full_path = project_root / config_path
            if not full_path.exists():
                missing.append(config_path)
            else:
                # 尝试解析 YAML
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    try:
                        import yaml
                        with open(full_path, 'r', encoding='utf-8') as f:
                            yaml.safe_load(f)
                        valid.append(config_path)
                    except Exception as e:
                        invalid.append(f"{config_path}: {e}")
                else:
                    valid.append(config_path)
        
        details = {
            "valid": valid,
            "missing": missing,
            "invalid": invalid
        }
        
        if missing or invalid:
            errors = []
            if missing:
                errors.append(f"缺失: {', '.join(missing)}")
            if invalid:
                errors.append(f"格式错误: {', '.join(invalid)}")
            return HealthCheckResult(
                "配置文件", False,
                f"配置文件问题: {'; '.join(errors)}",
                (time.time() - start) * 1000, details, "program"
            )
        
        return HealthCheckResult(
            "配置文件", True,
            f"配置文件有效 ({len(valid)}个)",
            (time.time() - start) * 1000, details, "program"
        )
    
    def check_data_directory(self) -> HealthCheckResult:
        """检查数据目录权限"""
        start = time.time()
        try:
            data_dir = project_root / "data"
            logs_dir = project_root / "logs"
            reports_dir = project_root / "reports"
            
            checks = []
            errors = []
            
            # 检查 data 目录
            if data_dir.exists():
                if os.access(data_dir, os.W_OK):
                    checks.append("data目录可写")
                else:
                    errors.append("data目录不可写")
            else:
                try:
                    data_dir.mkdir(parents=True, exist_ok=True)
                    checks.append("data目录已创建")
                except Exception as e:
                    errors.append(f"无法创建data目录: {e}")
            
            # 检查 logs 目录
            if logs_dir.exists():
                if os.access(logs_dir, os.W_OK):
                    checks.append("logs目录可写")
                else:
                    errors.append("logs目录不可写")
            else:
                try:
                    logs_dir.mkdir(parents=True, exist_ok=True)
                    checks.append("logs目录已创建")
                except Exception as e:
                    errors.append(f"无法创建logs目录: {e}")
            
            # 检查 reports 目录
            if reports_dir.exists():
                if os.access(reports_dir, os.W_OK):
                    checks.append("reports目录可写")
                else:
                    errors.append("reports目录不可写")
            else:
                try:
                    reports_dir.mkdir(parents=True, exist_ok=True)
                    checks.append("reports目录已创建")
                except Exception as e:
                    errors.append(f"无法创建reports目录: {e}")
            
            if errors:
                return HealthCheckResult(
                    "数据目录", False,
                    f"目录权限问题: {'; '.join(errors)}",
                    (time.time() - start) * 1000,
                    {"errors": errors, "checks": checks}, "program"
                )
            
            return HealthCheckResult(
                "数据目录", True,
                f"数据目录检查通过: {', '.join(checks)}",
                (time.time() - start) * 1000,
                {"data_dir": str(data_dir), "logs_dir": str(logs_dir), "reports_dir": str(reports_dir)},
                "program"
            )
        except Exception as e:
            return HealthCheckResult(
                "数据目录", False,
                f"数据目录检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "program"
            )
    
    def check_redis(self) -> HealthCheckResult:
        """检查 Redis 连接"""
        start = time.time()
        try:
            import redis
            redis_host = os.getenv('REDIS_HOST')
            redis_port = int(os.getenv('REDIS_PORT', 6379))
            redis_password = os.getenv('REDIS_PASSWORD')
            redis_db = int(os.getenv('REDIS_DB', 0))
            
            if not redis_host:
                return HealthCheckResult(
                    "Redis", False,
                    "REDIS_HOST 未设置",
                    (time.time() - start) * 1000, {}, "program"
                )
            
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                db=redis_db,
                socket_connect_timeout=self.timeout,
                decode_responses=True
            )
            client.ping()
            info = client.info()
            
            details = {
                "host": redis_host,
                "port": redis_port,
                "version": info.get("redis_version", "unknown"),
                "used_memory_human": info.get("used_memory_human", "unknown"),
                "connected_clients": info.get("connected_clients", 0)
            }
            
            return HealthCheckResult(
                "Redis", True,
                f"Redis 连接正常 ({redis_host}:{redis_port}, 版本: {details['version']})",
                (time.time() - start) * 1000, details, "program"
            )
        except ImportError:
            return HealthCheckResult(
                "Redis", False,
                "Redis 模块未安装 (pip install redis)",
                (time.time() - start) * 1000, {}, "program"
            )
        except Exception as e:
            return HealthCheckResult(
                "Redis", False,
                f"Redis 连接失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "program"
            )
    
    def check_mysql(self) -> HealthCheckResult:
        """检查 MySQL 连接"""
        start = time.time()
        try:
            import pymysql
            mysql_host = os.getenv('DB_HOST') or os.getenv('MYSQL_HOST')
            mysql_port = int(os.getenv('DB_PORT') or os.getenv('MYSQL_PORT', 3306))
            mysql_user = os.getenv('DB_USER') or os.getenv('MYSQL_USER')
            mysql_password = os.getenv('DB_PASSWORD') or os.getenv('MYSQL_PASSWORD')
            mysql_database = os.getenv('DB_NAME') or os.getenv('MYSQL_DATABASE')
            
            if not all([mysql_host, mysql_user, mysql_password]):
                missing = []
                if not mysql_host: missing.append("DB_HOST")
                if not mysql_user: missing.append("DB_USER")
                if not mysql_password: missing.append("DB_PASSWORD")
                return HealthCheckResult(
                    "MySQL", False,
                    f"MySQL 配置不完整: {', '.join(missing)} 未设置",
                    (time.time() - start) * 1000, {}, "program"
                )
            
            conn = pymysql.connect(
                host=mysql_host,
                port=mysql_port,
                user=mysql_user,
                password=mysql_password,
                database=mysql_database,
                connect_timeout=self.timeout
            )
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
            
            conn.close()
            
            details = {
                "host": mysql_host,
                "port": mysql_port,
                "database": mysql_database,
                "version": version
            }
            
            return HealthCheckResult(
                "MySQL", True,
                f"MySQL 连接正常 ({mysql_host}:{mysql_port}/{mysql_database}, 版本: {version})",
                (time.time() - start) * 1000, details, "program"
            )
        except ImportError:
            return HealthCheckResult(
                "MySQL", False,
                "PyMySQL 模块未安装 (pip install pymysql)",
                (time.time() - start) * 1000, {}, "program"
            )
        except Exception as e:
            return HealthCheckResult(
                "MySQL", False,
                f"MySQL 连接失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "program"
            )
    
    def check_kafka(self) -> HealthCheckResult:
        """检查 Kafka 连接"""
        start = time.time()
        try:
            from kafka import KafkaAdminClient
            kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            
            if not kafka_servers or kafka_servers == 'localhost:9092':
                return HealthCheckResult(
                    "Kafka", True,
                    "Kafka 未配置，跳过检查",
                    (time.time() - start) * 1000, {"skipped": True}, "program"
                )
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_servers,
                request_timeout_ms=self.timeout * 1000
            )
            topics = admin_client.list_topics()
            admin_client.close()
            
            details = {
                "servers": kafka_servers,
                "topic_count": len(topics)
            }
            
            return HealthCheckResult(
                "Kafka", True,
                f"Kafka 连接正常 ({kafka_servers}, {len(topics)} 个 topics)",
                (time.time() - start) * 1000, details, "program"
            )
        except ImportError:
            return HealthCheckResult(
                "Kafka", True,
                "Kafka 模块未安装，跳过检查",
                (time.time() - start) * 1000, {"skipped": True}, "program"
            )
        except Exception as e:
            return HealthCheckResult(
                "Kafka", False,
                f"Kafka 连接失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "program"
            )
    
    def check_api_availability(self) -> HealthCheckResult:
        """检查主要 API 可用性"""
        start = time.time()
        apis = [
            ("tushare", "api.tushare.pro", 80),
            ("akshare", "www.akshare.xyz", 443),
        ]
        
        api_results = []
        all_ok = True
        
        for name, host, port in apis:
            try:
                socket.create_connection((host, port), timeout=self.timeout)
                api_results.append({"name": name, "status": "ok", "host": host, "port": port})
            except Exception as e:
                api_results.append({"name": name, "status": "failed", "error": str(e)})
                all_ok = False
        
        status_msg = "所有 API 可访问" if all_ok else "部分 API 不可访问"
        return HealthCheckResult(
            "API 可用性", all_ok, status_msg,
            (time.time() - start) * 1000,
            {"apis": api_results}, "program"
        )
    
    # ==================== 涨停系统专项检查 ====================

    def check_limitup_kline_data(self) -> HealthCheckResult:
        """检查涨停K线数据"""
        start = time.time()

        kline_dir = project_root / 'data' / 'kline'
        if not kline_dir.exists():
            return HealthCheckResult(
                "涨停K线数据", False,
                f"K线数据目录不存在: {kline_dir}",
                (time.time() - start) * 1000, {"path": str(kline_dir)}, "program"
            )

        parquet_files = list(kline_dir.glob("*.parquet"))
        if len(parquet_files) < 100:
            return HealthCheckResult(
                "涨停K线数据", False,
                f"K线数据文件数量不足: {len(parquet_files)} < 100",
                (time.time() - start) * 1000,
                {"count": len(parquet_files)}, "program"
            )

        # 检查数据新鲜度
        try:
            sample_file = parquet_files[0]
            df = pd.read_parquet(sample_file)
            
            # 兼容不同列名 (trade_date 或 date)
            date_col = 'trade_date' if 'trade_date' in df.columns else 'date'
            latest_date = pd.to_datetime(df[date_col]).max()
            days_diff = (datetime.now() - latest_date).days

            if days_diff > 7:
                return HealthCheckResult(
                    "涨停K线数据", False,
                    f"K线数据过旧: 最新数据 {days_diff} 天前",
                    (time.time() - start) * 1000,
                    {"latest_date": latest_date.strftime('%Y-%m-%d'), "days_diff": days_diff}, "program"
                )

            return HealthCheckResult(
                "涨停K线数据", True,
                f"K线数据正常: {len(parquet_files)} 只股票, 最新数据 {days_diff} 天前",
                (time.time() - start) * 1000,
                {"count": len(parquet_files), "latest_date": latest_date.strftime('%Y-%m-%d')}, "program"
            )
        except Exception as e:
            return HealthCheckResult(
                "涨停K线数据", False,
                f"K线数据检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "program"
            )

    def check_limitup_data_directory(self) -> HealthCheckResult:
        """检查涨停数据目录"""
        start = time.time()

        limitup_dir = project_root / 'data' / 'limitup'
        limitup_dir.mkdir(parents=True, exist_ok=True)

        # 检查目录可写
        try:
            test_file = limitup_dir / '.test_write'
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            return HealthCheckResult(
                "涨停数据目录", False,
                f"涨停数据目录不可写: {e}",
                (time.time() - start) * 1000, {"path": str(limitup_dir)}, "program"
            )

        return HealthCheckResult(
            "涨停数据目录", True,
            f"涨停数据目录正常: {limitup_dir}",
            (time.time() - start) * 1000, {"path": str(limitup_dir)}, "program"
        )

    def check_limitup_strategy_config(self) -> HealthCheckResult:
        """检查涨停策略配置"""
        start = time.time()

        config_file = project_root / 'config' / 'limitup_config.yaml'
        if not config_file.exists():
            return HealthCheckResult(
                "涨停策略配置", False,
                f"涨停策略配置文件不存在: {config_file}",
                (time.time() - start) * 1000, {"path": str(config_file)}, "program"
            )

        try:
            import yaml
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            required_keys = ['data', 'selection']
            for key in required_keys:
                if key not in config:
                    return HealthCheckResult(
                        "涨停策略配置", False,
                        f"配置缺少必要字段: {key}",
                        (time.time() - start) * 1000, {"missing_key": key}, "program"
                    )

            return HealthCheckResult(
                "涨停策略配置", True,
                f"涨停策略配置正常",
                (time.time() - start) * 1000, {"path": str(config_file)}, "program"
            )
        except Exception as e:
            return HealthCheckResult(
                "涨停策略配置", False,
                f"涨停策略配置检查失败: {e}",
                (time.time() - start) * 1000, {"error": str(e)}, "program"
            )

    # ==================== 运行所有检查 ====================
    
    def run_all_checks(self) -> List[HealthCheckResult]:
        """运行所有检查"""
        self.start_time = time.time()
        
        # 系统级检查
        system_checks = [
            ("Python版本", self.check_python_version),
            ("磁盘空间", self.check_disk_space),
            ("临时目录空间", self.check_temp_space),
            ("内存", self.check_memory),
            ("CPU", self.check_cpu),
            ("网络连接", self.check_network),
        ]
        
        # 程序级检查
        program_checks = [
            ("依赖包", self.check_dependencies),
            ("环境变量", self.check_env_variables),
            ("配置文件", self.check_config_files),
            ("数据目录", self.check_data_directory),
            ("Redis", self.check_redis),
            ("MySQL", self.check_mysql),
            ("Kafka", self.check_kafka),
            ("API 可用性", self.check_api_availability),
        ]

        # 涨停系统专项检查
        limitup_checks = [
            ("涨停K线数据", self.check_limitup_kline_data),
            ("涨停数据目录", self.check_limitup_data_directory),
            ("涨停策略配置", self.check_limitup_strategy_config),
        ]
        
        checks_to_run = []
        if not self.skip_system:
            checks_to_run.extend(system_checks)
        if not self.skip_program:
            checks_to_run.extend(program_checks)
            checks_to_run.extend(limitup_checks)

        for name, check_func in checks_to_run:
            if not self.json_output:
                print(f"\n🔍 检查 {name}...")
            result = check_func()
            self.results.append(result)
            if not self.json_output:
                icon = "✅" if result.passed else "❌"
                level_icon = "🖥️" if result.level == "system" else "📦"
                print(f"{icon} {level_icon} {result.message}")
        
        return self.results
    
    def print_summary(self):
        """打印检查摘要"""
        total_time = (time.time() - self.start_time) * 1000 if self.start_time else 0
        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed
        
        system_checks = [r for r in self.results if r.level == "system"]
        program_checks = [r for r in self.results if r.level == "program"]
        
        system_passed = sum(1 for r in system_checks if r.passed)
        program_passed = sum(1 for r in program_checks if r.passed)
        
        if self.json_output:
            output = {
                "timestamp": datetime.now().isoformat(),
                "overall": {
                    "passed": passed,
                    "failed": failed,
                    "total": len(self.results),
                    "total_time_ms": round(total_time, 2),
                    "healthy": failed == 0
                },
                "system_level": {
                    "passed": system_passed,
                    "failed": len(system_checks) - system_passed,
                    "total": len(system_checks)
                },
                "program_level": {
                    "passed": program_passed,
                    "failed": len(program_checks) - program_passed,
                    "total": len(program_checks)
                },
                "checks": [r.to_dict() for r in self.results]
            }
            print(json.dumps(output, indent=2, ensure_ascii=False))
        else:
            print("\n" + "=" * 70)
            print(f"系统健康检查摘要 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)
            print(f"🖥️  系统级: {system_passed}/{len(system_checks)} 通过")
            print(f"📦 程序级: {program_passed}/{len(program_checks)} 通过")
            print(f"📊 总计: {passed}/{len(self.results)} 通过 | 总耗时: {total_time:.0f}ms")
            print("=" * 70)
            
            if failed == 0:
                print("✅ 所有检查通过，系统健康")
            else:
                print("❌ 部分检查未通过，请修复后重试")
                print("\n失败的检查:")
                for r in self.results:
                    if not r.passed:
                        level_str = "[系统]" if r.level == "system" else "[程序]"
                        print(f"  {level_str} {r.name}: {r.message}")
            print("=" * 70)
        
        return failed == 0


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="系统健康检查脚本 - 在数据采集前执行前置检查",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
检查分类：
  🖥️  系统级 - Python版本、磁盘、内存、CPU、网络
  📦 程序级 - 依赖包、环境变量、配置文件、服务连接

示例:
  python system_health_check.py
  python system_health_check.py --min-disk 20 --timeout 10
  python system_health_check.py --json --retry 5
  python system_health_check.py --skip-system  # 只检查程序级
  python system_health_check.py --skip-program  # 只检查系统级
        """
    )
    
    parser.add_argument(
        "--min-disk",
        type=float,
        default=10.0,
        help="最小可用磁盘空间(GB)，默认 10GB"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=5,
        help="连接超时时间(秒)，默认 5秒"
    )
    parser.add_argument(
        "--retry",
        type=int,
        default=3,
        help="失败重试次数，默认 3次"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="输出 JSON 格式结果"
    )
    parser.add_argument(
        "--skip-system",
        action="store_true",
        help="跳过系统级检查"
    )
    parser.add_argument(
        "--skip-program",
        action="store_true",
        help="跳过程序级检查"
    )
    
    args = parser.parse_args()
    
    if not args.json:
        print("=" * 70)
        print(f"系统健康检查 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
    
    checker = SystemHealthChecker(
        min_disk_gb=args.min_disk,
        timeout=args.timeout,
        retry_count=args.retry,
        json_output=args.json,
        skip_system=args.skip_system,
        skip_program=args.skip_program
    )
    
    checker.run_all_checks()
    healthy = checker.print_summary()
    
    return 0 if healthy else 1


if __name__ == '__main__':
    sys.exit(main())
