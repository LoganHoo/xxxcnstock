"""
定时任务配置管理器
从 YAML 配置文件生成 cron 任务
支持 Redis 锁和重试机制
"""
import yaml
import os
import redis
import time
import subprocess
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskLock:
    """Redis 任务锁"""
    
    def __init__(self, task_name: str, timeout: int = 3600):
        self.task_name = task_name
        self.timeout = timeout
        self.lock_key = f"cron:lock:{task_name}"
        self._redis = None
        self._connected = False
    
    def _get_redis(self):
        """获取 Redis 连接"""
        if self._redis is None:
            try:
                redis_host = os.getenv('REDIS_HOST', '49.233.10.199')
                redis_port = int(os.getenv('REDIS_PORT', '6379'))
                redis_password = os.getenv('REDIS_PASSWORD', '100200')
                self._redis = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    db=0,
                    socket_timeout=5,
                    decode_responses=True
                )
                self._redis.ping()
                self._connected = True
            except Exception as e:
                logger.warning(f"Redis 连接失败: {e}，将跳过锁检查")
                self._connected = False
        return self._redis
    
    def acquire(self) -> bool:
        """获取锁"""
        if not self._get_redis() or not self._connected:
            logger.info(f"Redis 未连接，跳过锁检查，执行任务: {self.task_name}")
            return True
        
        try:
            result = self._redis.set(
                self.lock_key,
                "1",
                nx=True,
                ex=self.timeout
            )
            if result:
                logger.info(f"获取锁成功: {self.task_name}")
                return True
            else:
                logger.warning(f"任务正在执行中，跳过: {self.task_name}")
                return False
        except Exception as e:
            logger.warning(f"获取锁失败: {e}，继续执行任务")
            return True
    
    def release(self):
        """释放锁"""
        if self._redis and self._connected:
            try:
                self._redis.delete(self.lock_key)
                logger.info(f"释放锁: {self.task_name}")
            except Exception as e:
                logger.warning(f"释放锁失败: {e}")


class TaskExecutor:
    """任务执行器 - 支持重试和日志"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.global_config = self.config.get('global', {})
        self.retry_enabled = self.global_config.get('retry_enabled', True)
        self.max_retries = self.global_config.get('max_retries', 3)
        self.retry_delay = self.global_config.get('retry_delay', 60)
        self.log_file = self.global_config.get('log_file', '/app/logs/cron.log')
        self._redis = None
    
    def _get_redis(self):
        """获取 Redis 连接"""
        if self._redis is None:
            try:
                import redis
                redis_host = os.getenv('REDIS_HOST', '49.233.10.199')
                redis_port = int(os.getenv('REDIS_PORT', '6379'))
                redis_password = os.getenv('REDIS_PASSWORD', '100200')
                self._redis = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    db=0,
                    socket_timeout=5,
                    decode_responses=True
                )
                self._redis.ping()
            except Exception as e:
                print(f"Redis 连接失败: {e}")
                self._redis = None
        return self._redis
    
    def _check_task_passed(self, task_name: str) -> bool:
        """检查指定任务是否已通过（质检通过）"""
        r = self._get_redis()
        if not r:
            return False
        
        try:
            status = r.get(f"task:status:{task_name}")
            return status == "passed"
        except:
            return False
    
    def _set_task_status(self, task_name: str, status: str):
        """设置任务状态"""
        r = self._get_redis()
        if not r:
            return
        
        try:
            r.set(f"task:status:{task_name}", status, ex=86400)
        except:
            pass
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _log(self, message: str):
        """写入日志"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{timestamp} - {message}\n"
        
        log_dir = Path(self.log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_message)
        except Exception as e:
            print(f"日志写入失败: {e}")
        print(log_message.strip())
    
    def _run_script(self, script: str, env: dict = None, timeout: int = None) -> int:
        """执行脚本"""
        cmd = [sys.executable, script]

        process_env = os.environ.copy()
        if env:
            process_env.update(env)

        process_env['PYTHONUNBUFFERED'] = '1'
        process_env['TZ'] = 'Asia/Shanghai'

        result = subprocess.run(
            cmd,
            env=process_env,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.stdout:
            self._log(result.stdout)
        if result.stderr:
            self._log(f"STDERR: {result.stderr}")
        
        return result.returncode
    
    def execute_task(self, task: dict) -> bool:
        """执行单个任务"""
        task_name = task.get('name', 'unknown')
        script = task.get('script', '')
        timeout = task.get('timeout', 600)
        task_env = task.get('env', {})
        skip_if_passed = task.get('skip_if_passed')
        
        if skip_if_passed:
            if self._check_task_passed(skip_if_passed):
                self._log(f"任务跳过（{skip_if_passed} 已通过）: {task_name}")
                return True
        
        use_lock = self.global_config.get('use_redis_lock', True)
        lock_timeout = self.global_config.get('redis_lock_timeout', 3600)
        
        lock = TaskLock(task_name, lock_timeout) if use_lock else None
        
        if lock and not lock.acquire():
            self._log(f"任务跳过（锁未获取）: {task_name}")
            return False
        
        try:
            self._log(f"开始执行任务: {task_name}")
            
            retries = 0
            while retries <= self.max_retries:
                returncode = self._run_script(script, task_env, timeout)
                
                if returncode == 0:
                    self._log(f"任务执行成功: {task_name}")
                    self._set_task_status(task_name, "passed")
                    
                    if task_name == "data_quality_check":
                        self._set_task_status("data_quality_check", "passed")
                    
                    return True
                
                retries += 1
                if retries <= self.max_retries:
                    self._log(f"任务执行失败 (尝试 {retries}/{self.max_retries})，{self.retry_delay}秒后重试: {task_name}")
                    time.sleep(self.retry_delay)
                else:
                    self._log(f"任务执行失败（已重试 {self.max_retries} 次）: {task_name}")
                    self._set_task_status(task_name, "failed")
                    return False
            
            return False
        finally:
            if lock:
                lock.release()
    
    def execute_with_dependencies(self, task: dict, completed_tasks: set) -> bool:
        """执行任务并检查依赖"""
        task_name = task.get('name', 'unknown')
        depends_on = task.get('depends_on', [])
        
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        
        for dep in depends_on:
            if dep not in completed_tasks:
                logger.warning(f"依赖任务未完成: {task_name} -> {dep}")
                return False
        
        return self.execute_task(task)


class CronTaskManager:
    """定时任务管理器"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def generate_cron_entry(self, task: dict) -> str:
        """生成单个 cron 条目"""
        global_config = self.config.get('global', {})
        env_config = self.config.get('environment', {})

        schedule = task.get('schedule', '')
        script = task.get('script', '')
        description = task.get('description', '')
        enabled = task.get('enabled', True)

        if not enabled:
            return f"# DISABLED: {description}\n# {schedule} cd /app && /usr/local/bin/python {script} >> /app/logs/cron.log 2>&1\n"

        env_exports = []
        for key, value in env_config.items():
            env_exports.append(f"export {key}=\"{value}\"")

        env_block = '\n'.join(env_exports) + '\n' if env_exports else ''

        lines = []
        lines.append(f"# {description}")
        lines.append(f"{schedule} cd /app && {env_block}/usr/local/bin/python {script} >> /app/logs/cron.log 2>&1")

        return '\n'.join(lines) + '\n'
    
    def generate_cron_file(self) -> str:
        """生成完整的 cron 配置文件"""
        global_config = self.config.get('global', {})
        tasks = self.config.get('tasks', [])
        
        lines = []
        
        # 头部配置
        lines.append(f"# XCNStock 定时任务配置")
        lines.append(f"# 由 cron_tasks.yaml 自动生成")
        lines.append(f"SHELL={global_config.get('shell', '/bin/bash')}")
        lines.append(f"PATH={global_config.get('path', '/usr/local/bin:/usr/bin:/bin')}")
        lines.append("")
        
        # 按类型分组任务
        daily_tasks = [t for t in tasks if t.get('day_type') == 'daily']
        weekday_tasks = [t for t in tasks if t.get('day_type') == 'weekday']
        
        # 每日任务
        if daily_tasks:
            lines.append("# " + "=" * 50)
            lines.append("# 每日任务")
            lines.append("# " + "=" * 50)
            for task in daily_tasks:
                lines.append(self.generate_cron_entry(task))
            lines.append("")
        
        # 交易日任务
        if weekday_tasks:
            lines.append("# " + "=" * 50)
            lines.append("# 交易日任务 (周一至周五)")
            lines.append("# " + "=" * 50)
            for task in weekday_tasks:
                lines.append(self.generate_cron_entry(task))
            lines.append("")
        
        return '\n'.join(lines)
    
    def get_task_by_name(self, name: str) -> Optional[dict]:
        """根据名称获取任务"""
        tasks = self.config.get('tasks', [])
        for task in tasks:
            if task.get('name') == name:
                return task
        return None
    
    def get_tasks_by_group(self, group_name: str) -> List[dict]:
        """根据分组获取任务"""
        groups = self.config.get('groups', [])
        tasks = self.config.get('tasks', [])
        
        for group in groups:
            if group.get('name') == group_name:
                task_names = group.get('tasks', [])
                return [t for t in tasks if t.get('name') in task_names]
        return []
    
    def enable_task(self, task_name: str) -> bool:
        """启用任务"""
        tasks = self.config.get('tasks', [])
        for task in tasks:
            if task.get('name') == task_name:
                task['enabled'] = True
                self._save_config()
                logger.info(f"已启用任务: {task_name}")
                return True
        return False
    
    def disable_task(self, task_name: str) -> bool:
        """禁用任务"""
        tasks = self.config.get('tasks', [])
        for task in tasks:
            if task.get('name') == task_name:
                task['enabled'] = False
                self._save_config()
                logger.info(f"已禁用任务: {task_name}")
                return True
        return False
    
    def _save_config(self):
        """保存配置文件"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.config, f, allow_unicode=True, default_flow_style=False)
    
    def list_tasks(self) -> List[Dict]:
        """列出所有任务"""
        tasks = self.config.get('tasks', [])
        result = []
        for task in tasks:
            result.append({
                'name': task.get('name'),
                'description': task.get('description'),
                'schedule': task.get('schedule'),
                'script': task.get('script'),
                'enabled': task.get('enabled', True),
                'day_type': task.get('day_type', 'custom')
            })
        return result
    
    def validate_tasks(self) -> Dict[str, List[str]]:
        """验证任务配置"""
        errors = {}
        warnings = {}
        tasks = self.config.get('tasks', [])
        
        for task in tasks:
            task_name = task.get('name', 'unknown')
            task_errors = []
            task_warnings = []
            
            # 检查必要字段
            if not task.get('schedule'):
                task_errors.append("缺少 schedule 字段")
            if not task.get('script'):
                task_errors.append("缺少 script 字段")
            if not task.get('description'):
                task_warnings.append("缺少 description 字段")
            
            # 检查脚本是否存在（支持带参数如 --retry）
            script_path_str = task.get('script', '').split()[0] if task.get('script') else ''
            script_path = Path(script_path_str)
            if not script_path.exists():
                task_warnings.append(f"脚本文件不存在: {script_path}")
            
            if task_errors:
                errors[task_name] = task_errors
            if task_warnings:
                warnings[task_name] = task_warnings
        
        return {'errors': errors, 'warnings': warnings}


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='定时任务配置管理器')
    parser.add_argument('command', choices=['generate', 'list', 'validate', 'enable', 'disable'],
                       help='执行命令')
    parser.add_argument('--config', '-c', default='config/cron_tasks.yaml',
                       help='配置文件路径')
    parser.add_argument('--task', '-t', help='任务名称')
    parser.add_argument('--output', '-o', help='输出文件路径')
    
    args = parser.parse_args()
    
    manager = CronTaskManager(args.config)
    
    if args.command == 'generate':
        cron_content = manager.generate_cron_file()
        if args.output:
            with open(args.output, 'w') as f:
                f.write(cron_content)
            print(f"Cron 配置已保存到: {args.output}")
        else:
            print(cron_content)
    
    elif args.command == 'list':
        print("=" * 70)
        print("定时任务列表")
        print("=" * 70)
        for task in manager.list_tasks():
            status = "✅" if task['enabled'] else "❌"
            print(f"\n{status} {task['name']}")
            print(f"   描述: {task['description']}")
            print(f"   时间: {task['schedule']}")
            print(f"   脚本: {task['script']}")
            print(f"   类型: {task['day_type']}")
    
    elif args.command == 'validate':
        result = manager.validate_tasks()
        print("=" * 70)
        print("配置验证结果")
        print("=" * 70)
        
        if result['errors']:
            print("\n❌ 错误:")
            for task_name, errs in result['errors'].items():
                print(f"  {task_name}:")
                for err in errs:
                    print(f"    - {err}")
        
        if result['warnings']:
            print("\n⚠️ 警告:")
            for task_name, warns in result['warnings'].items():
                print(f"  {task_name}:")
                for warn in warns:
                    print(f"    - {warn}")
        
        if not result['errors'] and not result['warnings']:
            print("\n✅ 所有任务配置正确")
    
    elif args.command == 'enable':
        if not args.task:
            print("请指定任务名称: --task <name>")
            return
        if manager.enable_task(args.task):
            print(f"✅ 已启用任务: {args.task}")
        else:
            print(f"❌ 未找到任务: {args.task}")
    
    elif args.command == 'disable':
        if not args.task:
            print("请指定任务名称: --task <name>")
            return
        if manager.disable_task(args.task):
            print(f"✅ 已禁用任务: {args.task}")
        else:
            print(f"❌ 未找到任务: {args.task}")


if __name__ == '__main__':
    main()
