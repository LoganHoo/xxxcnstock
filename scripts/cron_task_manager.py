"""
定时任务配置管理器
从 YAML 配置文件生成 cron 任务
"""
import yaml
from pathlib import Path
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        
        schedule = task.get('schedule', '')
        script = task.get('script', '')
        description = task.get('description', '')
        enabled = task.get('enabled', True)
        
        if not enabled:
            return f"# DISABLED: {description}\n# {schedule} root cd /app && /usr/local/bin/python {script} >> /app/logs/cron.log 2>&1\n"
        
        lines = []
        lines.append(f"# {description}")
        lines.append(f"{schedule} root cd /app && /usr/local/bin/python {script} >> /app/logs/cron.log 2>&1")
        
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
            
            # 检查脚本是否存在
            script_path = Path(task.get('script', ''))
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
