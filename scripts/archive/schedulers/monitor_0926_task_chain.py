#!/usr/bin/env python3
"""
09:26任务链监控脚本
================================================================================
监控内容：
1. 任务执行时间和成功率
2. 资源保障各阶段耗时
3. 熔断器状态变化
4. 生成监控报告
================================================================================
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List

project_root = Path(__file__).parent.parent


class TaskChainMonitor:
    """任务链监控器"""
    
    def __init__(self):
        self.project_root = project_root
        self.log_dir = project_root / "logs"
        self.report_file = self.log_dir / "0926_task_chain_monitor.json"
        
    def read_log_file(self, log_file: Path, lines: int = 100) -> List[str]:
        """读取日志文件最后N行"""
        if not log_file.exists():
            return []
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                return all_lines[-lines:] if len(all_lines) > lines else all_lines
        except:
            return []
    
    def parse_resource_guardian_log(self) -> Dict:
        """解析资源保障日志"""
        log_file = self.log_dir / "system" / "fund_behavior_resource_guardian.log"
        lines = self.read_log_file(log_file, 200)
        
        phases = {
            "prepare": {"found": False, "start_time": None, "end_time": None, "duration": 0},
            "validate": {"found": False, "start_time": None, "end_time": None, "duration": 0}
        }
        
        for line in lines:
            # 解析prepare阶段
            if "执行阶段: 数据准备" in line:
                phases["prepare"]["found"] = True
                # 提取时间
                try:
                    time_str = line.split('|')[0].strip()
                    phases["prepare"]["start_time"] = time_str
                except:
                    pass
            elif "准备阶段结果:" in line and phases["prepare"]["found"]:
                try:
                    time_str = line.split('|')[0].strip()
                    phases["prepare"]["end_time"] = time_str
                except:
                    pass
            
            # 解析validate阶段
            elif "执行阶段: 资源验证" in line:
                phases["validate"]["found"] = True
                try:
                    time_str = line.split('|')[0].strip()
                    phases["validate"]["start_time"] = time_str
                except:
                    pass
            elif "验证阶段结果:" in line and phases["validate"]["found"]:
                try:
                    time_str = line.split('|')[0].strip()
                    phases["validate"]["end_time"] = time_str
                except:
                    pass
        
        return phases
    
    def parse_core_guardian_log(self) -> Dict:
        """解析核心守护程序日志"""
        log_file = self.log_dir / "system" / "core_task_guardian.log"
        lines = self.read_log_file(log_file, 100)
        
        status = {
            "last_check": None,
            "last_full": None,
            "circuit_breaker_opens": 0,
            "success_count": 0,
            "failure_count": 0
        }
        
        for line in lines:
            if "核心任务前置检查" in line:
                try:
                    status["last_check"] = line.split('|')[0].strip()
                except:
                    pass
            elif "核心任务守护程序启动" in line:
                try:
                    status["last_full"] = line.split('|')[0].strip()
                except:
                    pass
            elif "熔断器打开" in line:
                status["circuit_breaker_opens"] += 1
            elif "任务成功完成" in line:
                status["success_count"] += 1
            elif "任务失败" in line:
                status["failure_count"] += 1
        
        return status
    
    def read_circuit_breaker_state(self) -> Dict:
        """读取熔断器状态"""
        state_file = self.project_root / "data" / ".circuit_breaker_state.json"
        
        if not state_file.exists():
            return {"is_open": False, "failure_count": 0, "status": "无状态文件"}
        
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                return {
                    "is_open": state.get("is_open", False),
                    "failure_count": state.get("failure_count", 0),
                    "threshold": state.get("threshold", 3),
                    "last_failure_time": state.get("last_failure_time"),
                    "updated_at": state.get("updated_at")
                }
        except:
            return {"is_open": False, "failure_count": 0, "status": "读取失败"}
    
    def read_test_report(self) -> Dict:
        """读取测试报告"""
        report_file = self.log_dir / "test_0926_task_chain_report.json"
        
        if not report_file.exists():
            return {"status": "无测试报告"}
        
        try:
            with open(report_file, 'r') as f:
                return json.load(f)
        except:
            return {"status": "读取失败"}
    
    def generate_monitor_report(self) -> Dict:
        """生成监控报告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "resource_guardian": self.parse_resource_guardian_log(),
            "core_guardian": self.parse_core_guardian_log(),
            "circuit_breaker": self.read_circuit_breaker_state(),
            "last_test": self.read_test_report()
        }
        
        # 保存报告
        self.report_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        return report
    
    def print_report(self, report: Dict):
        """打印监控报告"""
        print("\n" + "="*70)
        print("09:26任务链监控报告")
        print("="*70)
        print(f"报告时间: {report['timestamp']}")
        print(f"报告日期: {report['date']}")
        
        # 资源保障状态
        print("\n" + "-"*70)
        print("资源保障状态")
        print("-"*70)
        
        rg = report.get("resource_guardian", {})
        for phase_name, phase_info in rg.items():
            if phase_info.get("found"):
                status = "✅ 已执行"
                print(f"  {phase_name}: {status}")
                if phase_info.get("start_time"):
                    print(f"    开始: {phase_info['start_time']}")
                if phase_info.get("end_time"):
                    print(f"    结束: {phase_info['end_time']}")
            else:
                print(f"  {phase_name}: ⏳ 未执行")
        
        # 核心守护程序状态
        print("\n" + "-"*70)
        print("核心守护程序状态")
        print("-"*70)
        
        cg = report.get("core_guardian", {})
        print(f"  上次检查: {cg.get('last_check') or '无记录'}")
        print(f"  上次执行: {cg.get('last_full') or '无记录'}")
        print(f"  成功次数: {cg.get('success_count', 0)}")
        print(f"  失败次数: {cg.get('failure_count', 0)}")
        
        # 熔断器状态
        print("\n" + "-"*70)
        print("熔断器状态")
        print("-"*70)
        
        cb = report.get("circuit_breaker", {})
        status = "🔴 打开" if cb.get("is_open") else "🟢 关闭"
        print(f"  状态: {status}")
        print(f"  失败次数: {cb.get('failure_count', 0)}/{cb.get('threshold', 3)}")
        if cb.get("last_failure_time"):
            print(f"  上次失败: {cb['last_failure_time']}")
        
        # 上次测试
        print("\n" + "-"*70)
        print("上次测试结果")
        print("-"*70)
        
        test = report.get("last_test", {})
        if test.get("status"):
            print(f"  状态: {test['status']}")
        else:
            summary = test.get("summary", {})
            total = summary.get("total", 0)
            passed = summary.get("passed", 0)
            print(f"  测试项: {passed}/{total} 通过")
            if total > 0:
                print(f"  通过率: {passed/total*100:.1f}%")
        
        print("\n" + "="*70)
        print(f"完整报告: {self.report_file}")
        print("="*70)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='09:26任务链监控')
    parser.add_argument('--watch', action='store_true', help='持续监控模式')
    parser.add_argument('--interval', type=int, default=60, help='监控间隔(秒)')
    
    args = parser.parse_args()
    
    monitor = TaskChainMonitor()
    
    if args.watch:
        print(f"开始持续监控 (间隔: {args.interval}秒)...")
        print("按 Ctrl+C 停止")
        try:
            while True:
                report = monitor.generate_monitor_report()
                monitor.print_report(report)
                print(f"\n下次更新: {args.interval}秒后...")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n监控已停止")
    else:
        report = monitor.generate_monitor_report()
        monitor.print_report(report)


if __name__ == "__main__":
    main()
