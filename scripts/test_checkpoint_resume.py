#!/usr/bin/env python3
"""
K线数据采集断点续传测试

测试内容:
1. 进度文件创建和保存
2. 中断后从断点恢复
3. 已完成的股票跳过
4. 部分完成的股票继续
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path

from core.paths import DATA_DIR
from core.logger import setup_logger

logger = setup_logger("test_checkpoint", log_file="system/test_checkpoint.log")

# 测试配置
TEST_CODES = ['600000', '000001', '300001', '688001', '000002']
CHECKPOINT_FILE = DATA_DIR / 'test_checkpoint.json'


class CheckpointManager:
    """断点续传管理器（测试版本）"""
    
    def __init__(self, checkpoint_file: Path):
        self.checkpoint_file = checkpoint_file
        self.progress = {
            'total': 0,
            'completed': [],
            'failed': [],
            'last_update': None
        }
        self._load()
    
    def _load(self):
        """加载进度"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    self.progress = json.load(f)
                logger.info(f"加载进度文件: {self.checkpoint_file}")
            except Exception as e:
                logger.error(f"加载进度文件失败: {e}")
    
    def save(self):
        """保存进度"""
        self.progress['last_update'] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
            logger.info(f"保存进度文件: {len(self.progress['completed'])}/{self.progress['total']} 完成")
        except Exception as e:
            logger.error(f"保存进度文件失败: {e}")
    
    def init_progress(self, codes: list):
        """初始化进度"""
        self.progress['total'] = len(codes)
        self.progress['completed'] = []
        self.progress['failed'] = []
        self.save()
    
    def mark_completed(self, code: str):
        """标记完成"""
        if code not in self.progress['completed']:
            self.progress['completed'].append(code)
            self.save()
    
    def mark_failed(self, code: str):
        """标记失败"""
        if code not in self.progress['failed']:
            self.progress['failed'].append(code)
            self.save()
    
    def is_completed(self, code: str) -> bool:
        """检查是否已完成"""
        return code in self.progress['completed']
    
    def get_remaining(self, codes: list) -> list:
        """获取剩余未完成的股票"""
        return [c for c in codes if not self.is_completed(c)]
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            'total': self.progress['total'],
            'completed': len(self.progress['completed']),
            'failed': len(self.progress['failed']),
            'remaining': self.progress['total'] - len(self.progress['completed'])
        }


async def test_checkpoint_creation():
    """测试进度文件创建"""
    print("\n" + "="*70)
    print("步骤1: 进度文件创建")
    print("="*70)
    
    # 删除旧进度文件
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print(f"已删除旧进度文件")
    
    # 创建管理器
    manager = CheckpointManager(CHECKPOINT_FILE)
    manager.init_progress(TEST_CODES)
    
    # 验证文件创建
    if CHECKPOINT_FILE.exists():
        print(f"✅ 进度文件创建成功: {CHECKPOINT_FILE}")
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
            print(f"   总任务数: {data['total']}")
            print(f"   已完成: {len(data['completed'])}")
            print(f"   失败: {len(data['failed'])}")
        return True
    else:
        print("❌ 进度文件创建失败")
        return False


async def test_progress_update():
    """测试进度更新"""
    print("\n" + "="*70)
    print("步骤2: 进度更新")
    print("="*70)
    
    manager = CheckpointManager(CHECKPOINT_FILE)
    
    # 模拟完成前3只股票
    for code in TEST_CODES[:3]:
        print(f"标记完成: {code}")
        manager.mark_completed(code)
        time.sleep(0.1)  # 模拟处理时间
    
    # 模拟1只失败
    print(f"标记失败: {TEST_CODES[3]}")
    manager.mark_failed(TEST_CODES[3])
    
    # 验证进度
    stats = manager.get_stats()
    print(f"\n当前进度:")
    print(f"  总数: {stats['total']}")
    print(f"  已完成: {stats['completed']}")
    print(f"  失败: {stats['failed']}")
    print(f"  剩余: {stats['remaining']}")
    
    if stats['completed'] == 3 and stats['failed'] == 1:
        print("✅ 进度更新正确")
        return True
    else:
        print("❌ 进度更新异常")
        return False


async def test_resume_from_checkpoint():
    """测试从断点恢复"""
    print("\n" + "="*70)
    print("步骤3: 从断点恢复")
    print("="*70)
    
    # 创建新的管理器（模拟程序重启）
    manager = CheckpointManager(CHECKPOINT_FILE)
    
    # 获取剩余任务
    remaining = manager.get_remaining(TEST_CODES)
    
    print(f"原始任务: {TEST_CODES}")
    print(f"已完成: {manager.progress['completed']}")
    print(f"剩余任务: {remaining}")
    
    expected_remaining = [TEST_CODES[4]]  # 只有最后一只未完成
    
    if remaining == expected_remaining:
        print("✅ 断点恢复正确")
        
        # 完成剩余任务
        for code in remaining:
            print(f"处理剩余任务: {code}")
            manager.mark_completed(code)
        
        return True
    else:
        print(f"❌ 断点恢复异常，预期: {expected_remaining}")
        return False


async def test_skip_completed():
    """测试跳过已完成"""
    print("\n" + "="*70)
    print("步骤4: 跳过已完成任务")
    print("="*70)
    
    manager = CheckpointManager(CHECKPOINT_FILE)
    
    # 检查所有股票是否都被标记为完成
    all_completed = all(manager.is_completed(code) for code in TEST_CODES)
    
    if all_completed:
        print("✅ 所有任务已标记为完成")
        
        # 再次处理应该全部跳过
        remaining = manager.get_remaining(TEST_CODES)
        
        if len(remaining) == 0:
            print("✅ 正确跳过所有已完成任务")
            return True
        else:
            print(f"❌ 未正确跳过，剩余: {remaining}")
            return False
    else:
        print(f"❌ 并非所有任务都已完成")
        stats = manager.get_stats()
        print(f"   完成: {stats['completed']}/{stats['total']}")
        return False


async def test_checkpoint_persistence():
    """测试进度持久化"""
    print("\n" + "="*70)
    print("步骤5: 进度持久化验证")
    print("="*70)
    
    # 重新加载进度文件
    if not CHECKPOINT_FILE.exists():
        print("❌ 进度文件不存在")
        return False
    
    with open(CHECKPOINT_FILE, 'r') as f:
        data = json.load(f)
    
    print(f"进度文件内容:")
    print(f"  总任务: {data['total']}")
    print(f"  已完成: {len(data['completed'])}")
    print(f"  失败: {len(data['failed'])}")
    print(f"  最后更新: {data.get('last_update', 'N/A')}")
    
    # 验证数据完整性
    if (data['total'] == len(TEST_CODES) and 
        len(data['completed']) == len(TEST_CODES)):
        print("✅ 进度持久化正确")
        return True
    else:
        print("❌ 进度数据异常")
        return False


async def run_all_tests():
    """运行所有测试"""
    print("="*70)
    print("断点续传功能测试")
    print("="*70)
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {', '.join(TEST_CODES)}")
    print(f"进度文件: {CHECKPOINT_FILE}")
    
    results = []
    
    # 步骤1: 进度文件创建
    results.append(("进度文件创建", await test_checkpoint_creation()))
    
    # 步骤2: 进度更新
    results.append(("进度更新", await test_progress_update()))
    
    # 步骤3: 从断点恢复
    results.append(("断点恢复", await test_resume_from_checkpoint()))
    
    # 步骤4: 跳过已完成
    results.append(("跳过已完成", await test_skip_completed()))
    
    # 步骤5: 进度持久化
    results.append(("进度持久化", await test_checkpoint_persistence()))
    
    # 汇总
    print("\n" + "="*70)
    print("测试汇总")
    print("="*70)
    
    for name, success in results:
        status = "✅" if success else "❌"
        print(f"{status} {name}")
    
    passed = sum(1 for _, s in results if s)
    total = len(results)
    
    print(f"\n总计: {passed}/{total} 通过 ({passed/total*100:.1f}%)")
    print("="*70)
    
    # 清理
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print(f"\n已清理测试文件: {CHECKPOINT_FILE}")


if __name__ == "__main__":
    asyncio.run(run_all_tests())
