#!/usr/bin/env python3
"""
断点续传管理器 (v3.0)

职责：
1. 保存采集进度到JSON文件
2. 支持断点恢复
3. 进度统计和摘要
4. 线程安全的异步保存

设计原则：
- 单一职责：只负责进度管理
- 数据完整性：使用锁保证一致性
- 容错性：文件损坏时自动重建

v3.0 改进：
- 移除对全局 config 的依赖（改为构造函数注入）
- 增强错误处理和日志记录
- 添加进度持久化策略配置
"""

import json
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class CheckpointConfig:
    """断点续传配置"""
    enabled: bool = True
    checkpoint_file: str = "data/kline/.fetch_progress.json"
    auto_save_interval: int = 10
    save_on_error: bool = True
    max_resume_age_hours: int = 24


class CheckpointManager:
    """
    断点续传管理器
    
    保存采集进度到JSON文件，支持中断后恢复
    
    使用方式：
        checkpoint = CheckpointManager(config=checkpoint_config)
        checkpoint.mark_completed('000001', result)
        await checkpoint.save()
        
        # 恢复时：
        pending = checkpoint.get_pending_codes(all_codes)
    """
    
    def __init__(self, config: CheckpointConfig = None):
        """
        初始化断点管理器
        
        Args:
            config: CheckpointConfig实例（可选）
        """
        self.config = config or CheckpointConfig()
        self.checkpoint_file = Path(self.config.checkpoint_file)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.data = self._load_or_create()
        self._save_counter = 0
        self._lock = asyncio.Lock()
    
    def _load_or_create(self) -> Dict:
        """加载现有进度或创建新的"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                saved_time = data.get('timestamp', '')
                if saved_time:
                    try:
                        saved_dt = datetime.fromisoformat(saved_time)
                        age_hours = (datetime.now() - saved_dt).total_seconds() / 3600
                        
                        if age_hours > self.config.max_resume_age_hours:
                            logger.info(f"进度文件已过期({age_hours:.1f}小时)，将重新开始")
                            return self._create_new()
                            
                    except (ValueError, TypeError) as e:
                        logger.warning(f"解析进度时间戳失败: {e}")
                
                logger.info(
                    f"加载进度文件: ✓{len(data.get('completed', []))} "
                    f"✗{len(data.get('failed', []))} "
                    f"⊘{len(data.get('skipped', []))}"
                )
                return data
                
            except json.JSONDecodeError as e:
                logger.error(f"进度文件JSON格式错误: {e}，将重新开始")
            except Exception as e:
                logger.warning(f"加载进度文件失败: {e}，将重新开始")
        
        return self._create_new()
    
    def _create_new(self) -> Dict:
        """创建新的进度数据结构"""
        return {
            'version': '3.0',
            'task_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'timestamp': datetime.now().isoformat(),
            'total': 0,
            'completed': [],
            'failed': [],
            'skipped': [],
            'delisted': [],
            'in_progress': None,
            'results': {},
            'stats': {
                'success_count': 0,
                'failed_count': 0,
                'skipped_count': 0,
                'delisted_count': 0,
                'total_rows': 0
            }
        }
    
    async def save(self, force: bool = False) -> bool:
        """
        异步保存进度
        
        Args:
            force: 是否强制保存（忽略自动保存间隔）
            
        Returns:
            是否保存成功
        """
        self._save_counter += 1
        
        if not force and self._save_counter % self.config.auto_save_interval != 0:
            return True
        
        async with self._lock:
            try:
                self.data['timestamp'] = datetime.now().isoformat()
                
                with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(self.data, f, indent=2, ensure_ascii=False, default=str)
                
                logger.debug(
                    f"进度已保存: ✓{len(self.data['completed'])} "
                    f"✗{len(self.data['failed'])} "
                    f"⊘{len(self.data['skipped'])}"
                )
                return True
                
            except Exception as e:
                logger.error(f"保存进度失败: {e}")
                return False
    
    def mark_completed(self, code: str, result: Dict = None):
        """标记股票采集完成"""
        if code not in self.data['completed']:
            self.data['completed'].append(code)
            self.data['stats']['success_count'] += 1
            
            if result:
                self.data['results'][code] = result
                self.data['stats']['total_rows'] += result.get('rows', 0)
    
    def mark_failed(self, code: str, error: str = ''):
        """标记股票采集失败"""
        if code not in self.data['failed']:
            self.data['failed'].append(code)
            self.data['stats']['failed_count'] += 1
            self.data['results'][code] = {'success': False, 'error': error}
    
    def mark_skipped(self, code: str, reason: str = 'fresh'):
        """标记股票被跳过"""
        if code not in self.data['skipped']:
            self.data['skipped'].append(code)
            self.data['stats']['skipped_count'] += 1
            self.data['results'][code] = {'success': True, 'status': f'skipped_{reason}'}
    
    def mark_delisted(self, code: str, reason: str = 'delisted'):
        """标记为退市股票"""
        if code not in self.data['delisted']:
            self.data['delisted'].append(code)
            self.data['stats']['delisted_count'] += 1
            self.data['results'][code] = {'success': False, 'status': f'delisted_{reason}'}
            
            if code not in self.data['skipped']:
                self.mark_skipped(code, reason)
    
    def set_in_progress(self, code: str = None):
        """设置当前正在处理的股票"""
        self.data['in_progress'] = code
    
    def get_pending_codes(self, all_codes: List[str]) -> List[str]:
        """
        获取待处理的股票代码列表
        
        Args:
            all_codes: 所有股票代码
            
        Returns:
            未处理的代码列表
        """
        completed_set = set(self.data.get('completed', []))
        failed_set = set(self.data.get('failed', []))
        skipped_set = set(self.data.get('skipped', []))
        
        processed = completed_set | failed_set | skipped_set
        pending = [c for c in all_codes if c not in processed]
        
        logger.info(
            f"断点恢复: 总计{len(all_codes)}只, "
            f"已完成{len(completed_set)}, 失败{len(failed_set)}, "
            f"跳过{len(skipped_set)}, 待处理{len(pending)}只"
        )
        
        return pending
    
    def is_completed(self, code: str) -> bool:
        """检查股票是否已完成"""
        return code in self.data.get('completed', [])
    
    def is_failed(self, code: str) -> bool:
        """检查股票是否失败"""
        return code in self.data.get('failed', [])
    
    def get_summary(self) -> Dict:
        """获取进度摘要"""
        stats = self.data.get('stats', {})
        total = self.data.get('total', 0)
        
        done = stats.get('success_count', 0) + stats.get('skipped_count', 0)
        failed = stats.get('failed_count', 0)
        delisted = stats.get('delisted_count', 0)
        
        return {
            'task_id': self.data.get('task_id', ''),
            'total': total,
            'completed': stats.get('success_count', 0),
            'skipped': stats.get('skipped_count', 0),
            'failed': failed,
            'delisted': delisted,
            'pending': max(0, total - done - failed),
            'progress_pct': round(done / total * 100, 1) if total > 0 else 0,
            'total_rows': stats.get('total_rows', 0),
            'last_update': self.data.get('timestamp', ''),
            'checkpoint_file': str(self.checkpoint_file)
        }
    
    def clear(self):
        """清除所有进度数据"""
        self.data = self._create_new()
        
        if self.checkpoint_file.exists():
            try:
                self.checkpoint_file.unlink()
                logger.info("进度文件已清除")
            except Exception as e:
                logger.error(f"删除进度文件失败: {e}")
    
    def get_failed_codes(self) -> List[str]:
        """获取失败的代码列表（用于重试）"""
        return list(self.data.get('failed', []))
    
    def get_result(self, code: str) -> Optional[Dict]:
        """获取单只股票的采集结果"""
        return self.data.get('results', {}).get(code)
