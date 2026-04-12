"""
报告配置基类
"""
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List


class ReportConfig:
    """报告配置加载器"""

    CONFIG_DIR = Path(__file__).parent / "configs"

    @classmethod
    def load_config(cls, report_name: str) -> Optional[Dict]:
        """加载报告配置"""
        config_path = cls.CONFIG_DIR / f"{report_name}.json"
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    @classmethod
    def get_all_configs(cls) -> List[str]:
        """获取所有报告配置名称"""
        configs = []
        if cls.CONFIG_DIR.exists():
            for f in cls.CONFIG_DIR.glob("*.json"):
                configs.append(f.stem)
        return configs


class BaseReportTemplate:
    """报告模板基类"""

    def __init__(self, config_name: str):
        self.config = ReportConfig.load_config(config_name)
        self.name = self.config.get('name', config_name) if self.config else config_name
        self.display_name = self.config.get('display_name', config_name) if self.config else config_name

    def get_subject(self, date: str = None) -> str:
        """生成邮件主题"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        template = self.config.get('subject_template', '{name} - {date}')
        return template.format(name=self.display_name, date=date)

    def is_section_enabled(self, key: str) -> bool:
        """检查章节是否启用"""
        if not self.config:
            return True
        for section in self.config.get('sections', []):
            if section.get('key') == key:
                return section.get('enabled', True)
        return True

    def get_section_config(self, key: str) -> Optional[Dict]:
        """获取章节配置"""
        if not self.config:
            return None
        for section in self.config.get('sections', []):
            if section.get('key') == key:
                return section
        return None

    def _get_date_str(self) -> str:
        """获取日期字符串"""
        return datetime.now().strftime('%Y-%m-%d')

    def _arrow(self, change: float) -> str:
        """返回箭头符号"""
        if change > 0:
            return "▲"
        elif change < 0:
            return "▼"
        return "─"

    def _safe_float(self, value, default: float = 0.0) -> float:
        """安全获取浮点数"""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def _format_change(self, change: float) -> str:
        """格式化涨跌幅"""
        arrow = self._arrow(change)
        return f"{arrow}{abs(change):.2f}%"

    def generate(self, **kwargs) -> str:
        """生成报告内容 - 子类需要重写"""
        return ""

    def safe_generate(self, **kwargs) -> str:
        """安全生成报告内容，带错误处理"""
        try:
            return self.generate(**kwargs)
        except Exception as e:
            return self._generate_fallback_error(str(e))

    def _generate_fallback_error(self, error_msg: str = "") -> str:
        """生成降级错误报告"""
        lines = []
        lines.append("=" * 50)
        lines.append(f"【{self.display_name}】")
        lines.append("=" * 50)
        lines.append("")
        lines.append("⚠️ 报告生成失败")
        if error_msg:
            lines.append(f"错误信息: {error_msg}")
        lines.append("")
        lines.append("请稍后重试或联系管理员")
        lines.append("=" * 50)
        return "\n".join(lines)