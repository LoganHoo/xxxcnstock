"""
报告模板基类

提供统一的报告模板接口和通用格式化方法
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime


class BaseReportTemplate(ABC):
    """报告模板基类"""

    # 模板配置
    LINE_WIDTH = 70
    SECTION_SEPARATOR = "=" * LINE_WIDTH
    SUB_SECTION_SEPARATOR = "-" * 50

    def __init__(self):
        """初始化模板"""
        self.report_time = datetime.now()

    @abstractmethod
    def generate(self, **kwargs) -> str:
        """
        生成报告内容

        Args:
            **kwargs: 报告数据

        Returns:
            str: 报告文本
        """
        pass

    # ========== 格式化工具方法 ==========
    def format_header(self, title: str, subtitle: str = "") -> str:
        """
        格式化报告头部

        Args:
            title: 标题
            subtitle: 副标题

        Returns:
            str
        """
        lines = [
            self.SECTION_SEPARATOR,
            f"【{title}】",
        ]
        if subtitle:
            lines.append(subtitle)
        lines.append(f"生成时间: {self.report_time.strftime('%Y-%m-%d %H:%M')}")
        lines.append(self.SECTION_SEPARATOR)
        return "\n".join(lines) + "\n"

    def format_section(self, title: str, content: str = "") -> str:
        """
        格式化章节

        Args:
            title: 章节标题
            content: 章节内容

        Returns:
            str
        """
        lines = [
            f"\n【{title}】",
            self.SUB_SECTION_SEPARATOR,
        ]
        if content:
            lines.append(content)
        return "\n".join(lines) + "\n"

    def format_sub_section(self, title: str, content: str = "") -> str:
        """
        格式化子章节

        Args:
            title: 子章节标题
            content: 子章节内容

        Returns:
            str
        """
        lines = [f"\n{title}"]
        if content:
            lines.append(content)
        return "\n".join(lines) + "\n"

    def format_placeholder(self, title: str, message: str = "数据暂不可用") -> str:
        """
        格式化占位符（数据缺失时）

        Args:
            title: 标题
            message: 提示消息

        Returns:
            str
        """
        return f"""
【{title}】
{self.SUB_SECTION_SEPARATOR}
  ⚠️ {message}
"""

    def format_list_item(self, index: int, content: str, indent: int = 2) -> str:
        """
        格式化列表项

        Args:
            index: 序号
            content: 内容
            indent: 缩进空格数

        Returns:
            str
        """
        return " " * indent + f"{index}. {content}"

    def format_bullet_item(self, content: str, indent: int = 2, bullet: str = "●") -> str:
        """
        格式化项目符号项

        Args:
            content: 内容
            indent: 缩进空格数
            bullet: 项目符号

        Returns:
            str
        """
        return " " * indent + f"{bullet} {content}"

    def format_key_value(self, key: str, value: Any, indent: int = 2) -> str:
        """
        格式化键值对

        Args:
            key: 键
            value: 值
            indent: 缩进空格数

        Returns:
            str
        """
        return " " * indent + f"{key}: {value}"

    def format_stock_item(self, code: str, name: str, change_pct: float,
                         extra_info: str = "", indent: int = 2) -> str:
        """
        格式化股票项

        Args:
            code: 股票代码
            name: 股票名称
            change_pct: 涨跌幅
            extra_info: 额外信息
            indent: 缩进空格数

        Returns:
            str
        """
        change_sign = "+" if change_pct > 0 else ""
        lines = [
            " " * indent + f"{code} {name}: {change_sign}{change_pct:.2f}%"
        ]
        if extra_info:
            lines.append(" " * (indent + 2) + extra_info)
        return "\n".join(lines)

    def format_footer(self, message: str = "报告结束") -> str:
        """
        格式化报告尾部

        Args:
            message: 结束消息

        Returns:
            str
        """
        return f"\n{self.SECTION_SEPARATOR}\n【{message}】\n{self.SECTION_SEPARATOR}\n"

    # ========== 数据验证工具 ==========
    def validate_data(self, data: Any, required_fields: List[str]) -> bool:
        """
        验证数据是否包含必需字段

        Args:
            data: 数据字典
            required_fields: 必需字段列表

        Returns:
            bool
        """
        if not isinstance(data, dict):
            return False
        return all(field in data for field in required_fields)

    def safe_get(self, data: Dict, key: str, default: Any = None) -> Any:
        """
        安全获取字典值

        Args:
            data: 字典
            key: 键
            default: 默认值

        Returns:
            Any
        """
        if not isinstance(data, dict):
            return default
        return data.get(key, default)

    def safe_get_nested(self, data: Dict, *keys: str, default: Any = None) -> Any:
        """
        安全获取嵌套字典值

        Args:
            data: 字典
            *keys: 键路径
            default: 默认值

        Returns:
            Any
        """
        current = data
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current

    # ========== 数值格式化 ==========
    @staticmethod
    def format_percentage(value: float, decimal_places: int = 2) -> str:
        """
        格式化百分比

        Args:
            value: 数值
            decimal_places: 小数位数

        Returns:
            str
        """
        sign = "+" if value > 0 else ""
        return f"{sign}{value:.{decimal_places}f}%"

    @staticmethod
    def format_number(value: float, decimal_places: int = 2) -> str:
        """
        格式化数字

        Args:
            value: 数值
            decimal_places: 小数位数

        Returns:
            str
        """
        return f"{value:.{decimal_places}f}"

    @staticmethod
    def format_volume(value: float, unit: str = "亿") -> str:
        """
        格式化成交量

        Args:
            value: 数值
            unit: 单位

        Returns:
            str
        """
        return f"{value:.2f}{unit}"


class DataSourceMixin:
    """数据源Mixin - 提供统一的数据加载接口"""

    def __init__(self):
        self._data_cache = {}

    def get_data(self, data_type: str, **kwargs) -> Any:
        """
        获取数据

        Args:
            data_type: 数据类型
            **kwargs: 额外参数

        Returns:
            Any
        """
        # 子类可以重写此方法
        raise NotImplementedError(f"数据类型 '{data_type}' 未实现")

    def clear_cache(self):
        """清除数据缓存"""
        self._data_cache.clear()
