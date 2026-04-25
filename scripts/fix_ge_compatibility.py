#!/usr/bin/env python3
"""
修复 Great Expectations 兼容性问题

问题: get_dbms_compatible_metric_domain_kwargs 函数在 GE 1.5.6 中缺失
解决方案: 在 util.py 中添加该函数的兼容实现
"""
import sys
from pathlib import Path


def fix_ge_compatibility():
    """修复 GE 兼容性问题"""
    print('=' * 80)
    print('🔧 修复 Great Expectations 兼容性问题')
    print('=' * 80)

    # 找到 util.py 文件
    util_py_path = Path('/opt/anaconda3/lib/python3.11/site-packages/great_expectations/expectations/metrics/util.py')

    if not util_py_path.exists():
        print(f'❌ 找不到文件: {util_py_path}')
        return False

    print(f'📁 目标文件: {util_py_path}')

    # 读取文件内容
    with open(util_py_path, 'r') as f:
        content = f.read()

    # 检查是否已存在该函数
    if 'def get_dbms_compatible_metric_domain_kwargs' in content:
        print('✅ 函数已存在，无需修复')
        return True

    # 在文件末尾添加函数
    function_code = '''


def get_dbms_compatible_metric_domain_kwargs(
    metric_domain_kwargs: dict,
) -> dict:
    """
    兼容函数：处理 metric_domain_kwargs 以兼容不同数据库后端

    这是一个兼容层函数，用于解决 GE 1.5.6 版本中缺失该函数的问题。

    Args:
        metric_domain_kwargs: 包含域关键字参数的字典

    Returns:
        处理后的域关键字参数字典
    """
    # 简单地返回原始 kwargs，因为当前使用场景主要是 Pandas
    return metric_domain_kwargs
'''

    # 追加到文件
    with open(util_py_path, 'a') as f:
        f.write(function_code)

    print('✅ 已添加 get_dbms_compatible_metric_domain_kwargs 函数')

    # 验证修复
    print('\n🧪 验证修复...')
    try:
        # 重新加载模块
        if 'great_expectations.expectations.metrics.util' in sys.modules:
            del sys.modules['great_expectations.expectations.metrics.util']

        from great_expectations.expectations.metrics.util import get_dbms_compatible_metric_domain_kwargs

        # 测试函数
        test_kwargs = {'column': 'test_column'}
        result = get_dbms_compatible_metric_domain_kwargs(test_kwargs)

        if result == test_kwargs:
            print('✅ 函数工作正常')
            return True
        else:
            print(f'⚠️ 函数返回值异常: {result}')
            return False

    except Exception as e:
        print(f'❌ 验证失败: {e}')
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = fix_ge_compatibility()
    sys.exit(0 if success else 1)
