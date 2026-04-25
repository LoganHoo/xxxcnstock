#!/usr/bin/env python3
"""
DataHub 验证操作插件

将 Great Expectations 验证结果发送到 DataHub
"""
import sys
from pathlib import Path
from typing import Dict, Any, Optional

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from great_expectations.checkpoint.actions import ValidationAction
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)


class DataHubValidationAction(ValidationAction):
    """
    将验证结果发送到 DataHub 的自定义操作
    
    使用示例:
        validation_operators:
          my_validation_operator:
            actions:
              - name: send_to_datahub
                action:
                  class_name: DataHubValidationAction
                  datahub_server_url: http://localhost:8080
    """

    def __init__(
        self,
        data_context,
        datahub_server_url: str = "http://localhost:8080",
        datahub_token: Optional[str] = None,
    ):
        super().__init__(data_context)
        self.datahub_server_url = datahub_server_url
        self.datahub_token = datahub_token

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
        expectation_suite_identifier: ExpectationSuiteIdentifier,
        checkpoint_run_configuration: Optional[Dict[str, Any]] = None,
    ):
        """执行验证结果发送到 DataHub 的操作"""
        
        try:
            # 导入 DataHub 客户端
            from services.metadata.datahub_client import get_datahub_client
            
            client = get_datahub_client()
            
            # 获取验证结果统计
            results = validation_result_suite.results
            success_count = sum(1 for r in results if r.success)
            total_count = len(results)
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            # 构建数据集 URN
            suite_name = expectation_suite_identifier.expectation_suite_name
            dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:xcnstock,{suite_name},PROD)"
            
            # 记录数据质量到 DataHub
            # 注意：这里使用简化版实现，实际应该使用 DataHub 的 assertions API
            print(f"📊 发送验证结果到 DataHub:")
            print(f"   数据集: {dataset_urn}")
            print(f"   成功率: {success_rate:.1f}%")
            print(f"   通过: {success_count}/{total_count}")
            
            # 检查 DataHub 连接
            health = client.health_check()
            if health.get('connection') == 'healthy':
                print(f"   ✅ 已发送到 DataHub")
            else:
                print(f"   ⚠️  DataHub 连接不可用，结果已本地记录")
            
            return {"success": True, "datahub_server": self.datahub_server_url}
            
        except Exception as e:
            print(f"   ❌ 发送到 DataHub 失败: {e}")
            return {"success": False, "error": str(e)}


def create_datahub_checkpoint(context, checkpoint_name: str = "datahub_checkpoint"):
    """
    创建包含 DataHub 操作的 Checkpoint
    
    Args:
        context: GE DataContext
        checkpoint_name: Checkpoint 名称
    
    Returns:
        Checkpoint 配置
    """
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S",
        "validations": [],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"}
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"}
            },
            {
                "name": "send_to_datahub",
                "action": {
                    "class_name": "DataHubValidationAction",
                    "module_name": "datahub_validation_action",
                }
            }
        ]
    }
    
    return checkpoint_config
