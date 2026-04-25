#!/usr/bin/env python3
"""
Kestra 集成测试

测试范围：
1. API 连接测试
2. 工作流部署测试
3. 工作流执行测试
4. 监控功能测试

运行：
    pytest tests/integration/test_kestra_integration.py -v
    pytest tests/integration/test_kestra_integration.py -v -k "test_connection"
"""
import os
import sys
import time
import pytest
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from kestra.lib.kestra_client import KestraClient, create_client, ExecutionStatus


class TestKestraConnection:
    """API 连接测试"""
    
    @pytest.fixture
    def client(self):
        """创建 Kestra 客户端"""
        return create_client()
    
    def test_connection(self, client):
        """测试 API 连接"""
        success, message = client.test_connection()
        print(f"\n连接测试结果: {message}")
        # 如果 Kestra 未运行，跳过测试
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")
        assert success, f"连接失败: {message}"
    
    def test_authentication(self, client):
        """测试认证"""
        success, message = client.test_connection()
        if not success and "认证失败" in message:
            pytest.fail(f"认证失败: {message}")
        # 其他情况可能是服务未运行，跳过
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")


class TestKestraDeployment:
    """工作流部署测试"""
    
    @pytest.fixture
    def client(self):
        """创建 Kestra 客户端"""
        return create_client()
    
    @pytest.fixture
    def test_flow_file(self, tmp_path):
        """创建测试工作流文件"""
        flow_content = """
id: test_integration_flow
namespace: xcnstock

description: 集成测试用工作流

tasks:
  - id: hello
    type: io.kestra.plugin.core.log.Log
    message: "Hello from integration test"
"""
        flow_file = tmp_path / "test_flow.yml"
        flow_file.write_text(flow_content)
        return flow_file
    
    def test_validate_yaml(self, client, test_flow_file):
        """测试 YAML 验证"""
        # 读取并验证 YAML
        import yaml
        with open(test_flow_file, 'r') as f:
            data = yaml.safe_load(f)
        
        assert 'id' in data
        assert 'namespace' in data
        assert 'tasks' in data
    
    def test_deploy_single_flow(self, client, test_flow_file):
        """测试部署单个工作流"""
        # 先测试连接
        success, message = client.test_connection()
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")
        
        # 部署测试工作流
        success, message = client.deploy_flow(test_flow_file)
        print(f"\n部署结果: {message}")
        assert success, f"部署失败: {message}"
        
        # 验证部署成功
        flows = client.list_flows("xcnstock")
        flow_ids = [f.id for f in flows]
        assert "test_integration_flow" in flow_ids, "工作流未在列表中找到"
    
    def test_list_flows(self, client):
        """测试列出工作流"""
        success, message = client.test_connection()
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")
        
        flows = client.list_flows("xcnstock")
        assert isinstance(flows, list)
        print(f"\n找到 {len(flows)} 个工作流")


class TestKestraExecution:
    """工作流执行测试"""
    
    @pytest.fixture
    def client(self):
        """创建 Kestra 客户端"""
        return create_client()
    
    @pytest.fixture(scope="module")
    def test_flow_for_execution(self, tmp_path_factory):
        """创建用于执行测试的工作流"""
        flow_content = """
id: test_execution_flow
namespace: xcnstock

description: 执行测试用工作流

tasks:
  - id: simple_task
    type: io.kestra.plugin.core.log.Log
    message: "Test execution task"
"""
        tmp_path = tmp_path_factory.mktemp("kestra_test")
        flow_file = tmp_path / "test_execution.yml"
        flow_file.write_text(flow_content)
        return flow_file
    
    def test_execute_flow(self, client, test_flow_for_execution):
        """测试触发工作流执行"""
        # 先测试连接
        success, message = client.test_connection()
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")
        
        # 先部署工作流
        success, message = client.deploy_flow(test_flow_for_execution)
        if not success:
            pytest.skip(f"工作流部署失败: {message}")
        
        # 触发执行
        execution_id, message = client.execute_flow("xcnstock", "test_execution_flow")
        print(f"\n执行结果: {message}")
        assert execution_id is not None, f"执行触发失败: {message}"
        
        # 等待执行完成
        success, execution = client.wait_for_execution(execution_id, timeout=60)
        print(f"执行状态: {execution.status.value if execution else 'Unknown'}")
        
        # 清理：删除测试工作流
        client.delete_flow("xcnstock", "test_execution_flow")
    
    def test_list_executions(self, client):
        """测试列出执行历史"""
        success, message = client.test_connection()
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")
        
        executions = client.list_executions(namespace="xcnstock", limit=5)
        assert isinstance(executions, list)
        print(f"\n找到 {len(executions)} 条执行记录")
    
    def test_get_execution(self, client):
        """测试获取执行详情"""
        success, message = client.test_connection()
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")
        
        # 获取最近的执行
        executions = client.list_executions(namespace="xcnstock", limit=1)
        if not executions:
            pytest.skip("没有执行记录可供测试")
        
        execution_id = executions[0].id
        execution = client.get_execution(execution_id)
        
        assert execution is not None
        assert execution.id == execution_id
        print(f"\n执行状态: {execution.status.value}")


class TestKestraMonitoring:
    """监控功能测试"""
    
    @pytest.fixture
    def client(self):
        """创建 Kestra 客户端"""
        return create_client()
    
    def test_get_logs(self, client):
        """测试获取日志"""
        success, message = client.test_connection()
        if not success:
            pytest.skip(f"Kestra 服务不可用: {message}")
        
        # 获取最近的执行
        executions = client.list_executions(namespace="xcnstock", limit=1)
        if not executions:
            pytest.skip("没有执行记录可供测试")
        
        execution_id = executions[0].id
        logs = client.get_logs(execution_id)
        
        assert isinstance(logs, list)
        print(f"\n获取到 {len(logs)} 条日志")


class TestKestraClientLibrary:
    """客户端库功能测试"""
    
    def test_client_initialization(self):
        """测试客户端初始化"""
        client = KestraClient(
            api_url="http://test:8080/api/v1",
            username="test@example.com",
            password="test123"
        )
        
        assert client.api_url == "http://test:8080/api/v1"
        assert client.username == "test@example.com"
        assert client.password == "test123"
    
    def test_client_from_env(self):
        """测试从环境变量创建客户端"""
        # 保存原始环境变量
        orig_url = os.getenv('KESTRA_API_URL')
        orig_user = os.getenv('KESTRA_USERNAME')
        orig_pass = os.getenv('KESTRA_PASSWORD')
        
        try:
            # 设置测试环境变量
            os.environ['KESTRA_API_URL'] = 'http://env-test:8080/api/v1'
            os.environ['KESTRA_USERNAME'] = 'env@test.com'
            os.environ['KESTRA_PASSWORD'] = 'env_pass'
            
            client = create_client()
            
            assert client.api_url == "http://env-test:8080/api/v1"
            assert client.username == "env@test.com"
            assert client.password == "env_pass"
        finally:
            # 恢复原始环境变量
            if orig_url:
                os.environ['KESTRA_API_URL'] = orig_url
            else:
                os.environ.pop('KESTRA_API_URL', None)
            
            if orig_user:
                os.environ['KESTRA_USERNAME'] = orig_user
            else:
                os.environ.pop('KESTRA_USERNAME', None)
            
            if orig_pass:
                os.environ['KESTRA_PASSWORD'] = orig_pass
            else:
                os.environ.pop('KESTRA_PASSWORD', None)
    
    def test_execution_status_enum(self):
        """测试执行状态枚举"""
        assert ExecutionStatus.SUCCESS.value == "SUCCESS"
        assert ExecutionStatus.FAILED.value == "FAILED"
        assert ExecutionStatus.RUNNING.value == "RUNNING"


def test_end_to_end():
    """
    端到端集成测试
    
    注意：此测试需要 Kestra 服务正在运行
    """
    client = create_client()
    
    # 1. 测试连接
    success, message = client.test_connection()
    if not success:
        pytest.skip(f"Kestra 服务不可用: {message}")
    
    print("\n" + "=" * 70)
    print("端到端集成测试")
    print("=" * 70)
    
    # 2. 列出工作流
    flows = client.list_flows("xcnstock")
    print(f"✅ 列出工作流: {len(flows)} 个")
    
    # 3. 列出执行历史
    executions = client.list_executions(namespace="xcnstock", limit=5)
    print(f"✅ 列出执行历史: {len(executions)} 条")
    
    # 4. 如果有执行记录，测试获取详情和日志
    if executions:
        execution_id = executions[0].id
        execution = client.get_execution(execution_id)
        print(f"✅ 获取执行详情: {execution.status.value}")
        
        logs = client.get_logs(execution_id)
        print(f"✅ 获取执行日志: {len(logs)} 条")
    
    print("=" * 70)
    print("端到端测试完成")
    print("=" * 70)


if __name__ == "__main__":
    # 直接运行测试
    pytest.main([__file__, "-v"])
