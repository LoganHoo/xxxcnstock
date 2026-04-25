#!/usr/bin/env python3
"""
DataHub REST API 客户端

通过 DataHub Frontend (端口 9002) 的 REST API 进行元数据管理
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import os
import json
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from core.logger import setup_logger

logger = setup_logger("datahub_rest_client")


class DataHubRestClient:
    """DataHub REST API 客户端"""
    
    def __init__(self):
        self.base_url = os.getenv('DATAHUB_UI_URL', 'http://192.168.1.168:9002')
        self.username = os.getenv('DATAHUB_USERNAME', 'datahub')
        self.password = os.getenv('DATAHUB_PASSWORD', 'datahub')
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        self.logger = logger
    
    def health_check(self) -> bool:
        """健康检查"""
        try:
            response = self.session.get(f"{self.base_url}/api/v2/graphql", timeout=10)
            return response.status_code in [200, 401]
        except Exception as e:
            self.logger.error(f"健康检查失败: {e}")
            return False
    
    def graphql_query(self, query: str, variables: Dict = None) -> Optional[Dict]:
        """执行 GraphQL 查询"""
        try:
            url = f"{self.base_url}/api/v2/graphql"
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            
            response = self.session.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"GraphQL 查询失败: {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"GraphQL 查询异常: {e}")
            return None
    
    def search_datasets(self, query: str = "*", start: int = 0, count: int = 10) -> List[Dict]:
        """搜索数据集"""
        graphql_query = """
        query search($input: SearchInput!) {
            search(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Dataset {
                            name
                            platform {
                                name
                            }
                        }
                    }
                }
            }
        }
        """
        
        variables = {
            "input": {
                "type": "DATASET",
                "query": query,
                "start": start,
                "count": count
            }
        }
        
        result = self.graphql_query(graphql_query, variables)
        
        if result and 'data' in result and 'search' in result['data']:
            datasets = []
            for item in result['data']['search']['searchResults']:
                entity = item.get('entity', {})
                datasets.append({
                    'urn': entity.get('urn'),
                    'name': entity.get('name'),
                    'platform': entity.get('platform', {}).get('name'),
                    'type': entity.get('type')
                })
            return datasets
        
        return []
    
    def create_dataset(self, platform: str, name: str, description: str = "", 
                      schema_fields: List[Dict] = None) -> Optional[str]:
        """
        创建数据集
        
        Args:
            platform: 平台名称 (如 mysql, kafka, xcnstock)
            name: 数据集名称
            description: 描述
            schema_fields: Schema 字段列表
        
        Returns:
            数据集 URN
        """
        try:
            # 使用 GraphQL mutation 创建数据集
            mutation = """
            mutation createDataset($input: DatasetInput!) {
                createDataset(input: $input) {
                    urn
                }
            }
            """
            
            variables = {
                "input": {
                    "name": name,
                    "platform": platform,
                    "description": description,
                    "schema": {
                        "fields": schema_fields or []
                    }
                }
            }
            
            result = self.graphql_query(mutation, variables)
            
            if result and 'data' in result and 'createDataset' in result['data']:
                urn = result['data']['createDataset']['urn']
                self.logger.info(f"数据集创建成功: {urn}")
                return urn
            else:
                self.logger.warning(f"数据集创建失败: {result}")
                return None
                
        except Exception as e:
            self.logger.error(f"创建数据集异常: {e}")
            return None
    
    def emit_lineage(self, source_urn: str, target_urn: str, 
                    lineage_type: str = "TRANSFORMED") -> bool:
        """
        发布血缘关系
        
        Args:
            source_urn: 源数据集 URN
            target_urn: 目标数据集 URN
            lineage_type: 血缘类型
        
        Returns:
            是否成功
        """
        try:
            mutation = """
            mutation updateLineage($input: UpdateLineageInput!) {
                updateLineage(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "edgesToAdd": [
                        {
                            "upstreamUrn": source_urn,
                            "downstreamUrn": target_urn,
                            "lineageType": lineage_type
                        }
                    ],
                    "edgesToRemove": []
                }
            }
            
            result = self.graphql_query(mutation, variables)
            
            if result and 'data' in result:
                self.logger.info(f"血缘发布成功: {source_urn} -> {target_urn}")
                return True
            else:
                self.logger.warning(f"血缘发布失败: {result}")
                return False
                
        except Exception as e:
            self.logger.error(f"发布血缘异常: {e}")
            return False


def main():
    """测试 REST 客户端"""
    client = DataHubRestClient()
    
    print("="*60)
    print("DataHub REST 客户端测试")
    print("="*60)
    
    # 1. 健康检查
    print("\n1. 健康检查")
    print("-"*60)
    if client.health_check():
        print("✅ DataHub 服务正常")
    else:
        print("❌ DataHub 服务异常")
        return
    
    # 2. 搜索数据集
    print("\n2. 搜索数据集")
    print("-"*60)
    datasets = client.search_datasets(count=5)
    if datasets:
        for ds in datasets:
            print(f"  - {ds['name']} ({ds['platform']})")
    else:
        print("  暂无数据集")
    
    # 3. 创建测试数据集
    print("\n3. 创建测试数据集")
    print("-"*60)
    urn = client.create_dataset(
        platform="xcnstock",
        name="test_dataset",
        description="测试数据集",
        schema_fields=[
            {"fieldPath": "code", "type": "STRING"},
            {"fieldPath": "name", "type": "STRING"}
        ]
    )
    if urn:
        print(f"✅ 数据集创建成功: {urn}")
    else:
        print("⚠️ 数据集创建失败")
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)


if __name__ == '__main__':
    main()
