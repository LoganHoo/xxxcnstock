#!/usr/bin/env python3
"""
DataHub 客户端模块

集成 DataHub (http://192.168.1.168:9002/) 用于:
- 元数据管理
- 数据血缘追踪
- 数据质量监控
- 数据集发现

使用示例:
    client = DataHubClient()
    
    # 获取数据集列表
    datasets = client.get_datasets()
    
    # 获取数据集元数据
    metadata = client.get_dataset_metadata("urn:li:dataset:(urn:li:dataPlatform:mysql,xcn_db.stock_kline,PROD)")
    
    # 发布数据血缘
    client.publish_lineage(
        source_urn="urn:li:dataset:(urn:li:dataPlatform:mysql,xcn_db.stock_kline,PROD)",
        target_urn="urn:li:dataset:(urn:li:dataPlatform:mysql,xcn_db.stock_selection,PROD)"
    )
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import os
import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime

import requests
from urllib.parse import urljoin

from core.logger import setup_logger

logger = setup_logger("datahub_client")


@dataclass
class DataHubConfig:
    """DataHub 配置"""
    gms_url: str = "http://192.168.1.168:8080"  # GMS (Metadata Service)
    ui_url: str = "http://192.168.1.168:9002"   # Frontend UI
    username: str = "datahub"
    password: str = "datahub"
    token: Optional[str] = None
    env: str = "PROD"
    timeout: int = 30


class DataHubClient:
    """DataHub 客户端"""
    
    def __init__(self, config: Optional[DataHubConfig] = None):
        """
        初始化 DataHub 客户端
        
        Args:
            config: DataHub 配置，默认从环境变量读取
        """
        if config is None:
            config = self._load_config_from_env()
        
        self.config = config
        self.logger = logger
        
        # 认证令牌
        self._access_token: Optional[str] = None
        
        # 会话
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        self.logger.info(f"DataHub 客户端初始化完成: GMS={config.gms_url}, UI={config.ui_url}")
    
    def _load_config_from_env(self) -> DataHubConfig:
        """从环境变量加载配置"""
        return DataHubConfig(
            gms_url=os.getenv('DATAHUB_GMS_URL', 'http://192.168.1.168:8080'),
            ui_url=os.getenv('DATAHUB_UI_URL', 'http://192.168.1.168:9002'),
            username=os.getenv('DATAHUB_USERNAME', 'datahub'),
            password=os.getenv('DATAHUB_PASSWORD', 'datahub'),
            token=os.getenv('DATAHUB_TOKEN'),
            env=os.getenv('DATAHUB_ENV', 'PROD')
        )
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """获取认证头"""
        headers = {}
        
        # 优先使用 Token 认证
        if self.config.token:
            headers['Authorization'] = f'Bearer {self.config.token}'
        elif self._access_token:
            headers['Authorization'] = f'Bearer {self._access_token}'
        
        return headers
    
    def authenticate(self) -> bool:
        """
        认证并获取访问令牌
        
        Returns:
            认证是否成功
        """
        try:
            # DataHub 使用 Basic Auth 或 Token
            # 这里使用简单的会话认证
            login_url = urljoin(self.config.ui_url, '/api/v2/graphql')
            
            # 尝试访问一个端点来验证连接
            response = self.session.post(
                login_url,
                json={"query": "{ me { username } }"},
                auth=(self.config.username, self.config.password),
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                self.logger.info("DataHub 认证成功")
                return True
            else:
                self.logger.error(f"DataHub 认证失败: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"DataHub 认证异常: {e}")
            return False
    
    def health_check(self) -> bool:
        """
        检查 DataHub 服务健康状态
        
        Returns:
            服务是否正常
        """
        try:
            # 检查 GMS 健康状态
            health_url = urljoin(self.config.gms_url, '/health')
            response = self.session.get(health_url, timeout=10)
            
            if response.status_code == 200:
                self.logger.info("DataHub GMS 服务正常")
                return True
            else:
                self.logger.warning(f"DataHub GMS 服务异常: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"DataHub 健康检查失败: {e}")
            return False
    
    def get_datasets(self, platform: Optional[str] = None, 
                     query: Optional[str] = None,
                     start: int = 0, 
                     count: int = 10) -> List[Dict[str, Any]]:
        """
        获取数据集列表
        
        Args:
            platform: 数据平台过滤 (如 mysql, kafka)
            query: 搜索关键词
            start: 起始位置
            count: 返回数量
        
        Returns:
            数据集列表
        """
        try:
            # 使用 GraphQL API 搜索数据集
            graphql_url = urljoin(self.config.gms_url, '/api/graphql')
            
            search_query = """
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
                                schemaMetadata {
                                    fields {
                                        fieldPath
                                        type
                                    }
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
                    "query": query or "*",
                    "start": start,
                    "count": count
                }
            }
            
            if platform:
                variables["input"]["filters"] = [
                    {"field": "platform", "values": [platform]}
                ]
            
            response = self.session.post(
                graphql_url,
                json={"query": search_query, "variables": variables},
                auth=(self.config.username, self.config.password),
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'search' in data['data']:
                    results = data['data']['search']['searchResults']
                    datasets = []
                    for result in results:
                        entity = result.get('entity', {})
                        datasets.append({
                            'urn': entity.get('urn'),
                            'name': entity.get('name'),
                            'platform': entity.get('platform', {}).get('name'),
                            'type': entity.get('type')
                        })
                    return datasets
            
            self.logger.warning(f"获取数据集列表失败: {response.status_code}")
            return []
            
        except Exception as e:
            self.logger.error(f"获取数据集列表异常: {e}")
            return []
    
    def get_dataset_metadata(self, urn: str) -> Optional[Dict[str, Any]]:
        """
        获取数据集元数据
        
        Args:
            urn: 数据集 URN
        
        Returns:
            元数据字典
        """
        try:
            graphql_url = urljoin(self.config.gms_url, '/api/graphql')
            
            query = """
            query getDataset($urn: String!) {
                dataset(urn: $urn) {
                    urn
                    name
                    platform {
                        name
                    }
                    schemaMetadata {
                        fields {
                            fieldPath
                            type
                            description
                        }
                    }
                    properties {
                        description
                        customProperties {
                            key
                            value
                        }
                    }
                    ownership {
                        owners {
                            owner {
                                username
                            }
                            type
                        }
                    }
                }
            }
            """
            
            response = self.session.post(
                graphql_url,
                json={"query": query, "variables": {"urn": urn}},
                auth=(self.config.username, self.config.password),
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'dataset' in data['data']:
                    return data['data']['dataset']
            
            return None
            
        except Exception as e:
            self.logger.error(f"获取数据集元数据异常: {e}")
            return None
    
    def publish_lineage(self, source_urn: str, target_urn: str, 
                       lineage_type: str = "TRANSFORMED") -> bool:
        """
        发布数据血缘关系
        
        Args:
            source_urn: 源数据集 URN
            target_urn: 目标数据集 URN
            lineage_type: 血缘类型 (TRANSFORMED, COPIED, etc.)
        
        Returns:
            发布是否成功
        """
        try:
            graphql_url = urljoin(self.config.gms_url, '/api/graphql')
            
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
            
            response = self.session.post(
                graphql_url,
                json={"query": mutation, "variables": variables},
                auth=(self.config.username, self.config.password),
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                self.logger.info(f"数据血缘发布成功: {source_urn} -> {target_urn}")
                return True
            else:
                self.logger.error(f"数据血缘发布失败: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"发布数据血缘异常: {e}")
            return False
    
    def emit_metadata(self, entity_type: str, entity_urn: str, 
                     aspect_name: str, aspect_value: Dict[str, Any]) -> bool:
        """
        发送元数据到 DataHub
        
        Args:
            entity_type: 实体类型 (dataset, dataJob, etc.)
            entity_urn: 实体 URN
            aspect_name: Aspect 名称
            aspect_value: Aspect 值
        
        Returns:
            发送是否成功
        """
        try:
            # 使用 DataHub REST API 发送元数据
            url = urljoin(self.config.gms_url, f'/api/v2/entity/{entity_urn}')
            
            payload = {
                "entityType": entity_type,
                "entityUrn": entity_urn,
                "aspectName": aspect_name,
                "aspect": aspect_value
            }
            
            response = self.session.post(
                url,
                json=payload,
                auth=(self.config.username, self.config.password),
                timeout=self.config.timeout
            )
            
            if response.status_code in [200, 201]:
                self.logger.info(f"元数据发送成功: {entity_urn}")
                return True
            else:
                self.logger.error(f"元数据发送失败: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"发送元数据异常: {e}")
            return False
    
    def register_dataset(self, platform: str, name: str, 
                        schema_fields: List[Dict[str, str]],
                        description: Optional[str] = None,
                        custom_properties: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        注册数据集到 DataHub
        
        Args:
            platform: 数据平台 (mysql, kafka, etc.)
            name: 数据集名称
            schema_fields: Schema 字段列表
            description: 描述
            custom_properties: 自定义属性
        
        Returns:
            数据集 URN，失败返回 None
        """
        try:
            # 构建 URN
            urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{self.config.env})"
            
            # 构建 schema
            schema = {
                "schemaName": name,
                "platform": platform,
                "version": 0,
                "created": {"time": int(datetime.now().timestamp() * 1000)},
                "fields": [
                    {
                        "fieldPath": f["name"],
                        "type": f.get("type", "STRING"),
                        "description": f.get("description", "")
                    }
                    for f in schema_fields
                ]
            }
            
            # 发送 schema 元数据
            success = self.emit_metadata(
                entity_type="dataset",
                entity_urn=urn,
                aspect_name="schemaMetadata",
                aspect_value=schema
            )
            
            if success:
                self.logger.info(f"数据集注册成功: {urn}")
                return urn
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"注册数据集异常: {e}")
            return None


def main():
    """测试 DataHub 客户端"""
    client = DataHubClient()
    
    # 健康检查
    print("="*60)
    print("DataHub 健康检查")
    print("="*60)
    if client.health_check():
        print("✅ DataHub 服务正常")
    else:
        print("❌ DataHub 服务异常")
        return
    
    # 认证
    print("\n" + "="*60)
    print("DataHub 认证")
    print("="*60)
    if client.authenticate():
        print("✅ 认证成功")
    else:
        print("❌ 认证失败")
        return
    
    # 获取数据集列表
    print("\n" + "="*60)
    print("数据集列表")
    print("="*60)
    datasets = client.get_datasets(count=5)
    if datasets:
        for ds in datasets:
            print(f"  - {ds['name']} ({ds['platform']})")
            print(f"    URN: {ds['urn']}")
    else:
        print("  暂无数据集")
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)


if __name__ == '__main__':
    main()
