#!/usr/bin/env python3
"""
Kestra API 测试脚本 - 使用正确的端点
"""
import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

KESTRA_API_URL = os.getenv('KESTRA_API_URL', 'http://localhost:8082/api/v1')
KESTRA_USERNAME = os.getenv('KESTRA_USERNAME', 'admin@kestra.io')
KESTRA_PASSWORD = os.getenv('KESTRA_PASSWORD', 'Kestra123')

auth = (KESTRA_USERNAME, KESTRA_PASSWORD)

print("=" * 70)
print("Kestra API 测试")
print("=" * 70)
print(f"API URL: {KESTRA_API_URL}")
print(f"用户名: {KESTRA_USERNAME}")
print("=" * 70)

# 测试 API 端点
endpoints = [
    "/",
    "/namespaces",
    "/flows",
    "/executions",
]

print("\n测试 API 端点:")
for path in endpoints:
    url = f"{KESTRA_API_URL}{path}"
    try:
        response = requests.get(url, auth=auth, timeout=5)
        status = "✅" if response.status_code == 200 else "⚠️"
        print(f"{status} {path} -> {response.status_code}")
        if response.status_code == 200:
            try:
                data = response.json()
                if 'results' in data:
                    print(f"   结果数: {len(data['results'])}")
            except:
                pass
    except Exception as e:
        print(f"❌ {path} -> 错误: {e}")

print("\n" + "=" * 70)
print("测试完成")
print("=" * 70)
