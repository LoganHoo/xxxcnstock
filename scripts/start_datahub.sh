#!/bin/bash
# DataHub 服务启动脚本

set -e

echo "======================================================================"
echo "🚀 启动 DataHub 元数据平台"
echo "======================================================================"

# 检查 Docker
echo ""
echo "📋 检查 Docker 环境..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，请先安装 Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装，请先安装 Docker Compose"
    exit 1
fi

echo "✅ Docker 环境检查通过"

# 创建数据目录
echo ""
echo "📁 创建数据目录..."
mkdir -p data/mysql
mkdir -p data/elasticsearch
mkdir -p data/neo4j

# 启动服务
echo ""
echo "🐳 启动 DataHub 服务..."
docker-compose -f docker-compose.datahub.yml up -d

# 等待服务启动
echo ""
echo "⏳ 等待服务启动 (约 60 秒)..."
sleep 10
echo "  - MySQL 启动中..."
sleep 15
echo "  - Elasticsearch 启动中..."
sleep 15
echo "  - Neo4j 启动中..."
sleep 10
echo "  - DataHub GMS 启动中..."
sleep 10

# 检查服务状态
echo ""
echo "🔍 检查服务状态..."
docker-compose -f docker-compose.datahub.yml ps

# 输出访问信息
echo ""
echo "======================================================================"
echo "✅ DataHub 启动完成"
echo "======================================================================"
echo ""
echo "📱 访问地址:"
echo "   • DataHub UI:    http://localhost:9002"
echo "   • GMS API:       http://localhost:8080"
echo "   • Neo4j Browser: http://localhost:7474"
echo ""
echo "🔑 默认账号:"
echo "   • 用户名: datahub"
echo "   • 密码:   datahub"
echo ""
echo "🛠️  常用命令:"
echo "   • 查看日志: docker-compose -f docker-compose.datahub.yml logs -f"
echo "   • 停止服务: docker-compose -f docker-compose.datahub.yml down"
echo "   • 重启服务: docker-compose -f docker-compose.datahub.yml restart"
echo ""
echo "📚 下一步:"
echo "   1. 访问 http://localhost:9002 登录 DataHub"
echo "   2. 运行: python scripts/bulk_import_metadata.py 导入元数据"
echo ""
