#!/bin/bash
# 部署脚本

set -e

echo "🚀 开始部署量化交易系统..."

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查环境变量
check_env() {
    echo -e "${YELLOW}检查环境变量...${NC}"
    
    if [ -z "$TUSHARE_TOKEN" ]; then
        echo -e "${RED}错误: TUSHARE_TOKEN 未设置${NC}"
        exit 1
    fi
    
    if [ -z "$MYSQL_ROOT_PASSWORD" ]; then
        echo -e "${RED}错误: MYSQL_ROOT_PASSWORD 未设置${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ 环境变量检查通过${NC}"
}

# 创建必要目录
create_dirs() {
    echo -e "${YELLOW}创建必要目录...${NC}"
    
    mkdir -p logs
    mkdir -p reports
    mkdir -p data/kline
    mkdir -p nginx/ssl
    
    echo -e "${GREEN}✓ 目录创建完成${NC}"
}

# 构建镜像
build_images() {
    echo -e "${YELLOW}构建Docker镜像...${NC}"
    
    docker-compose -f docker-compose.prod.yml build
    
    echo -e "${GREEN}✓ 镜像构建完成${NC}"
}

# 启动服务
start_services() {
    echo -e "${YELLOW}启动服务...${NC}"
    
    docker-compose -f docker-compose.prod.yml up -d
    
    echo -e "${GREEN}✓ 服务启动完成${NC}"
}

# 等待服务就绪
wait_for_services() {
    echo -e "${YELLOW}等待服务就绪...${NC}"
    
    sleep 10
    
    # 检查MySQL
    until docker exec quant-mysql mysqladmin ping --silent; do
        echo "等待MySQL就绪..."
        sleep 2
    done
    
    # 检查Redis
    until docker exec quant-redis redis-cli ping | grep -q PONG; do
        echo "等待Redis就绪..."
        sleep 2
    done
    
    echo -e "${GREEN}✓ 所有服务已就绪${NC}"
}

# 初始化数据库
init_database() {
    echo -e "${YELLOW}初始化数据库...${NC}"
    
    docker exec -i quant-mysql mysql -u root -p$MYSQL_ROOT_PASSWORD quant_trading < init.sql
    
    echo -e "${GREEN}✓ 数据库初始化完成${NC}"
}

# 显示状态
show_status() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}🎉 部署完成!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "服务访问地址:"
    echo "  - Web监控面板: http://localhost"
    echo "  - API服务: http://localhost:5000"
    echo "  - Nginx代理: http://localhost:8080"
    echo ""
    echo "查看日志:"
    echo "  docker-compose -f docker-compose.prod.yml logs -f"
    echo ""
    echo "停止服务:"
    echo "  docker-compose -f docker-compose.prod.yml down"
    echo ""
}

# 主流程
main() {
    check_env
    create_dirs
    build_images
    start_services
    wait_for_services
    init_database
    show_status
}

# 执行
main
