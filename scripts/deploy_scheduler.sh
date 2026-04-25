#!/bin/bash
# XCNStock Scheduler Docker 部署脚本
# 用于快速部署任务调度器服务

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目配置
PROJECT_NAME="xcnstock"
COMPOSE_FILE="docker-compose.scheduler.yml"
IMAGE_NAME="xcnstock/scheduler"

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Docker和Docker Compose
check_prerequisites() {
    print_info "检查 prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker 未安装，请先安装 Docker"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose 未安装，请先安装 Docker Compose"
        exit 1
    fi
    
    print_success "Prerequisites 检查通过"
}

# 创建必要的目录
setup_directories() {
    print_info "创建必要的目录..."
    
    mkdir -p data logs config
    
    # 确保目录权限正确
    chmod 755 data logs config
    
    print_success "目录创建完成"
}

# 构建镜像
build_image() {
    print_info "构建 Docker 镜像..."
    
    docker-compose -f $COMPOSE_FILE build --no-cache
    
    print_success "镜像构建完成"
}

# 启动服务
start_services() {
    print_info "启动调度器服务..."
    
    docker-compose -f $COMPOSE_FILE up -d
    
    print_success "服务已启动"
}

# 启动带Redis的服务
start_with_redis() {
    print_info "启动调度器服务（带Redis）..."
    
    docker-compose -f $COMPOSE_FILE --profile with-redis up -d
    
    print_success "服务已启动（带Redis）"
}

# 停止服务
stop_services() {
    print_info "停止调度器服务..."
    
    docker-compose -f $COMPOSE_FILE down
    
    print_success "服务已停止"
}

# 查看状态
show_status() {
    print_info "服务状态:"
    docker-compose -f $COMPOSE_FILE ps
}

# 查看日志
show_logs() {
    print_info "查看日志 (按 Ctrl+C 退出)..."
    docker-compose -f $COMPOSE_FILE logs -f --tail=100
}

# 查看调度器状态
show_scheduler_status() {
    print_info "调度器任务状态:"
    
    STATE_FILE="data/scheduler_state.json"
    if [ -f "$STATE_FILE" ]; then
        cat "$STATE_FILE" | python3 -m json.tool 2>/dev/null || cat "$STATE_FILE"
    else
        print_warning "状态文件不存在: $STATE_FILE"
    fi
}

# 重启服务
restart_services() {
    print_info "重启调度器服务..."
    
    docker-compose -f $COMPOSE_FILE restart
    
    print_success "服务已重启"
}

# 更新部署
update_deployment() {
    print_info "更新部署..."
    
    # 拉取最新代码（如果在git仓库中）
    if [ -d ".git" ]; then
        git pull origin main || print_warning "Git pull 失败，使用本地代码"
    fi
    
    # 重新构建并启动
    docker-compose -f $COMPOSE_FILE down
    docker-compose -f $COMPOSE_FILE build --no-cache
    docker-compose -f $COMPOSE_FILE up -d
    
    print_success "部署更新完成"
}

# 清理数据
cleanup_data() {
    print_warning "这将删除所有数据！是否继续? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        docker-compose -f $COMPOSE_FILE down -v
        rm -rf data/* logs/*
        print_success "数据已清理"
    else
        print_info "取消清理"
    fi
}

# 健康检查
health_check() {
    print_info "执行健康检查..."
    
    # 检查容器状态
    if docker-compose -f $COMPOSE_FILE ps | grep -q "Up"; then
        print_success "容器运行正常"
    else
        print_error "容器未运行"
        return 1
    fi
    
    # 检查调度器状态文件
    STATE_FILE="data/scheduler_state.json"
    if [ -f "$STATE_FILE" ]; then
        print_success "状态文件存在"
    else
        print_warning "状态文件不存在，调度器可能尚未启动"
    fi
    
    print_success "健康检查完成"
}

# 显示帮助信息
show_help() {
    cat << EOF
XCNStock Scheduler Docker 部署脚本

用法: $0 [命令]

命令:
    build       构建 Docker 镜像
    start       启动服务
    start-redis 启动服务（带Redis支持）
    stop        停止服务
    restart     重启服务
    status      查看服务状态
    logs        查看日志
    scheduler   查看调度器任务状态
    update      更新部署（拉取代码并重建）
    health      健康检查
    cleanup     清理所有数据（危险操作！）
    help        显示此帮助信息

示例:
    $0 build          # 构建镜像
    $0 start          # 启动服务
    $0 logs           # 查看日志
    $0 scheduler      # 查看任务状态

配置文件:
    - docker-compose.scheduler.yml  Docker Compose 配置
    - Dockerfile.scheduler          镜像构建配置
    - config/cron_tasks.yaml        定时任务配置

数据目录:
    - data/    数据文件
    - logs/    日志文件
EOF
}

# 主函数
main() {
    # 切换到脚本所在目录
    cd "$(dirname "$0")/.."
    
    case "${1:-help}" in
        build)
            check_prerequisites
            setup_directories
            build_image
            ;;
        start)
            check_prerequisites
            setup_directories
            start_services
            show_status
            ;;
        start-redis)
            check_prerequisites
            setup_directories
            start_with_redis
            show_status
            ;;
        stop)
            stop_services
            ;;
        restart)
            restart_services
            show_status
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs
            ;;
        scheduler)
            show_scheduler_status
            ;;
        update)
            update_deployment
            ;;
        health)
            health_check
            ;;
        cleanup)
            cleanup_data
            ;;
        help|*)
            show_help
            ;;
    esac
}

# 执行主函数
main "$@"
