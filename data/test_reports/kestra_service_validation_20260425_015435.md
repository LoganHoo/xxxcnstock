# Kestra 服务验证报告

验证时间: 2026-04-25 01:54:35

## 环境变量配置

- **API URL**: http://localhost:8082/api/v1
- **Web URL**: http://localhost:8082/ui/
- **用户名**: admin@kestra.io
- **命名空间**: xcnstock

## 验证结果

| 检查项 | 状态 |
|--------|------|
| 配置格式 | ✅ 通过 |
| 服务连接 | ❌ 失败 |
| 工作流配置 | ✅ 10个 |

## 结论

⚠️ **配置正确，但服务未运行**

**启动命令:**
```bash
# Docker方式启动
docker run -d --name kesta -p 8082:8080 kestra/kestra:latest server local
```
