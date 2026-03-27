# 邮件通知配置说明

## 概述

定时任务支持在每日数据采集完成后，自动发送邮件通知到指定邮箱，包含数据验证结果和完整性报告。

---

## 快速配置

### 1. 获取QQ邮箱授权码

#### 步骤：
1. 登录QQ邮箱（https://mail.qq.com）
2. 点击右上角 **设置** -> **账户**
3. 找到 **POP3/IMAP/SMTP/Exchange/CardDAV/CalDAV服务**
4. 开启 **IMAP/SMTP服务**
5. 生成 **授权码**（不是QQ密码）

#### 注意：
- 授权码用于第三方应用登录，不是QQ密码
- 授权码生成后请妥善保存，只显示一次
- 建议使用专门的邮箱用于发送通知

### 2. 配置环境变量

编辑项目根目录下的 `.env` 文件：

```bash
# 邮件通知配置
SMTP_SERVER=smtp.qq.com
SMTP_PORT=587
SMTP_USE_TLS=true
SENDER_EMAIL=your_email@qq.com
SENDER_PASSWORD=your_authorization_code
NOTIFICATION_EMAILS=287363@qq.com,other@example.com
```

#### 配置说明：

| 变量名 | 说明 | 示例 |
|--------|------|------|
| `SMTP_SERVER` | SMTP服务器地址 | `smtp.qq.com` |
| `SMTP_PORT` | SMTP端口 | `587` (TLS) 或 `465` (SSL) |
| `SMTP_USE_TLS` | 是否使用TLS | `true` |
| `SENDER_EMAIL` | 发件人邮箱 | `your_email@qq.com` |
| `SENDER_PASSWORD` | 发件人授权码 | `abcdefghijklmnop` |
| `NOTIFICATION_EMAILS` | 收件人邮箱（多个用逗号分隔） | `287363@qq.com` |

---

## 邮件内容

### 邮件主题
```
XCNStock 每日数据采集报告 - 2026-03-24
```

### 邮件内容包含：

#### 1. 市场状态
- 当前时间
- 是否交易日
- 是否收盘后
- 上一交易日

#### 2. 数据验证
- 总股票数
- 有效数据数量
- 无效数据数量
- 警告数量

#### 3. 数据完整性
- 期望日期
- 总股票数
- 包含数据的股票数
- 缺失数据的股票数
- 完整率

#### 4. 附件
- 每日报告JSON文件（`daily_report.json`）

---

## 测试邮件发送

### 方式一：运行测试脚本

```bash
python scripts/test_scheduled_task.py
```

### 方式二：手动测试

```python
from scripts.email_sender import EmailSender
import os

sender = EmailSender(
    smtp_server='smtp.qq.com',
    smtp_port=587,
    sender_email=os.getenv('SENDER_EMAIL'),
    sender_password=os.getenv('SENDER_PASSWORD')
)

# 发送测试邮件
sender.send_email(
    to_emails=['287363@qq.com'],
    subject='测试邮件',
    content='这是一封测试邮件'
)
```

---

## 常见问题

### 1. 邮件发送失败

#### 错误：`SMTPAuthenticationError`
- **原因**：邮箱或授权码错误
- **解决**：检查 `SENDER_EMAIL` 和 `SENDER_PASSWORD` 是否正确

#### 错误：`SMTPServerDisconnected`
- **原因**：SMTP服务器连接失败
- **解决**：
  - 检查网络连接
  - 确认SMTP服务器地址和端口正确
  - 尝试使用SSL端口465

#### 错误：`ConnectionRefusedError`
- **原因**：端口被阻止
- **解决**：
  - 检查防火墙设置
  - 尝试其他端口（587或465）

### 2. 收不到邮件

#### 检查步骤：
1. 检查垃圾邮件文件夹
2. 确认收件人邮箱地址正确
3. 检查发件人邮箱是否被限制
4. 查看日志文件：`logs/scheduled_fetch_optimized.log`

### 3. 邮件内容乱码

- **原因**：编码问题
- **解决**：确保使用UTF-8编码（已默认配置）

---

## 其他邮箱配置

### 163邮箱

```bash
SMTP_SERVER=smtp.163.com
SMTP_PORT=465
SMTP_USE_TLS=false
SENDER_EMAIL=your_email@163.com
SENDER_PASSWORD=your_authorization_code
```

### Gmail

```bash
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USE_TLS=true
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_app_password
```

**注意**：Gmail需要开启"两步验证"并生成"应用专用密码"

### 企业邮箱

```bash
SMTP_SERVER=smtp.exmail.qq.com
SMTP_PORT=465
SMTP_USE_TLS=false
SENDER_EMAIL=your_email@company.com
SENDER_PASSWORD=your_password
```

---

## 安全建议

1. **使用专用邮箱**：建议使用专门的邮箱发送通知，不使用个人主邮箱
2. **定期更换授权码**：定期更换邮箱授权码，提高安全性
3. **限制发送频率**：避免频繁发送邮件，防止被标记为垃圾邮件
4. **保护配置文件**：`.env` 文件包含敏感信息，确保不被提交到版本控制

---

## 邮件模板

### 纯文本格式

```
XCNStock 每日数据采集报告
==================================================

时间: 2026-03-24 17:30:00
市场状态: 周一 交易日

数据验证:
  - 总股票数: 5079
  - 有效数据: 5070
  - 无效数据: 9
  - 警告数量: 15

数据完整性:
  - 期望日期: 2026-03-24
  - 总股票数: 5079
  - 包含数据: 5070
  - 缺失数据: 9
  
==================================================

XCNStock A股量化分析系统
```

### HTML格式

邮件同时包含HTML格式，显示效果更美观，包含：
- 渐变色标题
- 彩色状态指示
- 数据统计卡片
- 响应式布局

---

## 禁用邮件通知

如果不需要邮件通知，可以：

### 方式一：不配置环境变量

不设置 `SENDER_EMAIL` 和 `SENDER_PASSWORD`，系统会自动跳过邮件发送。

### 方式二：注释配置

在 `.env` 文件中注释掉邮件配置：

```bash
# SENDER_EMAIL=your_email@qq.com
# SENDER_PASSWORD=your_authorization_code
```

---

## 日志查看

### 邮件发送日志

```bash
# 查看定时任务日志
tail -f logs/scheduled_fetch_optimized.log

# 搜索邮件相关日志
grep "邮件" logs/scheduled_fetch_optimized.log

# 查看最近的邮件发送记录
grep "邮件发送" logs/scheduled_fetch_optimized.log | tail -10
```

### 日志示例

```
2026-03-24 17:30:00 - INFO - 发送通知邮件
2026-03-24 17:30:01 - INFO - ✅ 邮件发送成功: 287363@qq.com
```

---

## 总结

1. **配置简单**：只需配置邮箱和授权码即可使用
2. **内容丰富**：包含市场状态、数据验证、完整性检查
3. **格式美观**：同时支持纯文本和HTML格式
4. **安全可靠**：使用TLS加密传输，授权码认证
5. **易于测试**：提供测试脚本，快速验证配置

配置完成后，每日数据采集完成时会自动发送邮件通知到 `287363@qq.com`！
