# 定时任务配置说明

## 方法一：使用 cron (Linux/macOS)

### 1. 编辑 crontab
```bash
crontab -e
```

### 2. 添加定时任务

#### 每日收盘后执行（推荐：下午 16:00）
```cron
# 每日下午16:00执行数据采集
0 16 * * 1-5 cd "/Volumes/Xdata/workstation/xcnstock 2" && /usr/bin/python3 scripts/scheduled_fetch.py >> logs/cron.log 2>&1
```

#### 每日凌晨执行（备选：凌晨 2:00）
```cron
# 每日凌晨2:00执行数据采集
0 2 * * * cd "/Volumes/Xdata/workstation/xcnstock 2" && /usr/bin/python3 scripts/scheduled_fetch.py >> logs/cron.log 2>&1
```

#### 每周执行一次（周日凌晨 3:00）
```cron
# 每周日凌晨3:00执行数据采集
0 3 * * 0 cd "/Volumes/Xdata/workstation/xcnstock 2" && /usr/bin/python3 scripts/scheduled_fetch.py >> logs/cron.log 2>&1
```

### 3. 查看 cron 任务
```bash
crontab -l
```

### 4. 查看 cron 日志
```bash
tail -f logs/cron.log
```

---

## 方法二：使用 launchd (macOS)

### 1. 创建 plist 文件
创建文件：`~/Library/LaunchAgents/com.xcnstock.fetch.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.xcnstock.fetch</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/Volumes/Xdata/workstation/xcnstock 2/scripts/scheduled_fetch.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Volumes/Xdata/workstation/xcnstock 2</string>
    <key>StandardOutPath</key>
    <string>/Volumes/Xdata/workstation/xcnstock 2/logs/launchd.log</string>
    <key>StandardErrorPath</key>
    <string>/Volumes/Xdata/workstation/xcnstock 2/logs/launchd_error.log</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>16</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
</dict>
</plist>
```

### 2. 加载任务
```bash
launchctl load ~/Library/LaunchAgents/com.xcnstock.fetch.plist
```

### 3. 卸载任务
```bash
launchctl unload ~/Library/LaunchAgents/com.xcnstock.fetch.plist
```

---

## 方法三：使用 systemd timer (Linux)

### 1. 创建 service 文件
创建文件：`/etc/systemd/system/xcnstock-fetch.service`

```ini
[Unit]
Description=XCNStock Data Fetch Service
After=network.target

[Service]
Type=oneshot
User=simonsquant
WorkingDirectory=/Volumes/Xdata/workstation/xcnstock 2
ExecStart=/usr/bin/python3 /Volumes/Xdata/workstation/xcnstock 2/scripts/scheduled_fetch.py
StandardOutput=append:/Volumes/Xdata/workstation/xcnstock 2/logs/systemd.log
StandardError=append:/Volumes/Xdata/workstation/xcnstock 2/logs/systemd_error.log

[Install]
WantedBy=multi-user.target
```

### 2. 创建 timer 文件
创建文件：`/etc/systemd/system/xcnstock-fetch.timer`

```ini
[Unit]
Description=XCNStock Data Fetch Timer

[Timer]
OnCalendar=*-*-* 16:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

### 3. 启用定时器
```bash
sudo systemctl enable xcnstock-fetch.timer
sudo systemctl start xcnstock-fetch.timer
```

### 4. 查看定时器状态
```bash
systemctl list-timers
```

---

## 方法四：使用 Python schedule 库

### 创建守护进程脚本
创建文件：`scripts/schedule_daemon.py`

```python
import schedule
import time
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def job():
    """执行数据采集任务"""
    script_path = PROJECT_ROOT / "scripts" / "scheduled_fetch.py"
    subprocess.run([sys.executable, str(script_path)])

# 每日下午16:00执行
schedule.every().day.at("16:00").do(job)

# 或者每个工作日下午16:00执行
# schedule.every().monday.at("16:00").do(job)
# schedule.every().tuesday.at("16:00").do(job)
# schedule.every().wednesday.at("16:00").do(job)
# schedule.every().thursday.at("16:00").do(job)
# schedule.every().friday.at("16:00").do(job)

print("定时任务守护进程已启动...")
print("将在每日下午16:00执行数据采集")

while True:
    schedule.run_pending()
    time.sleep(60)
```

### 启动守护进程
```bash
nohup python3 scripts/schedule_daemon.py > logs/daemon.log 2>&1 &
```

---

## 推荐配置

根据项目需求（每日收盘后更新历史数据），推荐使用：

### macOS: launchd
- 稳定性好，系统原生支持
- 自动重启失败的任务
- 日志管理方便

### Linux: systemd timer
- 系统级定时任务
- 支持依赖管理
- 日志集成完善

### 快速测试: cron
- 配置简单
- 适合快速验证

---

## 日志位置

所有日志文件存储在 `logs/` 目录：
- `scheduled_fetch.log` - 定时任务执行日志
- `cron.log` - cron 输出日志
- `launchd.log` - launchd 输出日志
- `systemd.log` - systemd 输出日志

---

## 注意事项

1. **执行时间**：建议在收盘后（下午 16:00）执行，确保数据完整
2. **网络连接**：确保服务器网络稳定
3. **磁盘空间**：定期检查磁盘空间，清理旧日志
4. **错误通知**：建议配置邮件或短信通知失败情况
5. **权限问题**：确保脚本有执行权限

---

## 快速开始

### 1. 测试定时任务脚本
```bash
cd "/Volumes/Xdata/workstation/xcnstock 2"
python3 scripts/scheduled_fetch.py
```

### 2. 配置 cron（最简单）
```bash
# 编辑 crontab
crontab -e

# 添加以下行（每日16:00执行）
0 16 * * 1-5 cd "/Volumes/Xdata/workstation/xcnstock 2" && /usr/bin/python3 scripts/scheduled_fetch.py >> logs/cron.log 2>&1
```

### 3. 验证配置
```bash
# 查看 cron 任务
crontab -l

# 查看日志
tail -f logs/scheduled_fetch.log
```
