# 数据人工验证系统实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现每日数据人工验证流程，包含连续性检查、最新数据验证、随机抽检3股、第三方验证1股，生成HTML报告并发送邮件。

**Architecture:** 独立验证脚本 `manual_verification.py`，基于现有数据验证器扩展，生成HTML报告，通过SMTP发送邮件。

**Tech Stack:** Python 3.11+, Polars, Jinja2(HTML模板), smtplib(邮件)

---

## Task 1: 创建HTML报告模板

**Files:**
- Create: `templates/verification_report.html`

**Step 1: 创建HTML模板文件**

```html
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{{ date }} 数据验证报告</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }
        .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .pass { color: green; font-weight: bold; }
        .fail { color: red; font-weight: bold; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #3498db; color: white; }
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 {{ date }} 数据验证报告</h1>
    </div>

    <div class="section">
        <h2>1. 数据概览</h2>
        <ul>
            <li>总股票数: {{ summary.total_stocks }}</li>
            <li>有效数据: {{ summary.valid_stocks }} ({{ summary.valid_rate }}%)</li>
            <li>最新日期: {{ summary.latest_date }} <span class="{{ summary.date_status }}">{{ summary.date_status_text }}</span></li>
        </ul>
    </div>

    <div class="section">
        <h2>2. 连续性检查</h2>
        <p>随机抽检股票: {{ continuity.sample_codes|join(', ') }}</p>
        <p>检查结果: <span class="{{ continuity.status }}">{{ continuity.status_text }}</span></p>
    </div>

    <div class="section">
        <h2>3. 人工验证区 (需人工确认)</h2>
        <table>
            <tr>
                <th>股票代码</th>
                <th>股票名称</th>
                <th>日期</th>
                <th>开盘</th>
                <th>收盘</th>
                <th>最高</th>
                <th>最低</th>
                <th>成交量</th>
                <th>人工确认</th>
            </tr>
            {% for stock in manual_verify %}
            <tr>
                <td>{{ stock.code }}</td>
                <td>{{ stock.name }}</td>
                <td>{{ stock.date }}</td>
                <td>{{ stock.open }}</td>
                <td>{{ stock.close }}</td>
                <td>{{ stock.high }}</td>
                <td>{{ stock.low }}</td>
                <td>{{ stock.volume }}</td>
                <td><input type="checkbox" id="verify_{{ stock.code }}"></td>
            </tr>
            {% endfor %}
        </table>
        <p><i>* 请人工核对上述数据，确认后勾选checkbox</i></p>
    </div>

    <div class="section">
        <h2>4. 第三方验证</h2>
        <ul>
            <li>验证股票: {{ third_party.code }}</li>
            <li>内部数据: 收盘 {{ third_party.internal_close }}</li>
            <li>第三方数据: 收盘 {{ third_party.external_close }} (来源: {{ third_party.source }})</li>
            <li>差异: {{ third_party.diff }}%</li>
            <li>结果: <span class="{{ third_party.status }}">{{ third_party.status_text }}</span></li>
        </ul>
    </div>

    <div class="section">
        <h2>5. 验证结论</h2>
        <p class="{{ conclusion.status }}">{{ conclusion.text }}</p>
    </div>
</body>
</html>
```

**Step 2: 提交**

```bash
git add templates/verification_report.html
git commit -m "feat: add HTML template for verification report"
```

---

## Task 2: 创建 manual_verification.py 主脚本

**Files:**
- Create: `scripts/manual_verification.py`

**Step 1: 编写脚本框架**

```python
#!/usr/bin/env python3
"""
数据人工验证脚本
每天晚上数据采集完成后执行，人工验证数据准确性
"""
import json
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import polars as pl

# 配置
DATA_DIR = Path("/app/data/kline")
REPORT_DIR = Path("/app/data/reports")
EMAIL_RECEIVER = "287363@qq.com"
EMAIL_USER = "287363@qq.com"
THIRD_PARTY_API = "akshare"  # 或 "eastmoney"

def get_all_stock_codes() -> List[str]:
    """获取所有股票代码"""
    files = list(DATA_DIR.glob("*.parquet"))
    return [f.stem for f in files]

def check_data_freshness() -> Dict:
    """检查数据新鲜度"""
    # 实现...
    pass

def check_data_continuity(codes: List[str]) -> Dict:
    """检查数据连续性"""
    # 实现...
    pass

def get_random_stocks(codes: List[str], n: int = 3) -> List[Dict]:
    """随机抽取n个股票"""
    # 实现...
    pass

def verify_with_third_party(code: str) -> Dict:
    """第三方数据验证"""
    # 实现...
    pass

def generate_html_report(data: Dict) -> str:
    """生成HTML报告"""
    # 实现...
    pass

def send_email(html_content: str):
    """发送邮件"""
    # 实现...
    pass

def main():
    # 1. 检查数据新鲜度
    # 2. 随机抽检3个股票
    # 3. 第三方验证1个股票
    # 4. 生成HTML报告
    # 5. 发送邮件
    pass

if __name__ == "__main__":
    main()
```

**Step 2: 提交**

```bash
git add scripts/manual_verification.py
git commit -m "feat: add manual verification script"
```

---

## Task 3: 实现数据新鲜度检查

**Files:**
- Modify: `scripts/manual_verification.py` (在 check_data_freshness 函数中)

**Step 1: 实现检查逻辑**

```python
def check_data_freshness() -> Dict:
    """检查数据新鲜度"""
    files = list(DATA_DIR.glob("*.parquet"))
    today = datetime.now().strftime("%Y-%m-%d")

    latest_dates = {}
    for f in files:
        try:
            df = pl.read_parquet(str(f))
            latest_dates[f.stem] = df["trade_date"].max()
        except:
            continue

    valid_count = sum(1 for d in latest_dates.values() if d == today)
    latest_count = len(latest_dates)

    return {
        "total_stocks": len(files),
        "valid_stocks": valid_count,
        "latest_date": today,
        "valid_rate": round(valid_count / latest_count * 100, 1) if latest_count else 0,
        "date_status": "pass" if valid_count > latest_count * 0.9 else "fail",
        "date_status_text": "✓ 通过" if valid_count > latest_count * 0.9 else "✗ 异常"
    }
```

**Step 3: 提交**

```bash
git add scripts/manual_verification.py
git commit -m "feat: implement data freshness check"
```

---

## Task 4: 实现随机抽检功能

**Files:**
- Modify: `scripts/manual_verification.py` (在 get_random_stocks 函数中)

**Step 1: 实现随机抽检逻辑**

```python
def get_stock_details(code: str) -> Dict:
    """获取股票详细信息"""
    file_path = DATA_DIR / f"{code}.parquet"
    df = pl.read_parquet(str(file_path))
    latest = df.sort("trade_date", descending=True).row(0)

    return {
        "code": code,
        "name": get_stock_name(code),  # 需要实现
        "date": str(latest[1]),
        "open": float(latest[2]),
        "close": float(latest[3]),
        "high": float(latest[4]),
        "low": float(latest[5]),
        "volume": int(latest[6])
    }

def get_random_stocks(codes: List[str], n: int = 3) -> List[Dict]:
    """随机抽取n个股票"""
    sample_codes = random.sample(codes, min(n, len(codes)))
    return [get_stock_details(code) for code in sample_codes]
```

---

## Task 5: 实现第三方验证

**Files:**
- Modify: `scripts/manual_verification.py` (在 verify_with_third_party 函数中)

**Step 1: 实现AKShare验证**

```python
def verify_with_akshare(code: str) -> Dict:
    """使用AKShare验证股票数据"""
    try:
        import akshare as ak

        today = datetime.now().strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(symbol=code, start_date=today, end_date=today)

        if len(df) > 0:
            external_close = float(df.iloc[0]["收盘"])
            return {
                "external_close": external_close,
                "source": "AKShare",
                "success": True
            }
    except Exception as e:
        return {"error": str(e), "success": False}

    return {"success": False}
```

---

## Task 6: 实现HTML报告生成和邮件发送

**Files:**
- Modify: `scripts/manual_verification.py` (在 generate_html_report 和 send_email 函数中)

**Step 1: 实现报告生成和发送**

```python
def generate_html_report(data: Dict) -> str:
    """生成HTML报告"""
    from jinja2 import Template

    template_path = Path(__file__).parent.parent / "templates" / "verification_report.html"
    with open(template_path, "r", encoding="utf-8") as f:
        template = Template(f.read())

    return template.render(**data)

def send_email(html_content: str):
    """发送邮件"""
    today = datetime.now().strftime("%Y-%m-%d")
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"📊 {today} 数据验证报告"

    msg.attach(MIMEText(html_content, "html", "utf-8"))

    with smtplib.SMTP_SSL("smtp.qq.com", 465) as server:
        server.login(EMAIL_USER, "your_auth_code")
        server.sendmail(EMAIL_USER, [EMAIL_RECEIVER], msg.as_string())
```

---

## Task 7: 更新定时任务配置

**Files:**
- Modify: `config/cron_tasks.yaml`

**Step 1: 添加验证任务**

```yaml
manual_verification:
  command: python scripts/manual_verification.py
  schedule: "0 20 * * *"
  description: 人工数据验证
  timeout: 1800
  env:
    PYTHONUNBUFFERED: "1"
```

---

## Task 8: 测试和验证

**Step 1: 本地测试**

```bash
docker exec xcnstock-scheduler python scripts/manual_verification.py
```

**Step 2: 检查HTML报告**

```bash
docker exec xcnstock-scheduler cat data/reports/verification_2026-04-03.html
```

**Step 3: 检查邮件发送**

检查 287363@qq.com 是否收到验证报告邮件

---

## 验收标准

- [ ] HTML报告能正确生成
- [ ] 随机3个股票数据展示正确
- [ ] 第三方验证API能返回数据
- [ ] 邮件能成功发送到 287363@qq.com
- [ ] 报告中的涨跌统计与实际一致
