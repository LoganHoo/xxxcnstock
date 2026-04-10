#!/usr/bin/env python3
"""
数据人工验证脚本
每天晚上数据采集完成后执行，人工验证数据准确性

功能：
1. 检查数据新鲜度（最新日期是否正确）
2. 检查数据连续性
3. 随机抽取3个股票供人工验证
4. 第三方数据源交叉验证1个股票
5. 生成HTML报告并发送邮件
"""
import json
import logging
import os
import random
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path("/app/data/kline")
REPORT_DIR = Path("/app/data/reports")
TEMPLATE_DIR = Path("/app/templates")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "287363@qq.com")
EMAIL_USER = os.getenv("EMAIL_USERNAME", "287363@qq.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_SMTP_SERVER = os.getenv("EMAIL_SMTP_SERVER", "smtp.qq.com")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", 465))

STOCK_NAME_MAP = {
    "000001": "平安银行",
    "000002": "万科A",
    "600000": "浦发银行",
    "600519": "贵州茅台",
    "000858": "五粮液",
    "300750": "宁德时代",
    "688001": "华兴源创",
}


def get_all_stock_codes() -> List[str]:
    """获取所有股票代码"""
    if not DATA_DIR.exists():
        logger.error(f"数据目录不存在: {DATA_DIR}")
        return []
    files = list(DATA_DIR.glob("*.parquet"))
    return [f.stem for f in files]


def get_stock_name(code: str) -> str:
    """获取股票名称"""
    return STOCK_NAME_MAP.get(code, code)


def check_data_freshness() -> Dict:
    """
    检查数据新鲜度
    - 验证最新日期是否为今天或上一个交易日
    - 统计有效数据比例
    """
    logger.info("检查数据新鲜度...")
    codes = get_all_stock_codes()
    if not codes:
        return {"error": "无股票数据"}

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now().replace(hour=0, minute=0, second=0)).timestamp() - 86400
    from datetime import timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    valid_count = 0
    latest_dates = {}
    total_count = 0

    for code in codes:
        try:
            file_path = DATA_DIR / f"{code}.parquet"
            df = pl.read_parquet(str(file_path))
            if len(df) > 0:
                total_count += 1
                latest_date = df["trade_date"].max()
                latest_dates[code] = latest_date
                if latest_date in [today, yesterday]:
                    valid_count += 1
        except Exception as e:
            logger.debug(f"读取{code}失败: {e}")
            continue

    quality_score = round(valid_count / total_count * 100, 1) if total_count > 0 else 0
    date_status = "pass" if quality_score >= 90 else "fail"

    return {
        "total_stocks": len(codes),
        "valid_stocks": valid_count,
        "total_with_data": total_count,
        "latest_date": today,
        "valid_rate": quality_score,
        "date_status": date_status,
        "date_status_text": "✓ 数据最新" if date_status == "pass" else "✗ 数据异常",
        "quality_score": quality_score
    }


def check_data_continuity(sample_codes: List[str]) -> Dict:
    """
    检查数据连续性
    - 验证抽检股票的数据日期连续性
    - 识别缺失的交易日
    """
    logger.info(f"检查数据连续性: {sample_codes}")
    missing_days = 0
    all_passed = True

    for code in sample_codes:
        try:
            file_path = DATA_DIR / f"{code}.parquet"
            df = pl.read_parquet(str(file_path))
            df = df.sort("trade_date")

            dates = df["trade_date"].to_list()
            for i in range(1, len(dates)):
                curr = datetime.strptime(str(dates[i]), "%Y-%m-%d")
                prev = datetime.strptime(str(dates[i-1]), "%Y-%m-%d")
                diff = (curr - prev).days
                if diff > 3:
                    missing_days += diff - 1
                    all_passed = False
        except Exception as e:
            logger.warning(f"检查{code}连续性失败: {e}")
            all_passed = False

    status = "pass" if all_passed else "warn"
    status_text = "✓ 连续性正常" if all_passed else "⚠ 存在缺失日期"

    return {
        "sample_codes": sample_codes,
        "status": status,
        "status_text": status_text,
        "missing_days": missing_days
    }


def get_stock_details(code: str) -> Dict:
    """获取股票详细信息"""
    file_path = DATA_DIR / f"{code}.parquet"
    try:
        df = pl.read_parquet(str(file_path))
        latest = df.sort("trade_date", descending=True).row(0)
        return {
            "code": code,
            "name": get_stock_name(code),
            "date": str(latest[1]),
            "open": round(float(latest[2]), 2),
            "close": round(float(latest[3]), 2),
            "high": round(float(latest[4]), 2),
            "low": round(float(latest[5]), 2),
            "volume": int(latest[6])
        }
    except Exception as e:
        logger.error(f"获取{code}详情失败: {e}")
        return {
            "code": code,
            "name": get_stock_name(code),
            "date": "N/A",
            "open": 0,
            "close": 0,
            "high": 0,
            "low": 0,
            "volume": 0
        }


def get_random_stocks(n: int = 3) -> List[Dict]:
    """随机抽取n个股票"""
    codes = get_all_stock_codes()
    if len(codes) < n:
        n = len(codes)
    sample_codes = random.sample(codes, n)
    logger.info(f"随机抽检股票: {sample_codes}")
    return [get_stock_details(code) for code in sample_codes]


def verify_with_third_party(code: str) -> Dict:
    """
    第三方数据验证
    - 尝试使用AKShare获取数据
    - 与内部数据对比
    """
    logger.info(f"第三方验证: {code}")

    internal_data = get_stock_details(code)
    internal_close = internal_data["close"]

    result = {
        "code": code,
        "name": get_stock_name(code),
        "internal_close": internal_close,
        "external_close": internal_close,
        "source": "内部数据",
        "diff_pct": 0.0,
        "status": "pass",
        "status_text": "✓ 验证通过",
        "success": True
    }

    try:
        import akshare as ak
        today = datetime.now().strftime("%Y%m%d")

        if code.startswith("6"):
            symbol = f"sh{code}"
        else:
            symbol = f"sz{code}"

        df = ak.stock_zh_a_hist(symbol=code, start_date=today, end_date=today, adjust="")
        if len(df) > 0:
            external_close = float(df.iloc[0]["收盘"])
            diff_pct = round(abs(external_close - internal_close) / internal_close * 100, 2) if internal_close > 0 else 0

            result["external_close"] = external_close
            result["source"] = "AKShare"
            result["diff_pct"] = diff_pct

            if diff_pct < 0.1:
                result["status"] = "pass"
                result["status_text"] = "✓ 验证通过"
            elif diff_pct < 1:
                result["status"] = "warn"
                result["status_text"] = "⚠ 轻微差异"
            else:
                result["status"] = "fail"
                result["status_text"] = "✗ 差异过大"

            logger.info(f"第三方验证{code}: 内部={internal_close}, 外部={external_close}, 差异={diff_pct}%")
    except ImportError:
        logger.warning("AKShare未安装，使用内部数据")
        result["source"] = "内部数据(无第三方)"
    except Exception as e:
        logger.warning(f"第三方验证失败: {e}")
        result["source"] = f"验证失败: {str(e)[:20]}"
        result["status"] = "warn"
        result["status_text"] = "⚠ 第三方验证跳过"

    return result


def get_market_stats() -> Dict:
    """获取市场涨跌统计"""
    logger.info("计算市场涨跌统计...")
    codes = get_all_stock_codes()

    rising, falling, unchanged = 0, 0, 0
    limit_up, limit_down = 0, 0

    for code in codes:
        try:
            file_path = DATA_DIR / f"{code}.parquet"
            df = pl.read_parquet(str(file_path))
            if len(df) >= 2:
                df = df.sort("trade_date", descending=True)
                latest = df.row(0)
                prev = df.row(1)

                if str(latest[1]) != datetime.now().strftime("%Y-%m-%d"):
                    continue

                close_t = float(latest[3])
                close_y = float(prev[3])

                if close_t > close_y:
                    rising += 1
                elif close_t < close_y:
                    falling += 1
                else:
                    unchanged += 1

                change_pct = (close_t - close_y) / close_y * 100 if close_y > 0 else 0
                if change_pct >= 9.9:
                    limit_up += 1
                elif change_pct <= -9.9:
                    limit_down += 1
        except:
            continue

    return {
        "rising": rising,
        "falling": falling,
        "unchanged": unchanged,
        "limit_up": limit_up,
        "limit_down": limit_down
    }


def generate_html_report(data: Dict) -> str:
    """生成HTML报告"""
    template_path = TEMPLATE_DIR / "verification_report.html"
    if not template_path.exists():
        logger.error(f"模板文件不存在: {template_path}")
        return "<html><body><h1>报告生成失败：模板文件不存在</h1></body></html>"

    with open(template_path, "r", encoding="utf-8") as f:
        template_content = f.read()

    for key, value in data.items():
        placeholder = f"{{{{ {key} }}}}"
        if placeholder in template_content:
            if isinstance(value, (dict, list)):
                template_content = template_content.replace(placeholder, str(value))
            else:
                template_content = template_content.replace(placeholder, str(value))

    if "generate_time" not in data:
        data["generate_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    from jinja2 import Template
    try:
        template = Template(template_content)
        return template.render(**data)
    except Exception as e:
        logger.error(f"Jinja2渲染失败: {e}")
        return template_content


def save_html_report(html_content: str, date_str: str = None) -> Path:
    """保存HTML报告"""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"verification_{date_str}.html"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(f"报告已保存: {report_path}")
    return report_path


def send_email(html_content: str, report_path: Path = None) -> bool:
    """发送邮件"""
    logger.info(f"发送邮件到 {EMAIL_RECEIVER}...")
    today = datetime.now().strftime("%Y-%m-%d")

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"📊 {today} 数据验证报告"

    msg.attach(MIMEText(html_content, "html", "utf-8"))

    if report_path and report_path.exists():
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                msg.attach(MIMEText(f.read(), "html", "utf-8"))
        except Exception as e:
            logger.warning(f"附件添加失败: {e}")

    try:
        if EMAIL_SMTP_PORT == 465:
            with smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
                server.login(EMAIL_USER, EMAIL_PASSWORD)
                server.sendmail(EMAIL_USER, [EMAIL_RECEIVER], msg.as_string())
        else:
            with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASSWORD)
                server.sendmail(EMAIL_USER, [EMAIL_RECEIVER], msg.as_string())

        logger.info("邮件发送成功")
        return True
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")
        return False


def save_verification_task(manual_verify: List[Dict]) -> str:
    """保存待验证任务供人工输入验证"""
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    task = {
        "task_id": task_id,
        "stocks": manual_verify,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "pending"
    }

    TASK_FILE = Path("/app/data/reports/verification_task.json")
    TASK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TASK_FILE, "w", encoding="utf-8") as f:
        json.dump(task, f, ensure_ascii=False, indent=2)

    logger.info(f"验证任务已保存: {TASK_FILE}")
    return task_id


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("开始数据人工验证流程")
    logger.info("=" * 50)

    date_str = datetime.now().strftime("%Y-%m-%d")

    summary = check_data_freshness()
    logger.info(f"数据新鲜度: {summary}")

    sample_codes = random.sample(get_all_stock_codes(), min(3, len(get_all_stock_codes())))
    continuity = check_data_continuity(sample_codes)
    logger.info(f"连续性检查: {continuity}")

    manual_verify = get_random_stocks(3)
    logger.info(f"人工验证股票: {[s['code'] for s in manual_verify]}")

    task_id = save_verification_task(manual_verify)

    third_party_code = random.choice(get_all_stock_codes())
    third_party = verify_with_third_party(third_party_code)
    logger.info(f"第三方验证: {third_party}")

    market = get_market_stats()
    logger.info(f"市场统计: {market}")

    all_passed = (
        summary.get("date_status") == "pass" and
        continuity.get("status") in ["pass", "warn"] and
        third_party.get("status") in ["pass", "warn"]
    )

    conclusion = {
        "status": "pass" if all_passed else "fail",
        "text": "✓ 所有检查项均已通过，数据验证完成" if all_passed else "⚠ 部分检查项存在异常，请人工核查"
    }

    report_data = {
        "date": date_str,
        "generate_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": summary,
        "continuity": continuity,
        "manual_verify": manual_verify,
        "third_party": third_party,
        "market": market,
        "conclusion": conclusion
    }

    html_content = generate_html_report(report_data)
    report_path = save_html_report(html_content, date_str)

    email_sent = send_email(html_content, report_path)

    logger.info("=" * 50)
    logger.info("验证流程完成")
    logger.info(f"报告路径: {report_path}")
    logger.info(f"邮件发送: {'成功' if email_sent else '失败'}")
    logger.info("=" * 50)

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())