#!/usr/bin/env python3
"""
数据人工验证 - 用户输入验证
用户输入股票的收盘价，与系统数据对比验证

使用方法:
    python scripts/manual_verify_input.py
    # 然后按提示输入3个股票的收盘价
"""
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import polars as pl

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path("/app/data/kline")
TASK_FILE = Path("/app/data/reports/verification_task.json")
RESULT_FILE = Path("/app/data/reports/verification_result.json")
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


def get_stock_name(code: str) -> str:
    """获取股票名称"""
    return STOCK_NAME_MAP.get(code, code)


def get_stock_data(code: str) -> Dict:
    """获取股票最新数据"""
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
        logger.error(f"获取{code}数据失败: {e}")
        return None


def save_verification_task(codes: List[str], task_id: str):
    """保存验证任务"""
    stocks = []
    for code in codes:
        data = get_stock_data(code)
        if data:
            stocks.append(data)

    task = {
        "task_id": task_id,
        "stocks": stocks,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "pending"
    }

    TASK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TASK_FILE, "w", encoding="utf-8") as f:
        json.dump(task, f, ensure_ascii=False, indent=2)

    logger.info(f"验证任务已保存: {TASK_FILE}")
    return task


def print_task_info(task: Dict):
    """打印任务信息"""
    print("\n" + "=" * 60)
    print("📋 数据人工验证任务")
    print("=" * 60)
    print(f"任务ID: {task['task_id']}")
    print(f"创建时间: {task['created_at']}")
    print("\n请人工核对以下股票数据，并在行情软件中查询最新收盘价：\n")

    for i, stock in enumerate(task['stocks'], 1):
        print(f"【股票 {i}】")
        print(f"  代码: {stock['code']} ({stock['name']})")
        print(f"  日期: {stock['date']}")
        print(f"  开盘: {stock['open']}")
        print(f"  收盘: {stock['close']}")
        print(f"  最高: {stock['high']}")
        print(f"  最低: {stock['low']}")
        print(f"  成交量: {stock['volume']}")
        print()

    print("-" * 60)
    print("请输入验证结果（对比行情软件中的实际收盘价）：")
    print("-" * 60)


def verify_input(task: Dict) -> Dict:
    """获取用户输入并验证"""
    results = []

    for i, stock in enumerate(task['stocks'], 1):
        print(f"\n【股票 {i}/3】 {stock['code']} ({stock['name']})")
        print(f"  系统收盘价: {stock['close']}")

        while True:
            try:
                user_input = input(f"  请输入实际收盘价（或直接回车确认一致）: ").strip()

                if user_input == "":
                    user_close = stock['close']
                    is_match = True
                else:
                    user_close = float(user_input)
                    diff = abs(user_close - stock['close'])
                    diff_pct = diff / stock['close'] * 100 if stock['close'] > 0 else 0
                    is_match = diff_pct < 0.1

                result = {
                    "code": stock['code'],
                    "name": stock['name'],
                    "system_close": stock['close'],
                    "user_close": user_close,
                    "match": is_match,
                    "diff_pct": round(diff_pct, 4) if not is_match else 0
                }
                results.append(result)

                if is_match:
                    print(f"  ✓ 验证通过")
                else:
                    print(f"  ✗ 不一致！差异: {diff:.2f} ({diff_pct:.2f}%)")

                break
            except ValueError:
                print("  输入无效，请输入数字")
            except KeyboardInterrupt:
                print("\n\n操作已取消")
                sys.exit(0)

    return results


def check_data_collect_if_needed(results: List[Dict]):
    """如果有不一致的股票，触发重新采集"""
    mismatched = [r for r in results if not r['match']]

    if not mismatched:
        logger.info("所有股票验证通过，无需重新采集")
        return

    print("\n" + "=" * 60)
    print("⚠️  以下股票数据不一致，需要重新采集：")
    print("=" * 60)

    for r in mismatched:
        print(f"  {r['code']} ({r['name']}): 系统={r['system_close']}, 实际={r['user_close']}")

    print("\n请手动执行数据重新采集：")
    print("  docker exec xcnstock-scheduler python /app/scripts/pipeline/data_collect.py --retry")

    logger.warning(f"发现 {len(mismatched)} 个股票数据不一致，需要重新采集")


def save_verification_result(task_id: str, results: List[Dict]):
    """保存验证结果"""
    all_match = all(r['match'] for r in results)

    result = {
        "task_id": task_id,
        "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "pass" if all_match else "fail",
        "all_match": all_match,
        "results": results
    }

    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    logger.info(f"验证结果已保存: {RESULT_FILE}")
    return result


def send_result_email(result: Dict):
    """发送验证结果邮件"""
    all_match = result['all_match']
    status = "✓ 验证通过" if all_match else "✗ 验证失败"

    content = f"""
数据验证结果报告
================

任务ID: {result['task_id']}
验证时间: {result['verified_at']}
验证状态: {status}

验证详情:
"""

    for r in result['results']:
        match_status = "✓ 通过" if r['match'] else "✗ 不一致"
        content += f"\n{r['code']} ({r['name']}): {match_status}"
        content += f"\n  系统收盘: {r['system_close']}"
        content += f"\n  实际收盘: {r['user_close']}"
        if not r['match']:
            content += f"\n  差异: {r['diff_pct']:.2f}%"

    if not all_match:
        content += "\n\n⚠️  存在不一致的数据，请检查并重新采集"

    msg = f"""Subject: {status} - 数据验证结果

{content}
"""

    try:
        import smtplib
        from email.mime.text import MIMEText

        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = f"数据验证结果 - {'通过' if all_match else '失败'}"

        if EMAIL_SMTP_PORT == 465:
            with smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
                server.login(EMAIL_USER, EMAIL_PASSWORD)
                server.sendmail(EMAIL_USER, [EMAIL_RECEIVER], msg.as_string())
        else:
            with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASSWORD)
                server.sendmail(EMAIL_USER, [EMAIL_RECEIVER], msg.as_string())

        logger.info("验证结果邮件已发送")
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")


def main():
    """主函数"""
    if not TASK_FILE.exists():
        print("错误：没有找到验证任务")
        print("请先运行 manual_verification.py 生成验证任务")
        return 1

    with open(TASK_FILE, "r", encoding="utf-8") as f:
        task = json.load(f)

    print(f"\n找到待验证任务: {task['task_id']}")
    print(f"创建时间: {task['created_at']}")

    task_id = task['task_id']

    print_task_info(task)
    results = verify_input(task)

    result = save_verification_result(task_id, results)

    check_data_collect_if_needed(results)

    send_result_email(result)

    print("\n" + "=" * 60)
    if result['all_match']:
        print("✅ 验证完成：所有数据一致")
    else:
        print("❌ 验证完成：存在不一致的数据")
    print("=" * 60)

    return 0 if result['all_match'] else 1


if __name__ == "__main__":
    exit(main())