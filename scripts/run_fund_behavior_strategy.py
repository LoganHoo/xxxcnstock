"""
资金行为学策略执行脚本
================================================================================

【策略架构 - 断点流水线】
--------------------------------------------------------------------------------
阶段        组件                      职责                    断点文件
--------------------------------------------------------------------------------
加载        load_data()              批量加载K线数据         *_load.parquet
验证        validate_data()          数据质量检查            *_validate.json
变换        calculate_factors()      计算市场因子            *_factor.parquet
暂存        buffer_factors()         因子持久化              *_factor.parquet
执行        execute_strategy()       策略信号生成            *_execute.json
分发        distribute_results()     MySQL/HTML/邮件        *_distribute.json

【断点机制】
- 每个阶段完成后保存状态到 JSON 文件
- 支持从断点恢复，跳过已完成的阶段
- 使用 --reset 参数强制从头开始

================================================================================
"""
import sys
import yaml
import logging
import argparse
import importlib
import gc
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.config import get_settings
from core.pipeline_state import PipelineStateManager, PipelineState, get_pipeline_manager
from core.data_quality_checker import DataQualityChecker
from filters.base_filter import FilterRegistry

DEFAULT_CONFIG_PATH = Path("config/strategies/fund_behavior_config.yaml")
DEFAULT_FILTER_CONFIG = Path("config/filters/fund_behavior_filters.yaml")

FundBehaviorStrategyEngine = importlib.import_module(
    "core.fund_behavior_strategy"
).FundBehaviorStrategyEngine

config_manager = importlib.import_module("core.fund_behavior_config").config_manager
FactorEngine = importlib.import_module("core.factor_engine").FactorEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


def resolve_project_path(path_value):
    if path_value is None:
        return None
    path = Path(path_value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def sync_runtime_config(config):
    config_manager.config = config or {}


def load_config(config_file=str(DEFAULT_CONFIG_PATH)):
    resolved_config_path = resolve_project_path(config_file)
    with open(resolved_config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    sync_runtime_config(config)
    logger.info(f"配置文件: {resolved_config_path}")
    return config


# =============================================================================
# 第一步：加载 (Load) - 批量加载K线数据
# =============================================================================
def load_data(data_path: str = None, batch_size: int = 500, target_columns: list = None) -> Tuple[pl.DataFrame, dict]:
    """极速并行加载K线数据

    Args:
        data_path: 数据路径
        batch_size: 每批文件数量
        target_columns: 目标列名列表（可配置）

    Returns:
        (数据, 元信息)
    """
    logger.info("=" * 60)
    logger.info("[LOAD] 阶段1: Polars并行加载K线数据")
    logger.info("=" * 60)

    if target_columns is None:
        target_columns = ["code", "trade_date", "open", "close", "high", "low", "volume"]

    float_cols = ['open', 'high', 'low', 'close', 'volume']
    kline_dir = resolve_project_path(data_path or 'data/kline')
    parquet_files = list(kline_dir.glob('*.parquet'))
    total_files = len(parquet_files)

    logger.info(f"发现 {total_files} 个股票数据文件，每批 {batch_size} 个")

    import concurrent.futures

    def read_single_parquet(f):
        try:
            df = pl.read_parquet(f)
            df = df.select([pl.col(c) for c in target_columns if c in df.columns])
            for col in float_cols:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
            return df
        except Exception as e:
            return None

    all_dfs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(read_single_parquet, f): f for f in parquet_files}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                all_dfs.append(result)

    if not all_dfs:
        raise RuntimeError("没有可用的有效数据文件")

    logger.info(f"[LOAD] 合并 {len(all_dfs)} 个DataFrame...")
    data = pl.concat(all_dfs)

    meta = {
        'total_rows': len(data),
        'total_stocks': data['code'].n_unique(),
        'date_range': f"{data['trade_date'].min()} ~ {data['trade_date'].max()}",
        'batch_count': 1,
        'columns': list(data.columns)
    }

    logger.info(f"[LOAD] 完成: {meta['total_rows']}行, {meta['total_stocks']}只股票")
    return data, meta


def _load_data_batch(data_path: str = None, batch_size: int = 500, target_columns: list = None) -> Tuple[pl.DataFrame, dict]:
    """批量加载模式（降级方案）"""
    logger.info("=" * 60)
    logger.info("[LOAD] 阶段1: 批量加载K线数据（降级模式）")
    logger.info("=" * 60)

    if target_columns is None:
        target_columns = ["code", "trade_date", "open", "close", "high", "low", "volume"]

    kline_dir = resolve_project_path(data_path or 'data/kline')
    parquet_files = list(kline_dir.glob('*.parquet'))
    total_files = len(parquet_files)

    logger.info(f"发现 {total_files} 个股票数据文件")

    all_dfs = []
    batch_count = 0

    for i in range(0, total_files, batch_size):
        batch = parquet_files[i:i + batch_size]
        batch_count += 1
        batch_dfs = []

        for f in batch:
            try:
                df = pl.read_parquet(f)
                df = df.select([pl.col(c) for c in target_columns if c in df.columns])
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df = df.with_columns(pl.col(col).cast(pl.Float64))
                batch_dfs.append(df)
            except Exception as e:
                logger.warning(f"跳过 {f.name}: {e}")

        if batch_dfs:
            all_dfs.extend(batch_dfs)
            logger.info(f"  批次 {batch_count}: 已加载 {len(batch_dfs)}/{len(batch)} 文件")

    if not all_dfs:
        raise RuntimeError("没有可用的有效数据文件")

    data = pl.concat(all_dfs)
    meta = {
        'total_rows': len(data),
        'total_stocks': data['code'].n_unique(),
        'date_range': f"{data['trade_date'].min()} ~ {data['trade_date'].max()}",
        'batch_count': batch_count
    }

    logger.info(f"[LOAD] 完成: {meta['total_rows']}行, {meta['total_stocks']}只股票")
    return data, meta


# =============================================================================
# 第二步：验证 (Validate) - 数据质量检查
# =============================================================================
def validate_data(data: pl.DataFrame) -> Tuple[bool, dict]:
    """验证数据质量

    Args:
        data: K线数据

    Returns:
        (是否通过, 检查结果)
    """
    logger.info("=" * 60)
    logger.info("[VALIDATE] 阶段2: 数据质量检查")
    logger.info("=" * 60)

    quality_checker = DataQualityChecker()
    results = quality_checker.check_all(data)

    passed = results['passed']
    issues = results.get('issues', [])
    warnings = results.get('warnings', [])

    if passed:
        logger.info("[VALIDATE] ✅ 数据质量检查通过")
    else:
        logger.warning(f"[VALIDATE] ⚠️ 数据质量检查未通过: {len(issues)}个问题")

    for issue in issues[:5]:
        logger.warning(f"  - {issue}")

    for w in warnings:
        logger.warning(f"  ⚠️ {w}")

    return passed, results


# =============================================================================
# 第三步：变换 (Transform) - 因子计算
# =============================================================================
def calculate_factors(data: pl.DataFrame, config: dict) -> Tuple[pl.DataFrame, dict]:
    """计算市场因子

    Args:
        data: K线数据
        config: 配置字典

    Returns:
        (因子数据, 元信息)
    """
    logger.info("=" * 60)
    logger.info("[TRANSFORM] 阶段3: 因子计算")
    logger.info("=" * 60)

    factor_engine = FactorEngine()
    factors = config.get('factors', {})
    factor_names = []

    if isinstance(factors, dict):
        for key, value in factors.items():
            if isinstance(value, dict) and value.get('enabled', True):
                factor_names.append(key)

    if not factor_names:
        factor_names = ["v_ratio10", "v_total", "cost_peak", "limit_up_score",
                        "pioneer_status", "ma5_bias"]

    logger.info(f"[TRANSFORM] 计算因子: {factor_names}")

    factor_data = factor_engine.calculate_all_factors(data, factor_names)

    meta = {
        'factor_count': len(factor_names),
        'factor_names': factor_names,
        'data_shape': list(factor_data.shape)
    }

    logger.info(f"[TRANSFORM] 完成: {factor_data.shape}")
    return factor_data, meta


# =============================================================================
# 第四步：暂存 (Buffer) - 因子持久化
# =============================================================================
def buffer_factors(factor_data: pl.DataFrame, pipeline: PipelineStateManager) -> str:
    """将因子数据写入磁盘缓存

    Args:
        factor_data: 因子数据
        pipeline: 流水线状态管理器

    Returns:
        缓存文件路径
    """
    logger.info("=" * 60)
    logger.info("[BUFFER] 阶段4: 因子数据暂存")
    logger.info("=" * 60)

    buffer_path = pipeline.get_checkpoint_path("factor", ".parquet")
    factor_data.write_parquet(buffer_path)

    file_size_mb = buffer_path.stat().st_size / 1024 / 1024
    logger.info(f"[BUFFER] 已保存: {buffer_path} ({file_size_mb:.2f} MB)")

    del factor_data
    gc.collect()

    return str(buffer_path)


def load_buffered_factors(pipeline: PipelineStateManager) -> Optional[pl.DataFrame]:
    """从缓存加载因子数据"""
    buffer_path = pipeline.get_checkpoint_path("factor", ".parquet")
    if buffer_path.exists():
        logger.info(f"[BUFFER] 从缓存恢复: {buffer_path}")
        return pl.read_parquet(buffer_path)
    return None


# =============================================================================
# 第五步：执行 (Execute) - 策略信号生成
# =============================================================================
def execute_strategy(factor_data: pl.DataFrame, config: dict, pipeline: PipelineStateManager) -> Tuple[dict, dict]:
    """执行资金行为学策略

    Args:
        factor_data: 因子数据
        config: 配置字典
        pipeline: 流水线状态管理器

    Returns:
        (策略结果, 元信息)
    """
    logger.info("=" * 60)
    logger.info("[EXECUTE] 阶段5: 策略执行")
    logger.info("=" * 60)

    filter_config_path = resolve_project_path(str(DEFAULT_FILTER_CONFIG))
    applied_filters = []

    if filter_config_path.exists() and len(factor_data) > 0:
        try:
            with open(filter_config_path, 'r', encoding='utf-8') as f:
                filter_config = yaml.safe_load(f)

            filters_config = filter_config.get('filters', {})
            filter_order = filter_config.get('filter_order', list(filters_config.keys()))
            original_count = len(factor_data)

            for filter_name in filter_order:
                filter_params = filters_config.get(filter_name, {})
                if not filter_params.get('enabled', False):
                    continue

                filter_class = FilterRegistry.get(filter_name)
                if not filter_class:
                    continue

                try:
                    filter_instance = filter_class(params=filter_params)
                    if not filter_instance.is_enabled():
                        continue

                    before_count = len(factor_data)
                    factor_data = filter_instance.filter(factor_data)
                    after_count = len(factor_data)

                    if before_count != after_count:
                        applied_filters.append({
                            'name': filter_name,
                            'removed': before_count - after_count
                        })
                        logger.info(f"[FILTER] {filter_name}: {before_count} -> {after_count}")
                except Exception as e:
                    logger.warning(f"[FILTER] {filter_name} 失败: {e}")

            logger.info(f"[FILTER] 完成: {original_count} -> {len(factor_data)} (移除 {original_count - len(factor_data)})")

        except Exception as e:
            logger.warning(f"[FILTER] 配置加载失败: {e}")

    total_capital = config.get('backtest', {}).get('initial_capital', 1000000)
    current_time = datetime.now().strftime("%H:%M")

    strategy_engine = FundBehaviorStrategyEngine()
    result = strategy_engine.execute_strategy(factor_data, total_capital, current_time)

    meta = {
        'applied_filters': applied_filters,
        'trend_count': len(result.get('trend_stocks', [])),
        'short_term_count': len(result.get('short_term_stocks', [])),
        'market_state': result.get('market_state', [])
    }

    logger.info(f"[EXECUTE] 完成: 波段{len(result.get('trend_stocks', []))}只, 短线{len(result.get('short_term_stocks', []))}只")

    return result, meta


# =============================================================================
# 第六步：分发 (Distribute) - MySQL/HTML/邮件
# =============================================================================
def distribute_results(result: dict, report_text: str, pipeline: PipelineStateManager) -> Tuple[bool, dict]:
    """分发结果到各个渠道

    Args:
        result: 策略结果
        report_text: 报告文本
        pipeline: 流水线状态管理器

    Returns:
        (是否成功, 分发结果)
    """
    logger.info("=" * 60)
    logger.info("[DISTRIBUTE] 阶段6: 结果分发")
    logger.info("=" * 60)

    report_date = pipeline.report_date
    success_count = 0
    fail_count = 0
    results = {}

    from services.report_db_service import ReportDBService
    from services.fund_behavior_db_service import FundBehaviorDBService

    db_service = None
    fb_db_service = None

    try:
        db_service = ReportDBService()
        db_service.init_tables()
        logger.info("[DISTRIBUTE] MySQL通用服务已连接")
    except (ConnectionError, OSError) as e:
        logger.error(f"[DISTRIBUTE] MySQL通用服务连接失败: {e}")
        db_service = None

    try:
        fb_db_service = FundBehaviorDBService()
        fb_db_service.init_tables()
        logger.info("[DISTRIBUTE] MySQL专业服务已连接")
    except (ConnectionError, OSError) as e:
        logger.error(f"[DISTRIBUTE] MySQL专业服务连接失败: {e}")
        fb_db_service = None

    subject = f"【量化决策】资金行为学策略 - {report_date}"
    result_json = {
        'market_state': result.get('market_state', []),
        'v_total': result.get('v_total', 0),
        'sentiment_temperature': result.get('sentiment_temperature', 0),
        'upward_pivot': result.get('upward_pivot', False),
        'position_size': result.get('position_size', {}),
        'hedge_effect': result.get('hedge_effect', False),
        'trend_stocks': result.get('trend_stocks', []),
        'short_term_stocks': result.get('short_term_stocks', [])
    }

    try:
        if db_service:
            try:
                db_service.save_report('fund_behavior', report_date, subject, report_text, result_json)
                results['mysql_general'] = 'success'
                success_count += 1
                logger.info("[DISTRIBUTE] ✅ MySQL通用报告已保存")
            except Exception as e:
                results['mysql_general'] = f'failed: {e}'
                fail_count += 1
                logger.error(f"[DISTRIBUTE] ❌ MySQL通用报告失败: {e}")
            finally:
                if db_service:
                    try:
                        db_service.close()
                        logger.info("[DISTRIBUTE] MySQL通用服务连接已关闭")
                    except Exception:
                        pass

        if fb_db_service:
            try:
                fb_db_service.save_daily_report(report_date, result_json)
                results['mysql_fund_behavior'] = 'success'
                success_count += 1
                logger.info("[DISTRIBUTE] ✅ MySQL专业报告已保存")
            except Exception as e:
                results['mysql_fund_behavior'] = f'failed: {e}'
                fail_count += 1
                logger.error(f"[DISTRIBUTE] ❌ MySQL专业报告失败: {e}")
            finally:
                if fb_db_service:
                    try:
                        fb_db_service.close()
                        logger.info("[DISTRIBUTE] MySQL专业服务连接已关闭")
                    except Exception:
                        pass
    except Exception as e:
        logger.error(f"[DISTRIBUTE] 数据库操作异常: {e}")

    try:
        txt_dir = resolve_project_path('data/reports')
        txt_path = txt_dir / f"fund_behavior_{report_date}.txt"
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        results['txt'] = 'success'
        success_count += 1
        logger.info(f"[DISTRIBUTE] ✅ TXT已保存: {txt_path}")
    except Exception as e:
        results['txt'] = f'failed: {e}'
        fail_count += 1
        logger.error(f"[DISTRIBUTE] ❌ TXT保存失败: {e}")

    try:
        from services.notify_service.templates.fund_behavior_report_template import generate_fund_behavior_html
        html_content = generate_fund_behavior_html(result)
        html_dir = resolve_project_path('data/reports/html')
        html_path = html_dir / f"fund_behavior_{report_date}.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        results['html'] = 'success'
        success_count += 1
        logger.info(f"[DISTRIBUTE] ✅ HTML已保存: {html_path}")
    except ImportError as e:
        results['html'] = f'failed: {e}'
        fail_count += 1
        logger.error(f"[DISTRIBUTE] ❌ HTML模板导入失败: {e}")
    except Exception as e:
        results['html'] = f'failed: {e}'
        fail_count += 1
        logger.error(f"[DISTRIBUTE] ❌ HTML保存失败: {e}")

    logger.info(f"[DISTRIBUTE] 完成: {success_count}成功, {fail_count}失败")
    return fail_count == 0, results


def send_email_notification(result: dict, report_text: str) -> bool:
    """发送邮件通知"""
    import concurrent.futures

    try:
        from services.notify_service.templates.fund_behavior_report_template import generate_fund_behavior_html
        from services.email_sender import EmailSender

        report_date = datetime.now().strftime('%Y-%m-%d')
        subject = f"【量化决策】资金行为学策略 - {report_date}"
        html_content = generate_fund_behavior_html(result)

        def _send_email():
            import os
            from dotenv import load_dotenv
            load_dotenv()
            
            import nacos
            settings = get_settings()
            client = nacos.NacosClient(
                settings.NACOS_SERVER_ADDR,
                namespace=settings.NACOS_NAMESPACE,
                username=settings.NACOS_USERNAME,
                password=settings.NACOS_PASSWORD or ""
            )
            xcomm_content = client.get_config("xcomm.yaml", "DEFAULT_GROUP")
            
            use_env_fallback = False
            
            if xcomm_content:
                import yaml
                cfg = yaml.safe_load(xcomm_content)
                email_cfg = cfg.get('email', {})
                recipients = email_cfg.get('notification', {}).get('emails', [])
                
                smtp_cfg = email_cfg.get('smtp', {})
                sender = EmailSender(
                    smtp_host=smtp_cfg.get('server'),
                    smtp_port=smtp_cfg.get('port', 465),
                    smtp_user=smtp_cfg.get('username'),
                    smtp_password=smtp_cfg.get('password'),
                    use_ssl=True
                )
                logger.info("[EMAIL] 从Nacos加载邮件配置成功")
            else:
                use_env_fallback = True
                logger.info("[EMAIL] Nacos配置不存在，使用.env配置")
                recipients = [os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')]
                sender = EmailSender(
                    smtp_host=os.getenv('EMAIL_SMTP_SERVER', 'smtp.qq.com'),
                    smtp_port=int(os.getenv('EMAIL_SMTP_PORT', 465)),
                    smtp_user=os.getenv('EMAIL_USERNAME', ''),
                    smtp_password=os.getenv('EMAIL_PASSWORD', ''),
                    use_ssl=True
                )
                logger.info("[EMAIL] 从.env加载邮件配置成功")

            if not recipients:
                recipients = ["287363@qq.com"]

            return sender.send(
                to_addrs=recipients,
                subject=subject,
                content=report_text,
                html_content=html_content
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_send_email)
            success = future.result(timeout=30)

        if success:
            logger.info("[EMAIL] ✅ 邮件已发送")
        else:
            logger.warning("[EMAIL] ⚠️ 邮件发送返回失败")

        return success

    except concurrent.futures.TimeoutError:
        logger.error("[EMAIL] ⏰ 邮件发送超时(30秒)")
        return False
    except ImportError as e:
        logger.error(f"[EMAIL] 模块导入失败: {e}")
        return False
    except Exception as e:
        logger.error(f"[EMAIL] 发送失败: {e}")
        return False


# =============================================================================
# 主流水线
# =============================================================================
def run_pipeline(args, config):
    """执行完整流水线

    状态流转（while循环确保每个状态都被正确处理）:
    START -> LOADED -> VALIDATED -> TRANSFORMED -> BUFFERED -> EXECUTED -> DISTRIBUTED -> DONE

    断点恢复时，从上次中断的状态继续执行。
    """
    report_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    pipeline = get_pipeline_manager(report_date)

    if args.reset:
        pipeline.reset()
        pipeline.cleanup_checkpoints()
        logger.info("=" * 60)
        logger.info("[RESET] 已重置流水线状态")
        logger.info("=" * 60)

    logger.info("=" * 60)
    logger.info(f"资金行为学策略流水线 | 日期: {report_date}")
    logger.info("=" * 60)

    load_meta = None
    result = None

    if pipeline.state == PipelineState.FAILED:
        logger.error("❌ 流水线处于失败状态，请使用 --reset 重新开始")
        return None

    result = None
    factor_data = None

    try:
        while pipeline.state not in (PipelineState.DONE, PipelineState.FAILED):
            current_state = pipeline.state

            if current_state == PipelineState.START:
                data, load_meta = load_data(args.data)
                pipeline.transition(PipelineState.LOADED, "load", load_meta)

                valid, validate_meta = validate_data(data)
                if not valid:
                    logger.warning("[VALIDATE] 数据验证未通过，继续执行...")
                pipeline.transition(PipelineState.VALIDATED, "validate", validate_meta)

                factor_data, transform_meta = calculate_factors(data, config)
                del data
                gc.collect()
                pipeline.transition(PipelineState.TRANSFORMED, "transform", transform_meta)

                buffer_path = buffer_factors(factor_data, pipeline)
                pipeline.transition(PipelineState.BUFFERED, "buffer", {'path': buffer_path})

                result, execute_meta = execute_strategy(factor_data, config, pipeline)
                del factor_data
                gc.collect()
                pipeline.transition(PipelineState.EXECUTED, "execute", execute_meta)

                execute_result_path = pipeline.get_checkpoint_path("execute_result", ".json")
                import json
                with open(execute_result_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, default=str)
                logger.info(f"[EXECUTE] 结果已持久化: {execute_result_path}")

                load_meta = pipeline.get_step_result("load") or {}
                report_text = generate_decision_report(result, load_meta, config)

                if not args.save_html_only:
                    distribute_success, distribute_meta = distribute_results(result, report_text, pipeline)
                    pipeline.transition(PipelineState.DISTRIBUTED, "distribute", distribute_meta)

                    if not args.no_email:
                        email_success = send_email_notification(result, report_text)
                        if email_success:
                            logger.info("[EMAIL] ✅ 邮件发送成功")
                else:
                    pipeline.transition(PipelineState.DISTRIBUTED, "distribute", {'mode': 'html_only'})

                pipeline.transition(PipelineState.DONE, "done", {'report_date': report_date})

            elif current_state == PipelineState.EXECUTED:
                execute_result_path = pipeline.get_checkpoint_path("execute_result", ".json")
                if execute_result_path.exists():
                    import json
                    with open(execute_result_path, 'r', encoding='utf-8') as f:
                        result = json.load(f)
                    logger.info("[RESUME] 已从断点恢复: execute阶段")
                else:
                    raise RuntimeError("无法找到执行结果缓存，请使用 --reset 重新开始")

                load_meta = pipeline.get_step_result("load") or {}
                report_text = generate_decision_report(result, load_meta, config)

                if not args.save_html_only:
                    distribute_success, distribute_meta = distribute_results(result, report_text, pipeline)
                    pipeline.transition(PipelineState.DISTRIBUTED, "distribute", distribute_meta)

                    if not args.no_email:
                        email_success = send_email_notification(result, report_text)
                        if email_success:
                            logger.info("[EMAIL] ✅ 邮件发送成功")
                else:
                    pipeline.transition(PipelineState.DISTRIBUTED, "distribute", {'mode': 'html_only'})

                pipeline.transition(PipelineState.DONE, "done", {'report_date': report_date})

            elif current_state == PipelineState.DISTRIBUTED:
                logger.info("[RESUME] 已从断点恢复: distribute阶段")
                pipeline.transition(PipelineState.DONE, "done", {'report_date': report_date})

            else:
                raise RuntimeError(f"未知状态: {current_state}，请使用 --reset 重新开始")

        logger.info("=" * 60)
        logger.info(f"✅ 流水线执行完成: {report_date}")
        logger.info("=" * 60)

        return result

    except Exception as e:
        step = pipeline.state.value
        pipeline.mark_failed(step, str(e))
        logger.error(f"❌ 流水线失败于 [{step}]: {e}")
        raise


# =============================================================================
# 辅助函数：生成决策报告
# =============================================================================
def generate_decision_report(result: dict, load_meta: dict = None, config: dict = None) -> str:
    """生成量化决策报告

    Args:
        result: 策略执行结果
        load_meta: 加载元信息（可选，用于显示数据概览）
        config: 配置字典

    Returns:
        str: 格式化的报告文本
    """
    report_lines = []
    report_lines.append("\n" + "=" * 70)
    report_lines.append(f"【量化决策报告 {datetime.now().strftime('%Y-%m-%d')}】")
    report_lines.append("=" * 70)

    v_total = result.get('v_total', 0.0)
    sentiment_temp = result.get('sentiment_temperature', 0.0)
    delta_temp = result.get('delta_temperature', 0.0)

    report_lines.append("\n【1. 市场环境定性】")
    report_lines.append("-" * 50)

    market_states = result.get('market_state', [])
    if market_states:
        state_counts = {}
        for s in market_states:
            state_counts[s] = state_counts.get(s, 0) + 1
        dominant_state = max(state_counts, key=state_counts.get)
        report_lines.append(f"  ● 周期定位：{dominant_state.upper()}")

    report_lines.append(f"  ● 量能判定：{v_total:.0f}亿元")
    report_lines.append(f"  ● 情绪温度：{sentiment_temp:.1f}°")
    report_lines.append(f"  ● 温差惯性：{delta_temp:+.1f}°")

    report_lines.append("\n【2. 核心观察点】")
    report_lines.append("-" * 50)
    report_lines.append(f"  ● 向上变盘：{'是' if result.get('upward_pivot') else '否'}")
    report_lines.append(f"  ● 对冲效果：{'是' if result.get('hedge_effect') else '否'}")
    report_lines.append(f"  ● 强势区域：{'是' if result.get('is_strong_region') else '否'}")

    report_lines.append("\n【3. 选股结果】")
    report_lines.append("-" * 50)
    trend_stocks = result.get('trend_stocks', [])
    short_term_stocks = result.get('short_term_stocks', [])
    report_lines.append(f"  ● 波段趋势：{len(trend_stocks)}只")
    report_lines.append(f"  ● 短线打板：{len(short_term_stocks)}只")

    position = result.get('position_size', {})
    report_lines.append("\n【4. 仓位分配】")
    report_lines.append("-" * 50)
    report_lines.append(f"  ● 波段仓位：{position.get('trend', 0)/10000:.0f}万")
    report_lines.append(f"  ● 短线仓位：{position.get('short_term', 0)/10000:.0f}万")
    report_lines.append(f"  ● 现金储备：{position.get('cash', 0)/10000:.0f}万")

    if load_meta:
        report_lines.append("\n【5. 数据概览】")
        report_lines.append("-" * 50)
        report_lines.append(f"  ● 股票数量：{load_meta.get('total_stocks', 'N/A')}只")
        report_lines.append(f"  ● 数据行数：{load_meta.get('total_rows', 'N/A')}行")
        report_lines.append(f"  ● 日期范围：{load_meta.get('date_range', 'N/A')}")

    report_lines.append("\n" + "=" * 70)
    return "\n".join(report_lines)


def main():
    parser = argparse.ArgumentParser(description='资金行为学策略执行脚本')
    parser.add_argument('--config', type=str, default=str(DEFAULT_CONFIG_PATH), help='配置文件路径')
    parser.add_argument('--data', type=str, help='数据文件路径')
    parser.add_argument('--capital', type=float, help='总资金')
    parser.add_argument('--date', type=str, help='指定报告日期 (YYYY-MM-DD格式)')
    parser.add_argument('--reset', action='store_true', help='重置流水线状态')
    parser.add_argument('--auto-reset', action='store_true', help='失败时自动重置并重新运行')
    parser.add_argument('--save-html-only', action='store_true', help='仅生成HTML')
    parser.add_argument('--no-email', action='store_true', help='跳过邮件发送')
    parser.add_argument('--skip-email', action='store_true', help='跳过邮件发送(同--no-email)')

    args = parser.parse_args()

    if args.skip_email:
        args.no_email = True

    config = load_config(args.config)

    if args.capital is not None:
        if 'backtest' not in config:
            config['backtest'] = {}
        config['backtest']['initial_capital'] = args.capital

    result = run_pipeline(args, config)

    if result:
        logger.info("策略执行成功")
    else:
        if args.auto_reset:
            logger.warning("⚠️ 流水线失败，尝试自动重置并重新运行...")
            args.reset = True
            args.auto_reset = False
            pipeline = get_pipeline_manager(args.date if args.date else datetime.now().strftime('%Y-%m-%d'))
            pipeline.reset()
            pipeline.cleanup_checkpoints()
            logger.info("[AUTO-RESET] 已自动重置流水线状态，重新运行...")
            result = run_pipeline(args, config)
            if result:
                logger.info("策略执行成功")
            else:
                logger.error("策略执行失败")
                sys.exit(1)
        else:
            logger.error("策略执行失败")
            sys.exit(1)


if __name__ == "__main__":
    main()
