"""
资金行为学策略执行脚本 (优化版 v2.0)
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

【优化特性 v2.0】
--------------------------------------------------------------------------------
1. 增量数据加载 - 只加载最近60天数据（满足MA20+10日量能计算）
2. 智能缓存 - 数据和因子结果自动缓存，避免重复计算
3. 延迟计算 - 使用Polars Lazy API优化执行计划
4. 异步分发 - MySQL写入异步化，减少阻塞

【断点机制】
- 每个阶段完成后保存状态到 JSON 文件
- 支持从断点恢复，跳过已完成的阶段
- 使用 --reset 参数强制从头开始

================================================================================
"""
import sys
import os
import yaml
import json
import logging
import argparse
import importlib
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from functools import lru_cache
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 加载环境变量（确保在导入其他模块前加载）
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')

from core.config import get_settings
from core.pipeline_state import PipelineStateManager, PipelineState, get_pipeline_manager
from core.data_quality_checker import DataQualityChecker
from core.optimized_data_loader import load_data_optimized
from core.optimized_factor_engine import calculate_factors_optimized
from filters.base_filter import FilterRegistry

DEFAULT_CONFIG_PATH = Path("config/strategies/fund_behavior_config.yaml")
DEFAULT_FILTER_CONFIG = Path("config/filters/fund_behavior_filters.yaml")

FundBehaviorStrategyEngine = importlib.import_module(
    "core.fund_behavior_strategy"
).FundBehaviorStrategyEngine

config_manager = importlib.import_module("core.fund_behavior_config").config_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# 过滤器实例缓存（P1优化）
# =============================================================================
_filter_instance_cache = {}


def get_cached_filter(filter_name: str, filter_params: dict):
    """获取缓存的过滤器实例
    
    使用参数哈希作为缓存键，避免重复创建过滤器实例
    
    Args:
        filter_name: 过滤器名称
        filter_params: 过滤器参数
        
    Returns:
        过滤器实例或None
    """
    # 创建参数哈希键（基于filter_name和关键参数）
    cache_key = f"{filter_name}:{hash(frozenset(filter_params.items()))}"
    
    if cache_key not in _filter_instance_cache:
        filter_class = FilterRegistry.get(filter_name)
        if not filter_class:
            return None
        
        try:
            filter_instance = filter_class(params=filter_params)
            if not filter_instance.is_enabled():
                return None
            _filter_instance_cache[cache_key] = filter_instance
        except Exception as e:
            logger.warning(f"[FILTER_CACHE] 创建过滤器实例失败 {filter_name}: {e}")
            return None
    
    return _filter_instance_cache.get(cache_key)


def clear_filter_cache():
    """清除过滤器实例缓存"""
    global _filter_instance_cache
    _filter_instance_cache = {}
    logger.info("[FILTER_CACHE] 过滤器实例缓存已清除")


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
# 第一步：加载 (Load) - 优化版增量加载
# =============================================================================
def load_data(data_path: str = None, batch_size: int = 500, target_columns: list = None) -> Tuple[pl.DataFrame, dict]:
    """优化版增量加载K线数据
    
    特性：
    - 只加载最近60天数据（满足MA20+10日量能计算需求）
    - 使用智能缓存避免重复加载
    - 使用Polars Lazy API延迟计算

    Args:
        data_path: 数据路径（已弃用，保留兼容性）
        batch_size: 每批文件数量（已弃用，保留兼容性）
        target_columns: 目标列名列表（可配置）

    Returns:
        (数据, 元信息)
    """
    logger.info("=" * 60)
    logger.info("[LOAD] 阶段1: 优化版增量加载K线数据")
    logger.info("=" * 60)
    
    if target_columns is None:
        target_columns = ["code", "trade_date", "open", "close", "high", "low", "volume"]
    
    # 使用优化版数据加载器（加载最近60天）
    data, meta = load_data_optimized(
        days=60,  # 60天 = MA20(20日) + 量能计算(10日) + 缓冲(30日)
        end_date=None,  # 默认昨天
        columns=target_columns,
        use_cache=True
    )
    
    logger.info(f"[LOAD] 完成: {meta['total_rows']}行, {meta['total_stocks']}只股票")
    logger.info(f"[LOAD] 日期范围: {meta['date_range']}")
    
    if meta.get('loaded_from_cache'):
        logger.info("[LOAD] ✅ 从缓存加载")
    
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
# 第三步：变换 (Transform) - 优化版因子计算
# =============================================================================
def calculate_factors(data: pl.DataFrame, config: dict) -> Tuple[pl.DataFrame, dict]:
    """优化版因子计算
    
    特性：
    - 使用延迟计算（Lazy API）
    - 智能因子缓存，避免重复计算
    - 向量化批量计算

    Args:
        data: K线数据
        config: 配置字典

    Returns:
        (因子数据, 元信息)
    """
    logger.info("=" * 60)
    logger.info("[TRANSFORM] 阶段3: 优化版因子计算")
    logger.info("=" * 60)

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

    # 使用优化版因子计算引擎
    factor_data = calculate_factors_optimized(data, factor_names, use_cache=True)

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

    # 注意：不再手动调用 gc.collect()，让Python自动管理内存
    # 在函数返回后，factor_data会自动被回收

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

            # 批量收集过滤器结果，减少I/O操作
            filter_results = []
            failed_filters = []
            
            for filter_name in filter_order:
                filter_params = filters_config.get(filter_name, {})
                if not filter_params.get('enabled', False):
                    continue

                # 使用缓存的过滤器实例（P1优化）
                filter_instance = get_cached_filter(filter_name, filter_params)
                if not filter_instance:
                    continue

                try:
                    before_count = len(factor_data)
                    factor_data = filter_instance.filter(factor_data)
                    after_count = len(factor_data)

                    if before_count != after_count:
                        applied_filters.append({
                            'name': filter_name,
                            'removed': before_count - after_count
                        })
                        filter_results.append(f"{filter_name}: {before_count} -> {after_count}")
                except Exception as e:
                    failed_filters.append(f"{filter_name}: {e}")
            
            # 批量输出过滤器结果（减少I/O次数）
            if filter_results:
                logger.info(f"[FILTER] 应用过滤器:\n  " + "\n  ".join(filter_results))
            if failed_filters:
                logger.warning(f"[FILTER] 失败的过滤器:\n  " + "\n  ".join(failed_filters))
            
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
# 第六步：分发 (Distribute) - 优化版异步分发
# =============================================================================
async def distribute_results_async(result: dict, report_text: str, pipeline: PipelineStateManager) -> Tuple[bool, dict]:
    """异步分发结果到各个渠道
    
    特性：
    - 异步MySQL写入，减少阻塞
    - 并行文件写入
    
    Args:
        result: 策略结果
        report_text: 报告文本
        pipeline: 流水线状态管理器

    Returns:
        (是否成功, 分发结果)
    """
    logger.info("=" * 60)
    logger.info("[DISTRIBUTE] 阶段6: 优化版异步结果分发")
    logger.info("=" * 60)

    report_date = pipeline.report_date
    results = {}

    from services.report_db_service import ReportDBService
    from services.fund_behavior_db_service import FundBehaviorDBService

    subject = f"【量化决策】资金行为学策略 - {report_date} - 开盘前综合决策"
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

    # 定义异步任务
    async def save_to_mysql_batch():
        """批量保存到MySQL（合并两个数据库操作，使用单一连接）
        
        P1优化：使用单一事务批量写入，减少连接开销
        """
        try:
            # 使用连接池获取共享连接
            from services.db_pool import get_db_pool
            pool_manager = get_db_pool()
            
            # 批量执行两个保存操作
            db_service = ReportDBService(pool_manager)
            fb_db_service = FundBehaviorDBService(pool_manager)
            
            # 初始化表（只需执行一次）
            db_service.init_tables()
            fb_db_service.init_tables()
            
            # 批量保存（使用同一连接池）
            success_general = db_service.save_report(
                'fund_behavior', report_date, subject, report_text, result_json
            )
            success_fb = fb_db_service.save_daily_report(report_date, result_json)
            
            logger.info("[DISTRIBUTE] ✅ MySQL批量报告已保存")
            return 'mysql_batch', 'success' if (success_general and success_fb) else 'partial'
        except (ImportError, ConnectionError, TimeoutError) as e:
            logger.error(f"[DISTRIBUTE] ❌ MySQL连接失败: {e}")
            return 'mysql_batch', f'connection_failed: {e}'
        except (ValueError, TypeError) as e:
            logger.error(f"[DISTRIBUTE] ❌ MySQL数据错误: {e}")
            return 'mysql_batch', f'data_error: {e}'

    async def save_txt():
        """异步保存TXT文件"""
        try:
            txt_dir = resolve_project_path('data/reports')
            txt_dir.mkdir(parents=True, exist_ok=True)
            txt_path = txt_dir / f"fund_behavior_{report_date}.txt"
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            logger.info(f"[DISTRIBUTE] ✅ TXT已保存: {txt_path}")
            return 'txt', 'success'
        except (IOError, OSError) as e:
            logger.error(f"[DISTRIBUTE] ❌ TXT文件I/O错误: {e}")
            return 'txt', f'io_error: {e}'
        except UnicodeEncodeError as e:
            logger.error(f"[DISTRIBUTE] ❌ TXT编码错误: {e}")
            return 'txt', f'encode_error: {e}'

    async def save_html():
        """异步保存HTML文件"""
        try:
            from services.notify_service.templates.fund_behavior_report_template import generate_fund_behavior_html
            html_content = generate_fund_behavior_html(result)
            html_dir = resolve_project_path('data/reports/html')
            html_dir.mkdir(parents=True, exist_ok=True)
            html_path = html_dir / f"fund_behavior_{report_date}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"[DISTRIBUTE] ✅ HTML已保存: {html_path}")
            return 'html', 'success'
        except ImportError as e:
            logger.error(f"[DISTRIBUTE] ❌ HTML模板导入失败: {e}")
            return 'html', f'import_error: {e}'
        except (IOError, OSError) as e:
            logger.error(f"[DISTRIBUTE] ❌ HTML文件I/O错误: {e}")
            return 'html', f'io_error: {e}'

    # 并行执行所有任务（P1优化：合并MySQL操作为一个批量任务）
    tasks = [
        save_to_mysql_batch(),  # 批量MySQL写入
        save_txt(),
        save_html()
    ]
    
    completed_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 统计结果
    success_count = 0
    fail_count = 0
    
    for res in completed_results:
        if isinstance(res, Exception):
            fail_count += 1
            logger.error(f"[DISTRIBUTE] 任务异常: {res}")
        else:
            key, status = res
            results[key] = status
            if status == 'success':
                success_count += 1
            else:
                fail_count += 1

    logger.info(f"[DISTRIBUTE] 完成: {success_count}成功, {fail_count}失败")
    return fail_count == 0, results


def distribute_results(result: dict, report_text: str, pipeline: PipelineStateManager) -> Tuple[bool, dict]:
    """同步包装器（兼容旧接口）"""
    return asyncio.run(distribute_results_async(result, report_text, pipeline))


def send_email_notification(result: dict, report_text: str) -> bool:
    """发送邮件通知"""
    import concurrent.futures

    try:
        from services.notify_service.templates.fund_behavior_report_template import generate_fund_behavior_html
        from services.email_sender import EmailSender

        report_date = datetime.now().strftime('%Y-%m-%d')
        subject = f"【量化决策】资金行为学策略 - {report_date} - 开盘前综合决策"
        html_content = generate_fund_behavior_html(result)

        def _send_email():
            import os
            from dotenv import load_dotenv
            load_dotenv()
            
            use_env_fallback = False
            recipients = None
            sender = None
            
            # 尝试从Nacos加载配置
            try:
                import nacos
                settings = get_settings()
                client = nacos.NacosClient(
                    settings.NACOS_SERVER_ADDR,
                    namespace=settings.NACOS_NAMESPACE,
                    username=settings.NACOS_USERNAME,
                    password=settings.NACOS_PASSWORD or ""
                )
                xcomm_content = client.get_config("xcomm.yaml", "DEFAULT_GROUP")
                
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
            except ImportError:
                use_env_fallback = True
                logger.info("[EMAIL] nacos模块未安装，使用.env配置")
            
            # 使用环境变量作为降级方案
            if use_env_fallback or sender is None:
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
                # 移除 del 和 gc.collect()，让Python自动管理内存
                # data 变量在函数作用域结束时自动释放
                pipeline.transition(PipelineState.TRANSFORMED, "transform", transform_meta)

                buffer_path = buffer_factors(factor_data, pipeline)
                pipeline.transition(PipelineState.BUFFERED, "buffer", {'path': buffer_path})

                result, execute_meta = execute_strategy(factor_data, config, pipeline)
                # 移除 del 和 gc.collect()，factor_data 在函数返回后自动释放
                pipeline.transition(PipelineState.EXECUTED, "execute", execute_meta)

                execute_result_path = pipeline.get_checkpoint_path("execute_result", ".json")
                import json
                with open(execute_result_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, default=str)
                logger.info(f"[EXECUTE] 结果已持久化: {execute_result_path}")

                # P0优化：使用公共函数生成和分发报告
                _generate_and_distribute_report(result, pipeline, args, config, report_date)

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

                # P0优化：使用公共函数生成和分发报告（消除重复代码）
                _generate_and_distribute_report(result, pipeline, args, config, report_date)

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
        import traceback
        logger.error(f"堆栈跟踪:\n{traceback.format_exc()}")
        raise


def _generate_and_distribute_report(
    result: dict,
    pipeline,
    args,
    config: dict,
    report_date: str
) -> None:
    """生成报告并分发（提取公共逻辑，消除代码重复）
    
    P0优化：将START和EXECUTED状态中共有的报告生成和分发逻辑提取为独立函数
    
    Args:
        result: 策略执行结果
        pipeline: 流水线状态管理器
        args: 命令行参数
        config: 配置字典
        report_date: 报告日期
    """
    load_meta = pipeline.get_step_result("load") or {}
    morning_data = load_morning_data(report_date)
    report_text = generate_decision_report(result, load_meta, config, morning_data, report_date)

    if not args.save_html_only:
        distribute_success, distribute_meta = distribute_results(result, report_text, pipeline)
        pipeline.transition(PipelineState.DISTRIBUTED, "distribute", distribute_meta)

        if not args.no_email:
            email_success = send_email_notification(result, report_text)
            if email_success:
                logger.info("[EMAIL] ✅ 邮件发送成功")
    else:
        pipeline.transition(PipelineState.DISTRIBUTED, "distribute", {'mode': 'html_only'})


# =============================================================================
# 辅助函数：加载新闻联播AI分析
# =============================================================================
def load_cctv_analysis(report_date: str = None) -> dict:
    """从MySQL加载新闻联播AI分析结果
    
    Args:
        report_date: 报告日期，默认为昨天
        
    Returns:
        dict: AI分析结果
    """
    if report_date is None:
        from datetime import datetime, timedelta
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        import pymysql
        from pymysql.cursors import DictCursor
        
        # P0优化：移除硬编码敏感信息，强制使用环境变量
        db_host = os.getenv('DB_HOST')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_name = os.getenv('DB_NAME', 'xcn_db')
        
        # 验证必要的环境变量
        if not all([db_host, db_user, db_password]):
            missing = []
            if not db_host:
                missing.append('DB_HOST')
            if not db_user:
                missing.append('DB_USER')
            if not db_password:
                missing.append('DB_PASSWORD')
            logger.error(f"[CCTV] 缺少必要的数据库环境变量: {', '.join(missing)}")
            return {}
        
        db_config = {
            'host': db_host,
            'port': int(os.getenv('DB_PORT', '3306')),
            'user': db_user,
            'password': db_password,
            'database': db_name,
            'charset': 'utf8mb4',
            'cursorclass': DictCursor
        }
        
        conn = pymysql.connect(**db_config)
        try:
            with conn.cursor() as cursor:
                # 查询新闻联播AI分析结果 (从 cctv_news_broadcast 表)
                cursor.execute("""
                    SELECT news_date, ai_summary, ai_bullish, ai_hot_sectors, 
                           ai_leading_stocks, ai_macro_guidance, ai_risk_alerts, ai_sentiment
                    FROM cctv_news_broadcast
                    WHERE news_date = %s
                    ORDER BY ai_updated_at DESC
                    LIMIT 1
                """, (report_date,))
                
                row = cursor.fetchone()
                if row and row.get('ai_summary'):
                    logger.info(f"[CCTV] 成功加载 {report_date} 新闻联播AI分析")
                    return {
                        'date': row['news_date'].strftime('%Y-%m-%d') if row['news_date'] else report_date,
                        'summary': row['ai_summary'] or '',
                        'bullish': row['ai_bullish'] or '',
                        'hot_sectors': row['ai_hot_sectors'] or '',
                        'leading_stocks': row['ai_leading_stocks'] or '',
                        'macro_guidance': row['ai_macro_guidance'] or '',
                        'risk_alerts': row['ai_risk_alerts'] or '',
                        'overall_sentiment': row['ai_sentiment'] or '中性'
                    }
                else:
                    logger.warning(f"[CCTV] 未找到 {report_date} 的新闻联播AI分析")
                    return {}
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"[CCTV] 加载新闻联播AI分析失败: {e}")
        return {}


# =============================================================================
# 辅助函数：加载晨间数据（P1优化：并行读取）
# =============================================================================
from concurrent.futures import ThreadPoolExecutor, as_completed


def _load_json_file(file_path: Path) -> tuple:
    """加载单个JSON文件（用于并行读取）
    
    Args:
        file_path: JSON文件路径
        
    Returns:
        (key, data) 元组
    """
    key = file_path.stem.replace('_data', '').replace('_index', '')
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return key, json.load(f)
    except (json.JSONDecodeError, IOError, OSError) as e:
        logger.warning(f"[MORNING_DATA] {file_path.name} 加载失败: {e}")
        return key, {}


def load_morning_data(report_date: str = None) -> dict:
    """加载晨间流水线采集的所有数据（P1优化：并行读取）
    
    使用ThreadPoolExecutor并行读取多个JSON文件，减少I/O等待时间
    
    Args:
        report_date: 报告日期
        
    Returns:
        dict: 包含宏观、外盘、大宗、情绪、新闻等数据
    """
    data_dir = resolve_project_path('data')
    morning_data = {
        'macro': {},
        'oil_dollar': {},
        'commodities': {},
        'sentiment': {},
        'news': {},
        'foreign_market': {},
        'cctv_analysis': {}
    }
    
    # 0. 加载新闻联播AI分析（从MySQL）- 独立处理
    yesterday = (datetime.strptime(report_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    morning_data['cctv_analysis'] = load_cctv_analysis(yesterday)
    
    # P1优化：使用线程池并行读取JSON文件
    json_files = [
        ('macro', data_dir / 'macro_data.json'),
        ('oil_dollar', data_dir / 'oil_dollar_data.json'),
        ('commodities', data_dir / 'commodities_data.json'),
        ('sentiment', data_dir / 'sentiment_data.json'),
        ('news', data_dir / 'news_data.json'),
    ]
    
    # 过滤存在的文件
    existing_files = [(key, path) for key, path in json_files if path.exists()]
    
    if existing_files:
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有读取任务
            future_to_key = {
                executor.submit(_load_json_file, path): key 
                for key, path in existing_files
            }
            
            # 收集结果
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    _, data = future.result()
                    morning_data[key] = data
                except Exception as e:
                    logger.warning(f"[MORNING_DATA] {key} 数据处理失败: {e}")
    
    # 处理 foreign_index.json（特殊格式转换）
    foreign_index_file = data_dir / 'foreign_index.json'
    if foreign_index_file.exists():
        try:
            with open(foreign_index_file, 'r', encoding='utf-8') as f:
                foreign_index = json.load(f)
                # 转换 foreign_index 格式为报告需要的格式
                morning_data['foreign_market'] = {
                    'sp500': foreign_index.get('us_index', {}).get('data', {}).get('sp500', {}),
                    'nasdaq': foreign_index.get('us_index', {}).get('data', {}).get('nasdaq', {}),
                    'dow': foreign_index.get('us_index', {}).get('data', {}).get('dow', {}),
                    'hang_seng': foreign_index.get('asia_index', {}).get('data', {}).get('hang_seng', {}),
                    'update_time': foreign_index.get('update_time', '')
                }
                # 同时提取大宗商品数据
                commodity = foreign_index.get('commodity', {}).get('data', {})
                if commodity:
                    morning_data['commodities'] = {
                        'metals': {
                            'gold': commodity.get('gold', {})
                        },
                        'oil': {
                            'wti': commodity.get('oil', {})
                        }
                    }
                logger.info(f"[MORNING_DATA] 成功加载 foreign_index.json: {foreign_index.get('date', 'unknown')}")
        except (json.JSONDecodeError, IOError, OSError) as e:
            logger.warning(f"[MORNING_DATA] foreign_index.json 加载失败: {e}")
    
    # 如果 foreign_index.json 不存在，尝试加载旧的 foreign_market_data.json
    if not morning_data.get('foreign_market'):
        foreign_file = data_dir / 'foreign_market_data.json'
        if foreign_file.exists():
            try:
                with open(foreign_file, 'r', encoding='utf-8') as f:
                    morning_data['foreign_market'] = json.load(f)
            except (json.JSONDecodeError, IOError, OSError) as e:
                logger.warning(f"[MORNING_DATA] 外盘数据加载失败: {e}")
    
    return morning_data


# =============================================================================
# 辅助函数：生成决策报告
# =============================================================================
def generate_decision_report(result: dict, load_meta: dict = None, config: dict = None, morning_data: dict = None, report_date: str = None) -> str:
    """生成量化决策报告

    Args:
        result: 策略执行结果
        load_meta: 加载元信息（可选，用于显示数据概览）
        config: 配置字典
        morning_data: 晨间采集的数据
        report_date: 报告日期（默认为当天）

    Returns:
        str: 格式化的报告文本
    """
    report_lines = []
    if report_date is None:
        report_date = datetime.now().strftime('%Y-%m-%d')
    report_lines.append("\n" + "=" * 70)
    report_lines.append(f"【🎯 量化决策报告 {report_date} 09:26】")
    report_lines.append("=" * 70)
    report_lines.append("📊 基于昨日复盘 + 夜间消息面 + 外盘走势 + 大宗商品 + 技术信号")
    report_lines.append("=" * 70)

    v_total = result.get('v_total', 0.0)
    sentiment_temp = result.get('sentiment_temperature', 0.0)
    delta_temp = result.get('delta_temperature', 0.0)

    # ========== 1. 市场环境定性 ==========
    report_lines.append("\n【1️⃣ 市场环境定性】")
    report_lines.append("-" * 50)

    market_states = result.get('market_state', [])
    if market_states:
        state_counts = {}
        for s in market_states:
            state_counts[s] = state_counts.get(s, 0) + 1
        dominant_state = max(state_counts, key=state_counts.get)
        state_emoji = {'STRONG': '🚀', 'OSCILLATING': '〰️', 'WEAK': '🔴', 'RISK': '⚠️'}.get(dominant_state.upper(), '➖')
        report_lines.append(f"  {state_emoji} 周期定位：{dominant_state.upper()}")

    # 量能显示优化（如果是万亿级别）
    v_display = v_total
    v_unit = "亿"
    if v_total > 10000:
        v_display = v_total / 10000
        v_unit = "万亿"
    report_lines.append(f"  💰 量能判定：{v_display:.2f}{v_unit}")
    report_lines.append(f"  🌡️ 情绪温度：{sentiment_temp:.1f}°")
    report_lines.append(f"  📈 温差惯性：{delta_temp:+.1f}°")
    
    # ========== 2. 宏观与外盘环境 ==========
    if morning_data:
        report_lines.append("\n【2️⃣ 宏观与外盘环境】")
        report_lines.append("-" * 50)
        
        # 宏观数据
        macro = morning_data.get('macro', {})
        if macro:
            dxy = macro.get('dxy', {}).get('value', 0)
            us10y = macro.get('us10y', {}).get('value', 0)
            cny = macro.get('cny', {}).get('value', 0)
            report_lines.append(f"  🌍 美元指数：{dxy:.2f}")
            report_lines.append(f"  📊 美债10Y：{us10y:.2f}%")
            report_lines.append(f"  💱 离岸人民币：{cny:.4f}")
        
        # 外盘数据 (支持 foreign_index.json 格式)
        foreign = morning_data.get('foreign_market', {})
        if foreign:
            # 处理 foreign_index.json 格式
            sp500_data = foreign.get('sp500', {})
            nasdaq_data = foreign.get('nasdaq', {})
            dow_data = foreign.get('dow', {})
            
            sp500_change = sp500_data.get('change_pct', 0)
            nasdaq_change = nasdaq_data.get('change_pct', 0)
            dow_change = dow_data.get('change_pct', 0)
            
            if sp500_change:
                report_lines.append(f"  🇺🇸 标普500：{sp500_change:+.2f}%")
            if nasdaq_change:
                report_lines.append(f"  🇺🇸 纳斯达克：{nasdaq_change:+.2f}%")
            if dow_change:
                report_lines.append(f"  🇺🇸 道琼斯：{dow_change:+.2f}%")
            
            # 港股
            hs_data = foreign.get('hang_seng', {})
            if hs_data:
                hs_change = hs_data.get('change_pct', 0)
                if hs_change:
                    report_lines.append(f"  🇭🇰 恒生指数：{hs_change:+.2f}%")
        
        # 大宗商品 (支持 foreign_index.json 格式)
        comm = morning_data.get('commodities', {})
        if comm:
            # 黄金
            gold_data = comm.get('metals', {}).get('gold', {})
            if gold_data:
                gold_price = gold_data.get('price', 0)
                gold_change = gold_data.get('change_pct', 0)
                if gold_price:
                    report_lines.append(f"  🥇 黄金：${gold_price:.2f} ({gold_change:+.2f}%)")
            
            # 原油
            oil_data = comm.get('oil', {}).get('wti', {})
            if oil_data:
                oil_price = oil_data.get('price', 0)
                oil_change = oil_data.get('change_pct', 0)
                if oil_price:
                    report_lines.append(f"  🛢️ WTI原油：${oil_price:.2f} ({oil_change:+.2f}%)")
        
        # 如果没有 foreign_index 数据，尝试旧格式
        if not foreign:
            # 石油美元 (旧格式)
            oil = morning_data.get('oil_dollar', {})
            if oil:
                oil_data = oil.get('oil', {})
                wti = oil_data.get('wti', {}).get('price', 0)
                brent = oil_data.get('brent', {}).get('price', 0)
                if wti:
                    report_lines.append(f"  🛢️ WTI原油：${wti:.2f}")
                elif brent:
                    report_lines.append(f"  🛢️ 布伦特原油：${brent:.2f}")
            
            # 大宗商品 (旧格式)
            comm_old = morning_data.get('commodities', {})
            if comm_old:
                metals = comm_old.get('metals', {})
                gold = metals.get('gold', {}).get('price', 0)
                if gold:
                    report_lines.append(f"  🥇 黄金：${gold:.2f}")
    
    # ========== 3. 新闻联播AI分析 ==========
    cctv = morning_data.get('cctv_analysis', {}) if morning_data else {}
    if cctv and cctv.get('summary'):
        report_lines.append("\n【3️⃣ 新闻联播AI分析 - 📺 政策风向】")
        report_lines.append("-" * 50)
        
        # 摘要
        summary = cctv.get('summary', '')
        if summary:
            report_lines.append(f"  📝 核心要点：{summary[:100]}...")
        
        # 利好因素
        bullish = cctv.get('bullish', '')
        if bullish:
            report_lines.append(f"  📈 利好因素：{bullish[:80]}...")
        
        # 热门板块
        hot_sectors = cctv.get('hot_sectors', '')
        if hot_sectors:
            report_lines.append(f"  🔥 热门板块：{hot_sectors}")
        
        # 龙头股
        leading = cctv.get('leading_stocks', '')
        if leading:
            report_lines.append(f"  ⭐ 龙头关注：{leading[:80]}...")
        
        # 宏观指导
        guidance = cctv.get('macro_guidance', '')
        if guidance:
            report_lines.append(f"  💡 策略指导：{guidance[:80]}...")
        
        # 风险提示
        risks = cctv.get('risk_alerts', '')
        if risks:
            report_lines.append(f"  ⚠️ 风险提示：{risks[:80]}...")
        
        # 整体情绪
        sentiment = cctv.get('overall_sentiment', '中性')
        sentiment_emoji = {'积极': '🟢', '中性': '🟡', '谨慎': '🔴'}.get(sentiment, '⚪')
        report_lines.append(f"  {sentiment_emoji} 政策情绪：{sentiment}")
    
    # ========== 4. 防守信号（核心交易指令） ==========
    report_lines.append("\n【4️⃣ 防守信号 - ⚠️ 核心交易指令】")
    report_lines.append("-" * 50)
    defense = result.get('defense_signals', {})
    action = defense.get('action', 'UNKNOWN')
    action_display = {'BUY': '✅ 积极买入', 'DEFENSE': '⛔ 防守观望', 'CAUTION': '⚠️ 谨慎操作'}.get(action, action)
    report_lines.append(f"  🎯 动作指令：{action_display}")
    
    reasons = defense.get('reasons', [])
    if reasons:
        for i, reason in enumerate(reasons, 1):
            report_lines.append(f"  📌 原因{i}：{reason}")
    else:
        report_lines.append(f"  ✅ 状态：市场环境正常，可积极参与")
    
    details = defense.get('details', {})
    if details.get('near_support'):
        report_lines.append(f"  ⚡ 位置提示：接近支撑位，关注反弹机会")
    if details.get('near_resistance'):
        report_lines.append(f"  ⚠️ 位置提示：接近阻力位，注意回落风险")

    # ========== 5. 核心观察点 ==========
    report_lines.append("\n【5️⃣ 核心观察点】")
    report_lines.append("-" * 50)
    upward = result.get('upward_pivot', False)
    hedge = result.get('hedge_effect', False)
    strong = result.get('is_strong_region', False)
    report_lines.append(f"  {'✅' if upward else '❌'} 向上变盘：{'是 - 大胆做多' if upward else '否 - 日内防守'}")
    report_lines.append(f"  {'✅' if hedge else '❌'} 对冲效果：{'是 - 量能充沛' if hedge else '否 - 量能不足'}")
    report_lines.append(f"  {'✅' if strong else '❌'} 强势区域：{'是' if strong else '否'}")

    # ========== 6. 选股结果 ==========
    report_lines.append("\n【6️⃣ 选股结果】")
    report_lines.append("-" * 50)
    trend_stocks = result.get('trend_stocks', [])
    short_term_stocks = result.get('short_term_stocks', [])
    report_lines.append(f"  📊 波段趋势：{len(trend_stocks)}只")
    report_lines.append(f"  🚀 短线打板：{len(short_term_stocks)}只")
    
    # 显示前10只波段股
    if trend_stocks:
        report_lines.append(f"  📝 前10只波段股：{', '.join(trend_stocks[:10])}")
    if short_term_stocks:
        report_lines.append(f"  📝 前5只短线股：{', '.join(short_term_stocks[:5])}")

    # ========== 7. 仓位分配建议 ==========
    position = result.get('position_size', {})
    report_lines.append("\n【7️⃣ 仓位分配建议】")
    report_lines.append("-" * 50)
    trend_pct = position.get('trend', 0) / 10000
    short_pct = position.get('short_term', 0) / 10000
    cash_pct = position.get('cash', 0) / 10000
    total = trend_pct + short_pct + cash_pct
    report_lines.append(f"  📈 波段仓位：{trend_pct:.0f}万 ({trend_pct/total*100:.0f}%)")
    report_lines.append(f"  ⚡ 短线仓位：{short_pct:.0f}万 ({short_pct/total*100:.0f}%)")
    report_lines.append(f"  💵 现金储备：{cash_pct:.0f}万 ({cash_pct/total*100:.0f}%)")
    
    # 根据防守信号调整建议
    if action == 'DEFENSE':
        report_lines.append(f"  ⚠️ 建议：当前防守信号，建议减仓至30%以下或空仓观望")
    elif action == 'CAUTION':
        report_lines.append(f"  ⚠️ 建议：当前谨慎信号，建议控制仓位在50%左右")
    else:
        report_lines.append(f"  ✅ 建议：当前积极信号，可满仓操作")

    # ========== 8. 今日策略建议 ==========
    report_lines.append("\n【8️⃣ 今日策略建议】")
    report_lines.append("-" * 50)
    if action == 'DEFENSE':
        report_lines.append("  ⛔ 避险为主：")
        report_lines.append("     • 开盘后如冲高回落，及时减仓")
        report_lines.append("     • 关注防御性板块（医药、消费）")
        report_lines.append("     • 可做T降低成本，但不新开仓")
    elif action == 'CAUTION':
        report_lines.append("  ⚠️ 谨慎操作：")
        report_lines.append("     • 控制仓位，分批建仓")
        report_lines.append("     • 关注强势股回调机会")
        report_lines.append("     • 设置止损，严格执行")
    else:
        if upward:
            report_lines.append("  🚀 积极做多：")
            report_lines.append("     • 大胆追涨强势股")
            report_lines.append("     • 关注热点板块龙头")
            report_lines.append("     • 可适当参与打板")
        else:
            report_lines.append("  📊 震荡上行：")
            report_lines.append("     • 低吸高抛，做T为主")
            report_lines.append("     • 关注均线支撑位")
            report_lines.append("     • 不追高，回调再买入")

    # ========== 9. 数据概览 ==========
    if load_meta:
        report_lines.append("\n【9️⃣ 数据概览】")
        report_lines.append("-" * 50)
        # load_meta 可能包含 data 键（从pipeline获取）或直接的元数据（从load_data返回）
        meta_data = load_meta.get('data', load_meta)
        total_stocks = meta_data.get('total_stocks', 'N/A')
        total_rows = meta_data.get('total_rows', 'N/A')
        date_range = meta_data.get('date_range', 'N/A')
        report_lines.append(f"  📊 股票数量：{total_stocks}只")
        if isinstance(total_rows, int):
            report_lines.append(f"  📈 数据行数：{total_rows:,}行")
        else:
            report_lines.append(f"  📈 数据行数：{total_rows}行")
        report_lines.append(f"  📅 日期范围：{date_range}")

    report_lines.append("\n" + "=" * 70)
    report_lines.append("🤖 本报告由XCNStock量化系统生成 | 仅供参考，不构成投资建议")
    report_lines.append("=" * 70)
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
