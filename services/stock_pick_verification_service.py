"""
股票推荐验证服务
"""
import json
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError

import polars as pl
from core.logger import get_logger

logger = get_logger(__name__)

DB_URL = os.getenv(
    'DB_URL',
    'mysql+pymysql://nextai:100200@localhost:3306/xcn_db?charset=utf8mb4'
)


class StockPickVerificationService:
    """股票推荐验证服务"""
    
    def __init__(self, db_url: str = None):
        if db_url is None:
            db_url = DB_URL
        
        self.engine = create_engine(db_url, pool_size=10, pool_recycle=3600)
        self.Session = sessionmaker(bind=self.engine)
        
        from models.stock_pick_verification import Base
        Base.metadata.create_all(self.engine)
        
        logger.info(f"StockPickVerificationService 初始化完成")
    
    def save_recommendations(
        self, 
        recommendations: List[Dict[str, Any]], 
        recommend_date: date = None,
        update_existing: bool = True
    ) -> int:
        """
        保存推荐股票
        
        Args:
            recommendations: 推荐股票列表
            recommend_date: 推荐日期，默认今天
            update_existing: 是否更新已存在的记录
        
        Returns:
            int: 成功保存的数量
        """
        if recommend_date is None:
            recommend_date = date.today()
        
        from models.stock_pick_verification import StockRecommendation
        
        saved_count = 0
        session = self.Session()
        
        try:
            for rec in recommendations:
                rec_data = {
                    'recommend_date': recommend_date,
                    'code': rec.get('code'),
                    'name': rec.get('name', ''),
                    'grade': rec.get('grade', 'C'),
                    'score': rec.get('enhanced_score') or rec.get('score'),
                    'recommend_price': Decimal(str(rec.get('price', 0))),
                    'recommend_change': Decimal(str(rec.get('change_pct', 0))),
                    'is_st': 1 if 'ST' in rec.get('name', '') else 0,
                    'industry': rec.get('industry'),
                    'support_strong': Decimal(str(rec.get('support_strong', 0))) if rec.get('support_strong') else None,
                    'resistance_strong': Decimal(str(rec.get('resistance_strong', 0))) if rec.get('resistance_strong') else None,
                    'ma20': Decimal(str(rec.get('ma20', 0))) if rec.get('ma20') else None,
                    'ma60': Decimal(str(rec.get('ma60', 0))) if rec.get('ma60') else None,
                    'cvd_signal': rec.get('cvd_signal'),
                    'reasons': json.dumps(rec.get('reasons', []), ensure_ascii=False) if isinstance(rec.get('reasons'), list) else rec.get('reasons'),
                    'stop_loss_pct': Decimal(str(rec.get('stop_loss_pct', -5))),
                    'take_profit_pct': Decimal(str(rec.get('take_profit_pct', 10))),
                }
                
                existing = session.query(StockRecommendation).filter(
                    StockRecommendation.recommend_date == recommend_date,
                    StockRecommendation.code == rec.get('code')
                ).first()
                
                if existing:
                    if update_existing:
                        for key, value in rec_data.items():
                            if key not in ['recommend_date', 'code']:
                                setattr(existing, key, value)
                        saved_count += 1
                else:
                    recommendation = StockRecommendation(**rec_data)
                    session.add(recommendation)
                    saved_count += 1
            
            session.commit()
            logger.info(f"保存推荐股票成功: {recommend_date} 共 {saved_count} 只")
            
        except Exception as e:
            session.rollback()
            logger.error(f"保存推荐失败: {e}")
            raise
        finally:
            session.close()
        
        return saved_count
    
    def update_tracking(
        self, 
        track_date: date = None,
        max_days: int = 30
    ) -> Dict[str, int]:
        """
        更新跟踪数据
        
        Args:
            track_date: 跟踪日期，默认今天
            max_days: 最大跟踪天数
        
        Returns:
            Dict: 更新统计
        """
        if track_date is None:
            track_date = date.today()
        
        from models.stock_pick_verification import StockRecommendation, StockPickTracking
        
        session = self.Session()
        stats = {'updated': 0, 'stopped': 0, 'errors': 0}
        
        try:
            recommendations = session.query(StockRecommendation).filter(
                StockRecommendation.status == 'tracking'
            ).all()
            
            for rec in recommendations:
                try:
                    track_day = (track_date - rec.recommend_date).days
                    
                    if track_day <= 0:
                        continue
                    
                    if track_day > max_days:
                        rec.status = 'completed'
                        rec.stop_reason = 'expired'
                        rec.stopped_at = track_date
                        stats['stopped'] += 1
                        continue
                    
                    price_data = self._get_stock_price(rec.code, track_date)
                    
                    if not price_data:
                        logger.warning(f"获取价格失败: {rec.code} {track_date}")
                        continue
                    
                    cumulative_profit = Decimal(str(
                        (price_data['close'] - float(rec.recommend_price)) / float(rec.recommend_price) * 100
                    ))
                    
                    existing_tracking = session.query(StockPickTracking).filter(
                        StockPickTracking.recommend_id == rec.id,
                        StockPickTracking.track_day == track_day
                    ).first()
                    
                    tracking_data = {
                        'recommend_id': rec.id,
                        'recommend_date': rec.recommend_date,
                        'code': rec.code,
                        'track_day': track_day,
                        'track_date': track_date,
                        'open_price': Decimal(str(price_data.get('open', 0))),
                        'high_price': Decimal(str(price_data.get('high', 0))),
                        'low_price': Decimal(str(price_data.get('low', 0))),
                        'close_price': Decimal(str(price_data['close'])),
                        'prev_close_price': Decimal(str(price_data.get('prev_close', 0))),
                        'daily_change': Decimal(str(price_data.get('change_pct', 0))),
                        'cumulative_profit': cumulative_profit,
                        'volume': price_data.get('volume'),
                        'amount': Decimal(str(price_data.get('amount', 0))) if price_data.get('amount') else None,
                        'turnover_rate': Decimal(str(price_data.get('turnover_rate', 0))) if price_data.get('turnover_rate') else None,
                    }
                    
                    if existing_tracking:
                        for key, value in tracking_data.items():
                            if key not in ['recommend_id', 'track_day']:
                                setattr(existing_tracking, key, value)
                    else:
                        tracking = StockPickTracking(**tracking_data)
                        session.add(tracking)
                    
                    if cumulative_profit > rec.max_profit:
                        rec.max_profit = cumulative_profit
                        rec.best_day = track_day
                    if cumulative_profit < rec.max_loss:
                        rec.max_loss = cumulative_profit
                        rec.worst_day = track_day
                    
                    if track_day == max_days:
                        rec.final_profit = cumulative_profit
                        rec.status = 'completed'
                        rec.stop_reason = 'expired'
                        stats['stopped'] += 1
                    
                    if cumulative_profit >= rec.take_profit_pct:
                        rec.final_profit = cumulative_profit
                        rec.status = 'stopped'
                        rec.stop_reason = 'profit_triggered'
                        rec.stopped_at = track_date
                        stats['stopped'] += 1
                    
                    if cumulative_profit <= rec.stop_loss_pct:
                        rec.final_profit = cumulative_profit
                        rec.status = 'stopped'
                        rec.stop_reason = 'loss_triggered'
                        rec.stopped_at = track_date
                        stats['stopped'] += 1
                    
                    stats['updated'] += 1
                    
                except Exception as e:
                    logger.error(f"更新跟踪失败: {rec.code} - {e}")
                    stats['errors'] += 1
            
            session.commit()
            logger.info(f"跟踪更新完成: {stats}")
            
        except Exception as e:
            session.rollback()
            logger.error(f"跟踪更新失败: {e}")
            raise
        finally:
            session.close()
        
        return stats
    
    def _get_stock_price(self, code: str, track_date: date) -> Optional[Dict]:
        """从K线数据获取股票价格"""
        kline_file = Path(f"data/kline/{code}.parquet")
        
        if not kline_file.exists():
            return None
        
        try:
            df = pl.read_parquet(str(kline_file))
            date_str = track_date.strftime('%Y-%m-%d')
            
            row = df.filter(pl.col('trade_date') == date_str)
            
            if len(row) == 0:
                return None
            
            r = row.to_dicts()[0]
            return {
                'open': r.get('open', 0),
                'high': r.get('high', 0),
                'low': r.get('low', 0),
                'close': r.get('close', 0),
                'volume': r.get('volume', 0),
                'amount': r.get('amount', 0),
                'change_pct': r.get('change_pct', 0),
                'turnover_rate': r.get('turnover_rate', 0),
                'prev_close': r.get('prev_close', 0),
            }
        except Exception as e:
            logger.error(f"读取K线数据失败: {code} - {e}")
            return None
    
    def get_recommendations(
        self, 
        recommend_date: date = None,
        grade: str = None,
        status: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """查询推荐记录"""
        from models.stock_pick_verification import StockRecommendation
        
        session = self.Session()
        try:
            query = session.query(StockRecommendation)
            
            if recommend_date:
                query = query.filter(StockRecommendation.recommend_date == recommend_date)
            if grade:
                query = query.filter(StockRecommendation.grade == grade)
            if status:
                query = query.filter(StockRecommendation.status == status)
            
            results = query.order_by(StockRecommendation.recommend_date.desc()).limit(limit).all()
            return [r.to_dict() for r in results]
        finally:
            session.close()
    
    def get_tracking_detail(self, recommend_id: int) -> List[Dict]:
        """获取跟踪明细"""
        from models.stock_pick_verification import StockPickTracking
        
        session = self.Session()
        try:
            trackings = session.query(StockPickTracking).filter(
                StockPickTracking.recommend_id == recommend_id
            ).order_by(StockPickTracking.track_day).all()
            return [t.to_dict() for t in trackings]
        finally:
            session.close()
    
    def get_statistics(self, recommend_date: date = None) -> Dict:
        """获取统计数据"""
        session = self.Session()
        try:
            sql = """
                SELECT 
                    recommend_date,
                    COUNT(*) as total_picks,
                    SUM(CASE WHEN grade = 'S' THEN 1 ELSE 0 END) as s_count,
                    SUM(CASE WHEN grade = 'A' THEN 1 ELSE 0 END) as a_count,
                    AVG(final_profit) as avg_profit,
                    MAX(max_profit) as best_profit,
                    MIN(max_loss) as worst_loss,
                    SUM(CASE WHEN final_profit > 0 THEN 1 ELSE 0 END) as win_count,
                    SUM(CASE WHEN status IN ('completed', 'stopped') THEN 1 ELSE 0 END) as finished_count
                FROM stock_recommendation
                WHERE 1=1
            """
            
            if recommend_date:
                sql += f" AND recommend_date = '{recommend_date}'"
            
            sql += " GROUP BY recommend_date ORDER BY recommend_date DESC LIMIT 30"
            
            result = session.execute(text(sql)).fetchall()
            
            stats = []
            for row in result:
                total = row[1]
                win_count = row[7] or 0
                finished = row[8] or 0
                
                stats.append({
                    'recommend_date': str(row[0]),
                    'total_picks': row[1],
                    's_count': row[2],
                    'a_count': row[3],
                    'avg_profit': float(row[4]) if row[4] else 0,
                    'best_profit': float(row[5]) if row[5] else 0,
                    'worst_loss': float(row[6]) if row[6] else 0,
                    'win_count': win_count,
                    'win_rate': round(win_count / finished * 100, 2) if finished > 0 else 0,
                    'finished_count': finished,
                })
            
            return {'daily_stats': stats}
            
        finally:
            session.close()
    
    def manual_stop(
        self, 
        recommend_id: int, 
        stop_date: date = None,
        reason: str = 'manual'
    ) -> bool:
        """手动终止跟踪"""
        from models.stock_pick_verification import StockRecommendation
        
        if stop_date is None:
            stop_date = date.today()
        
        session = self.Session()
        try:
            rec = session.query(StockRecommendation).filter(
                StockRecommendation.id == recommend_id
            ).first()
            
            if not rec:
                return False
            
            rec.status = 'stopped'
            rec.stop_reason = reason
            rec.stopped_at = stop_date
            
            tracking = session.query(StockPickTracking).filter(
                StockPickTracking.recommend_id == recommend_id
            ).order_by(StockPickTracking.track_day.desc()).first()
            
            if tracking:
                rec.final_profit = tracking.cumulative_profit
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"手动终止失败: {e}")
            return False
        finally:
            session.close()
