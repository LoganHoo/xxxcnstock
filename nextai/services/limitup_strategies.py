"""六大涨停预测模型"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime

import polars as pl

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


@dataclass
class LimitUpSignal:
    code: str
    name: str
    strategy: str
    score: float
    confidence: float
    signals: Dict[str, bool] = field(default_factory=dict)
    factors: Dict[str, float] = field(default_factory=dict)
    reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


class LimitUpPullbackStrategy:
    """涨停回调战法"""

    def scan(self, kline: pl.DataFrame, code: str, name: str) -> Optional[LimitUpSignal]:
        if kline.height < 10:
            return None

        close = kline.select(pl.col("close")).to_series()
        volume = kline.select(pl.col("volume")).to_series()

        has_limit_up = False
        limit_up_idx = -1
        for i in range(min(10, close.len() - 1)):
            idx = close.len() - 1 - i
            if idx < 1:
                continue
            prev_close = close[idx - 1]
            curr_close = close[idx]
            if prev_close > 0:
                pct = (curr_close - prev_close) / prev_close * 100
                if pct >= 9.5:
                    has_limit_up = True
                    limit_up_idx = idx
                    break

        if not has_limit_up:
            return None

        current = close.tail(1).item()
        limit_up_close = close[limit_up_idx]
        if limit_up_close == 0:
            return None

        pullback_pct = (current - limit_up_close) / limit_up_close * 100

        if pullback_pct > 0 or pullback_pct < -8:
            return None

        recent_vol = volume.tail(3).mean()
        limit_up_vol = volume[limit_up_idx]
        volume_shrink = limit_up_vol > 0 and recent_vol < limit_up_vol * 0.7

        ma5 = close.tail(5).mean()

        score = 60.0
        signals = {"limit_up_pullback": True}

        if abs(pullback_pct) < 5:
            score += 15
            signals["shallow_pullback"] = True

        if volume_shrink:
            score += 15
            signals["volume_shrink"] = True

        if ma5 and current >= ma5 * 0.98:
            score += 10
            signals["near_ma5"] = True

        return LimitUpSignal(
            code=code,
            name=name,
            strategy="limitup_pullback",
            score=min(score, 100),
            confidence=score / 100,
            signals=signals,
            factors={"pullback_pct": round(pullback_pct, 2)},
            reason=f"涨停后回调{pullback_pct:.1f}%，{'缩量' if volume_shrink else '放量'}",
        )


class DragonHeadStrategy:
    """龙回头策略"""

    def scan(self, kline: pl.DataFrame, code: str, name: str) -> Optional[LimitUpSignal]:
        if kline.height < 15:
            return None

        close = kline.select(pl.col("close")).to_series()
        volume = kline.select(pl.col("volume")).to_series()

        consecutive_boards = 0
        for i in range(min(10, close.len() - 1)):
            idx = close.len() - 1 - i
            if idx < 1:
                break
            prev = close[idx - 1]
            curr = close[idx]
            if prev > 0 and (curr - prev) / prev * 100 >= 9.5:
                consecutive_boards += 1
            else:
                break

        if consecutive_boards < 2:
            return None

        recent_days = min(5, close.len() - consecutive_boards)
        if recent_days < 3:
            return None

        recent_vol = volume.tail(recent_days).mean()
        peak_vol = volume[-(consecutive_boards + recent_days) : -recent_boards].mean() if close.len() > consecutive_boards + recent_days else volume.head(consecutive_boards).mean()
        volume_shrink = peak_vol > 0 and recent_vol < peak_vol * 0.6

        score = 65.0
        signals = {"dragon_head": True, "consecutive_boards": consecutive_boards >= 3}

        if consecutive_boards >= 3:
            score += 15
        if volume_shrink:
            score += 15
            signals["volume_shrink"] = True

        current = close.tail(1).item()
        ma5 = close.tail(5).mean()
        if ma5 and current >= ma5 * 0.97:
            score += 5
            signals["near_ma5"] = True

        return LimitUpSignal(
            code=code,
            name=name,
            strategy="dragon_head",
            score=min(score, 100),
            confidence=score / 100,
            signals=signals,
            factors={"consecutive_boards": consecutive_boards},
            reason=f"连板{consecutive_boards}天后回调，{'缩量' if volume_shrink else '放量'}",
        )


class RelayModeStrategy:
    """接力模式"""

    def scan(self, kline: pl.DataFrame, code: str, name: str, sector_heat: float = 0.5) -> Optional[LimitUpSignal]:
        if sector_heat < 0.7:
            return None

        if kline.height < 5:
            return None

        close = kline.select(pl.col("close")).to_series()
        volume = kline.select(pl.col("volume")).to_series()

        current = close.tail(1).item()
        prev = close.tail(2).item() if close.len() >= 2 else current

        if prev > 0:
            change_pct = (current - prev) / prev * 100
        else:
            change_pct = 0

        if change_pct < 3:
            return None

        recent_vol = volume.tail(3).mean()
        prev_vol = volume[-5:-3].mean() if volume.len() >= 5 else volume.mean()
        volume_ratio = recent_vol / prev_vol if prev_vol and prev_vol > 0 else 1.0

        score = 55.0
        signals = {"relay_mode": True}

        if sector_heat > 0.8:
            score += 15
            signals["hot_sector"] = True

        if volume_ratio > 1.5:
            score += 10
            signals["volume_expansion"] = True

        if change_pct > 5:
            score += 10
            signals["strong_surge"] = True

        return LimitUpSignal(
            code=code,
            name=name,
            strategy="relay_mode",
            score=min(score, 100),
            confidence=score / 100,
            signals=signals,
            factors={"sector_heat": sector_heat, "volume_ratio": round(volume_ratio, 2)},
            reason=f"板块热度{sector_heat:.1f}，接力上涨{change_pct:.1f}%",
        )


class ThemeLinkageStrategy:
    """题材联动"""

    def scan(self, kline: pl.DataFrame, code: str, name: str, sector_change: float = 0.0) -> Optional[LimitUpSignal]:
        if sector_change < 3.0:
            return None

        if kline.height < 5:
            return None

        close = kline.select(pl.col("close")).to_series()

        current = close.tail(1).item()
        prev_5 = close[-6] if close.len() >= 6 else close[0]
        stock_5d_change = (current - prev_5) / prev_5 * 100 if prev_5 > 0 else 0

        linkage = min(stock_5d_change / sector_change, 1.0) if sector_change > 0 else 0

        score = 50.0
        signals = {"theme_linkage": True}

        if linkage > 0.7:
            score += 20
            signals["high_linkage"] = True

        if sector_change > 5:
            score += 15
            signals["sector_breakout"] = True

        return LimitUpSignal(
            code=code,
            name=name,
            strategy="theme_linkage",
            score=min(score, 100),
            confidence=score / 100,
            signals=signals,
            factors={"sector_change": round(sector_change, 2), "linkage": round(linkage, 2)},
            reason=f"题材涨幅{sector_change:.1f}%，联动系数{linkage:.2f}",
        )


class FundResonanceStrategy:
    """资金共振"""

    def scan(self, kline: pl.DataFrame, code: str, name: str, fund_data: Optional[dict] = None) -> Optional[LimitUpSignal]:
        if fund_data is None:
            return None

        main_net_inflow = fund_data.get("main_net_inflow", 0)
        super_large_ratio = fund_data.get("super_large_ratio", 0)

        if main_net_inflow < 50_000_000:
            return None

        if super_large_ratio < 0.3:
            return None

        score = 70.0
        signals = {"fund_resonance": True}

        if main_net_inflow > 100_000_000:
            score += 15
            signals["huge_inflow"] = True

        if super_large_ratio > 0.5:
            score += 10
            signals["super_large_dominant"] = True

        if kline.height >= 5:
            close = kline.select(pl.col("close")).to_series()
            current = close.tail(1).item()
            prev = close[-2] if close.len() >= 2 else current
            if prev > 0 and (current - prev) / prev * 100 > 3:
                score += 5
                signals["price_surge"] = True

        return LimitUpSignal(
            code=code,
            name=name,
            strategy="fund_resonance",
            score=min(score, 100),
            confidence=score / 100,
            signals=signals,
            factors={"main_net_inflow": main_net_inflow, "super_large_ratio": round(super_large_ratio, 2)},
            reason=f"主力净流入{main_net_inflow / 10000:.0f}万，超大单占比{super_large_ratio:.0%}",
        )


class LateSurgeStrategy:
    """尾盘突袭"""

    def scan(self, kline: pl.DataFrame, code: str, name: str, intraday_data: Optional[dict] = None) -> Optional[LimitUpSignal]:
        if intraday_data is None:
            return None

        late_change = intraday_data.get("late_change_1430", 0)
        volume_ratio = intraday_data.get("volume_ratio", 1.0)

        if late_change < 5:
            return None

        if volume_ratio < 3:
            return None

        score = 65.0
        signals = {"late_surge": True}

        if late_change > 7:
            score += 15
            signals["strong_late_surge"] = True

        if volume_ratio > 5:
            score += 10
            signals["huge_volume"] = True

        return LimitUpSignal(
            code=code,
            name=name,
            strategy="late_surge",
            score=min(score, 100),
            confidence=score / 100,
            signals=signals,
            factors={"late_change": round(late_change, 2), "volume_ratio": round(volume_ratio, 2)},
            reason=f"14:30后涨幅{late_change:.1f}%，量比{volume_ratio:.1f}",
        )


class LimitUpStrategyEngine:
    """涨停预测引擎 - 六大模型融合"""

    def __init__(self):
        self.strategies = {
            "limitup_pullback": LimitUpPullbackStrategy(),
            "dragon_head": DragonHeadStrategy(),
            "relay_mode": RelayModeStrategy(),
            "theme_linkage": ThemeLinkageStrategy(),
            "fund_resonance": FundResonanceStrategy(),
            "late_surge": LateSurgeStrategy(),
        }

    def scan_all(
        self,
        kline: pl.DataFrame,
        code: str,
        name: str,
        sector_heat: float = 0.5,
        sector_change: float = 0.0,
        fund_data: Optional[dict] = None,
        intraday_data: Optional[dict] = None,
    ) -> List[LimitUpSignal]:
        results = []

        r = self.strategies["limitup_pullback"].scan(kline, code, name)
        if r:
            results.append(r)

        r = self.strategies["dragon_head"].scan(kline, code, name)
        if r:
            results.append(r)

        r = self.strategies["relay_mode"].scan(kline, code, name, sector_heat)
        if r:
            results.append(r)

        r = self.strategies["theme_linkage"].scan(kline, code, name, sector_change)
        if r:
            results.append(r)

        r = self.strategies["fund_resonance"].scan(kline, code, name, fund_data)
        if r:
            results.append(r)

        r = self.strategies["late_surge"].scan(kline, code, name, intraday_data)
        if r:
            results.append(r)

        results.sort(key=lambda x: x.score, reverse=True)
        return results

    def scan_single(self, strategy_name: str, kline: pl.DataFrame, code: str, name: str, **kwargs) -> Optional[LimitUpSignal]:
        strategy = self.strategies.get(strategy_name)
        if not strategy:
            return None

        if strategy_name == "relay_mode":
            return strategy.scan(kline, code, name, kwargs.get("sector_heat", 0.5))
        elif strategy_name == "theme_linkage":
            return strategy.scan(kline, code, name, kwargs.get("sector_change", 0.0))
        elif strategy_name == "fund_resonance":
            return strategy.scan(kline, code, name, kwargs.get("fund_data"))
        elif strategy_name == "late_surge":
            return strategy.scan(kline, code, name, kwargs.get("intraday_data"))
        else:
            return strategy.scan(kline, code, name)
