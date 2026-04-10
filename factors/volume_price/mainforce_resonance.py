"""
主力痕迹共振因子
基于四大主力信号计算共振强度: S1涨停质量 + S2缺口强度 + S3连阳强度 + S4放量强度
"""
import polars as pl
import numpy as np
from core.factor_library import BaseFactor, register_factor


@register_factor("mainforce_resonance")
class MainForceResonanceFactor(BaseFactor):
    """主力痕迹共振因子

    综合评分 = (S1得分 + S2得分 + S3得分 + S4得分) / 4 * 100

    S1(涨停质量, 25分):
        - 检测20日内是否有涨停 + 量能1.5倍 + 横盘不跌
        - 满分条件: 10日内涨停且量能>2倍

    S2(缺口强度, 25分):
        - 检测3日内是否有未回补向上跳空缺口
        - 满分条件: 缺口>3%且距今<20日

    S3(连阳强度, 25分):
        - 检测连续3日收盘创新高且不破前日收盘
        - 满分条件: 连续5日以上

    S4(放量强度, 25分):
        - 检测放量突破日均量2倍且站稳
        - 满分条件: 量能>3倍日均且价格持续上涨
    """

    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "mainforce_resonance",
            category=category or "volume_price",
            params=params or {},
            description=description or "主力痕迹共振强度评分(0-100)"
        )

    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        data = data.sort(["code", "trade_date"])

        close = data["close"].to_numpy()
        prev_close = data["close"].shift(1).to_numpy()
        volume = data["volume"].to_numpy()
        low = data["low"].to_numpy()
        high = data["high"].to_numpy()
        trade_date = data["trade_date"].to_numpy()

        scores = np.zeros(len(data))
        s1_scores = np.zeros(len(data))
        s2_scores = np.zeros(len(data))
        s3_scores = np.zeros(len(data))
        s4_scores = np.zeros(len(data))

        codes = data["code"].to_numpy()
        unique_codes = np.unique(codes)

        for code in unique_codes:
            idx = np.where(codes == code)[0]
            if len(idx) < 11:
                continue

            code_close = close[idx]
            code_prev_close = prev_close[idx]
            code_volume = volume[idx]
            code_low = low[idx]
            code_high = high[idx]
            code_trade_date = trade_date[idx]

            avg_vol = code_volume[:-1].mean() if len(code_volume) > 1 else code_volume.mean()

            for i in idx:
                local_i = np.where(idx == i)[0][0]
                lookback = min(20, local_i)

                if lookback < 10:
                    continue

                s1_score = 0.0
                for j in range(max(1, local_i - lookback), local_i):
                    if code_close[j] >= code_prev_close[j] * 1.095:
                        vol_ratio = code_volume[j] / code_volume[j-1] if code_volume[j-1] > 0 else 0
                        if vol_ratio >= 1.5:
                            consolidation_ok = True
                            for k in range(j + 1, min(j + 11, len(code_close))):
                                if code_low[k] < code_close[j] * 0.97:
                                    consolidation_ok = False
                                    break
                                if code_close[k] < code_prev_close[k] * 0.9:
                                    consolidation_ok = False
                                    break
                            if consolidation_ok:
                                base_score = 12.5
                                vol_bonus = min(12.5, (vol_ratio - 1.5) * 5)
                                s1_score = min(25.0, base_score + vol_bonus)
                                break

                gap_score = 0.0
                for j in range(max(2, local_i - 3), local_i):
                    if code_low[j] > code_high[j-1]:
                        gap_size = (code_low[j] - code_high[j-1]) / code_high[j-1]
                        days_since = local_i - j
                        if days_since <= 20:
                            base = min(15.0, gap_size * 200)
                            recency = max(0, 10.0 - days_since * 0.3)
                            gap_score = min(25.0, base + recency)
                            break

                consecutive = 0
                cons_start = local_i
                for j in range(max(1, local_i - 5), local_i + 1):
                    if j == 0:
                        continue
                    if code_close[j] > code_close[j-1] and code_low[j] >= code_close[j-1]:
                        if consecutive == 0:
                            cons_start = j - 1
                        consecutive += 1
                    else:
                        if consecutive >= 3:
                            break
                        consecutive = 0

                if consecutive >= 3:
                    s3_score = min(25.0, 10.0 + consecutive * 3)
                else:
                    s3_score = 0.0

                vol_score = 0.0
                if local_i >= 3 and avg_vol > 0:
                    for j in range(max(3, local_i - 5), local_i):
                        if code_volume[j] >= avg_vol * 2:
                            vol_ratio_j = code_volume[j] / avg_vol
                            breakout_price = code_close[j]
                            standing = True
                            for k in range(j + 1, local_i + 1):
                                if code_close[k] < breakout_price:
                                    standing = False
                                    break
                            if standing:
                                base = min(15.0, (vol_ratio_j - 2) * 5)
                                duration = min(10, local_i - j)
                                vol_score = min(25.0, base + duration * 1)
                                break

                total_score = s1_score + gap_score + s3_score + vol_score

                scores[i] = total_score
                s1_scores[i] = s1_score
                s2_scores[i] = gap_score
                s3_scores[i] = s3_score
                s4_scores[i] = vol_score

        data = data.with_columns([
            pl.Series("mainforce_resonance", scores).alias("mainforce_resonance"),
            pl.Series("mf_s1_limit_up", s1_scores).alias("mf_s1_limit_up"),
            pl.Series("mf_s2_gap", s2_scores).alias("mf_s2_gap"),
            pl.Series("mf_s3_consecutive", s3_scores).alias("mf_s3_consecutive"),
            pl.Series("mf_s4_volume", s4_scores).alias("mf_s4_volume"),
        ])

        factor_col = self.get_factor_column_name()
        data = data.with_columns([
            pl.col("mainforce_resonance").alias(factor_col),
        ])

        data = data.with_columns([
            pl.col(factor_col).fill_null(0.0).fill_nan(0.0),
            pl.col("mf_s1_limit_up").fill_null(0.0).fill_nan(0.0),
            pl.col("mf_s2_gap").fill_null(0.0).fill_nan(0.0),
            pl.col("mf_s3_consecutive").fill_null(0.0).fill_nan(0.0),
            pl.col("mf_s4_volume").fill_null(0.0).fill_nan(0.0),
        ])

        return data