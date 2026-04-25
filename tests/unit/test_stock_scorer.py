"""
股票评分器单元测试
测试综合评分计算和权重分配
"""
import pytest
from services.stock_service.scorer import StockScorer
from core.models import StockScore


class TestStockScorer:
    """股票评分器测试"""

    @pytest.fixture
    def scorer(self):
        """创建评分器实例"""
        return StockScorer()

    def test_calculate_total_score_basic(self, scorer):
        """测试基本评分计算"""
        score = scorer.calculate_total_score(
            fundamental_score=80.0,
            volume_price_score=70.0,
            fund_flow_score=75.0,
            sentiment_score=65.0
        )
        
        # 验证计算结果
        expected = (
            80.0 * 0.35 +  # fundamental
            70.0 * 0.25 +  # volume_price
            75.0 * 0.25 +  # fund_flow
            65.0 * 0.15    # sentiment
        )
        
        assert score == round(expected, 2)

    def test_calculate_total_score_equal_weights(self, scorer):
        """测试相同分数时的计算"""
        score = scorer.calculate_total_score(
            fundamental_score=50.0,
            volume_price_score=50.0,
            fund_flow_score=50.0,
            sentiment_score=50.0
        )
        
        assert score == 50.0

    def test_calculate_total_score_max_values(self, scorer):
        """测试最大值计算"""
        score = scorer.calculate_total_score(
            fundamental_score=100.0,
            volume_price_score=100.0,
            fund_flow_score=100.0,
            sentiment_score=100.0
        )
        
        assert score == 100.0

    def test_calculate_total_score_min_values(self, scorer):
        """测试最小值计算"""
        score = scorer.calculate_total_score(
            fundamental_score=0.0,
            volume_price_score=0.0,
            fund_flow_score=0.0,
            sentiment_score=0.0
        )
        
        assert score == 0.0

    def test_calculate_total_score_single_dimension_high(self, scorer):
        """测试单一维度高分的影响"""
        # 只有基本面高分
        score = scorer.calculate_total_score(
            fundamental_score=100.0,
            volume_price_score=0.0,
            fund_flow_score=0.0,
            sentiment_score=0.0
        )
        
        # 基本面权重 35%
        assert score == 35.0

    def test_calculate_total_score_weight_distribution(self, scorer):
        """测试权重分配正确性"""
        # 验证权重总和为 1
        total_weight = sum(StockScorer.WEIGHTS.values())
        assert total_weight == 1.0
        
        # 验证各维度权重
        assert StockScorer.WEIGHTS['fundamental'] == 0.35
        assert StockScorer.WEIGHTS['volume_price'] == 0.25
        assert StockScorer.WEIGHTS['fund_flow'] == 0.25
        assert StockScorer.WEIGHTS['sentiment'] == 0.15


class TestStockScorerCreateScore:
    """测试创建评分对象"""

    @pytest.fixture
    def scorer(self):
        return StockScorer()

    def test_create_score_basic(self, scorer):
        """测试基本评分对象创建"""
        score_obj = scorer.create_score(
            code='000001',
            name='平安银行',
            fundamental_score=80.0,
            volume_price_score=70.0,
            fund_flow_score=75.0,
            sentiment_score=65.0,
            reasons=['基本面良好', '技术面强势']
        )
        
        assert isinstance(score_obj, StockScore)
        assert score_obj.code == '000001'
        assert score_obj.name == '平安银行'
        assert score_obj.fundamental_score == 80.0
        assert score_obj.volume_price_score == 70.0
        assert score_obj.fund_flow_score == 75.0
        assert score_obj.sentiment_score == 65.0
        assert len(score_obj.reasons) == 2

    def test_create_score_default_values(self, scorer):
        """测试默认值处理"""
        score_obj = scorer.create_score(
            code='000001',
            name='测试股票'
        )
        
        assert score_obj.code == '000001'
        assert score_obj.name == '测试股票'
        assert score_obj.fundamental_score == 0
        assert score_obj.volume_price_score == 0
        assert score_obj.fund_flow_score == 0
        assert score_obj.sentiment_score == 0
        assert score_obj.reasons == []

    def test_create_score_total_calculation(self, scorer):
        """测试总分自动计算"""
        score_obj = scorer.create_score(
            code='000001',
            name='平安银行',
            fundamental_score=80.0,
            volume_price_score=70.0,
            fund_flow_score=75.0,
            sentiment_score=65.0
        )
        
        expected_total = scorer.calculate_total_score(
            80.0, 70.0, 75.0, 65.0
        )
        
        assert score_obj.total_score == expected_total


class TestStockScorerEdgeCases:
    """边界条件测试"""

    @pytest.fixture
    def scorer(self):
        return StockScorer()

    def test_negative_scores(self, scorer):
        """测试负分处理"""
        score = scorer.calculate_total_score(
            fundamental_score=-10.0,
            volume_price_score=50.0,
            fund_flow_score=50.0,
            sentiment_score=50.0
        )
        
        # 负分应该被正常计算
        assert score < 50.0

    def test_scores_over_100(self, scorer):
        """测试超百分处理"""
        score = scorer.calculate_total_score(
            fundamental_score=150.0,
            volume_price_score=50.0,
            fund_flow_score=50.0,
            sentiment_score=50.0
        )
        
        # 超百分应该被正常计算
        assert score > 50.0

    def test_floating_point_precision(self, scorer):
        """测试浮点数精度"""
        score = scorer.calculate_total_score(
            fundamental_score=66.666,
            volume_price_score=77.777,
            fund_flow_score=88.888,
            sentiment_score=55.555
        )
        
        # 结果应该保留2位小数
        assert len(str(score).split('.')[-1]) <= 2

    def test_very_small_scores(self, scorer):
        """测试极小分数"""
        score = scorer.calculate_total_score(
            fundamental_score=0.01,
            volume_price_score=0.01,
            fund_flow_score=0.01,
            sentiment_score=0.01
        )
        
        assert score > 0
        assert score < 1


class TestStockScorerRealWorldScenarios:
    """真实场景测试"""

    @pytest.fixture
    def scorer(self):
        return StockScorer()

    def test_strong_fundamental_weak_technical(self, scorer):
        """测试基本面强但技术面弱"""
        score_obj = scorer.create_score(
            code='000001',
            name='价值股',
            fundamental_score=95.0,  # 基本面很强
            volume_price_score=30.0,  # 技术面较弱
            fund_flow_score=40.0,
            sentiment_score=50.0,
            reasons=['低估值', '高股息']
        )
        
        # 基本面权重最高(35%)，总分应该中等偏上
        assert 50 < score_obj.total_score < 70

    def test_weak_fundamental_strong_technical(self, scorer):
        """测试基本面弱但技术面强"""
        score_obj = scorer.create_score(
            code='000002',
            name='题材股',
            fundamental_score=30.0,  # 基本面较弱
            volume_price_score=90.0,  # 技术面很强
            fund_flow_score=85.0,    # 资金流入
            sentiment_score=80.0,    # 情绪高涨
            reasons=['突破形态', '量价齐升']
        )
        
        # 技术面+资金面+情绪面权重共65%，总分应该较高
        assert score_obj.total_score > 60

    def test_balanced_stock(self, scorer):
        """测试各方面均衡的股票"""
        score_obj = scorer.create_score(
            code='000003',
            name='均衡股',
            fundamental_score=70.0,
            volume_price_score=70.0,
            fund_flow_score=70.0,
            sentiment_score=70.0,
            reasons=['稳健增长']
        )
        
        # 各方面均衡，总分应该为70
        assert score_obj.total_score == 70.0

    def test_all_time_high_stock(self, scorer):
        """测试历史新高股票"""
        score_obj = scorer.create_score(
            code='000004',
            name='强势股',
            fundamental_score=85.0,
            volume_price_score=95.0,
            fund_flow_score=90.0,
            sentiment_score=85.0,
            reasons=['历史新高', '资金持续流入', '业绩超预期']
        )
        
        # 各方面都很强，总分应该很高
        assert score_obj.total_score > 85


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
