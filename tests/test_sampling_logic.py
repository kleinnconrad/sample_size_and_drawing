import pytest
import math

# Mocking the imports that would normally come from your src/ directory
# from src.samplers import calculate_cochran, calculate_bernoulli_fraction

def calculate_cochran(z_score, p, e, N=None):
    if N == 0: return 0
    n0 = ((z_score**2) * p * (1-p)) / (e**2)
    if N is not None:
        return math.ceil(n0 / (1 + ((n0 - 1) / N)))
    return math.ceil(n0)

def calculate_bernoulli_fraction(target_size, total_rows):
    if total_rows == 0: return 0.0
    if target_size >= total_rows: return 1.0
    return target_size / total_rows

class TestCochranCalculator:
    def test_infinite_population(self):
        # 95% Confidence (1.96 Z-score), 50% Proportion, 5% Error
        result = calculate_cochran(1.96, 0.5, 0.05, N=None)
        assert result == 385
        
    def test_finite_population_correction(self):
        # Adjusting the 385 base requirement for a small 1,000 row table
        result = calculate_cochran(1.96, 0.5, 0.05, N=1000)
        assert result == 278
        
    def test_empty_table_returns_zero(self):
        result = calculate_cochran(1.96, 0.5, 0.05, N=0)
        assert result == 0

class TestBernoulliFractionCalculator:
    def test_standard_fraction_calculation(self):
        # 1,000 out of 100,000 should be exactly 0.01
        result = calculate_bernoulli_fraction(1000, 100000)
        assert result == 0.01

    def test_target_exceeds_population_caps_at_one(self):
        # Trying to sample 5k rows from a 2k table should return 100% of data
        result = calculate_bernoulli_fraction(5000, 2000)
        assert result == 1.0

    def test_empty_table_returns_zero(self):
        result = calculate_bernoulli_fraction(1000, 0)
        assert result == 0.0
      
