import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class TestSparkSampling:
    
    @pytest.fixture(autouse=True)
    def setup_data(self, spark):
        """Creates a mock DataFrame of 1,000 rows for the tests to act on."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("device_type", StringType(), True)
        ])
        
        # Generate 1000 dummy rows
        data = [(i, "sensor_a" if i % 2 == 0 else "sensor_b") for i in range(1000)]
        self.df = spark.createDataFrame(data, schema)

    def test_bernoulli_sample_execution(self, spark):
        """Verifies the PySpark .sample() method runs and returns an approximate target."""
        # Setup: Target 100 rows out of the 1,000 row mock dataframe
        target_rows = 100
        fraction = target_rows / self.df.count()
        
        # Execution
        sampled_df = self.df.sample(withReplacement=False, fraction=fraction, seed=42)
        sampled_count = sampled_df.count()
        
        # Assertion: Bernoulli isn't exact, but it should be within a reasonable variance (+/- 25 rows)
        assert 75 <= sampled_count <= 125
        assert len(sampled_df.columns) == 2

    def test_sample_never_returns_duplicates(self, spark):
        """Verifies withReplacement=False is strictly honored."""
        fraction = 0.5
        sampled_df = self.df.sample(withReplacement=False, fraction=fraction, seed=42)
        
        # Group by ID and count. The max count for any ID should be exactly 1.
        max_duplicates = sampled_df.groupBy("id").count().agg({"count": "max"}).collect()[0][0]
        
        assert max_duplicates == 1
      
