import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Creates a local Spark session for testing purposes.
    The session is created once for the entire test suite.
    """
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("DatabricksSamplingTests")
        # Reduce logging noise during tests
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    
    yield spark_session
    
    # Teardown
    spark_session.stop()
  
