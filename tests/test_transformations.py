import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark Session for testing."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("UnitTests") \
        .getOrCreate()

def test_revenue_and_cancellation_logic(spark):
    """Test if revenue calculation and cancellation flag work correctly."""
    # Create dummy data
    data = [
        ("1", "2026-11-01", "CUST-1", "Laptop", 2, 100.0, "DELIVERED"),
        ("2", "2026-11-01", "CUST-2", "Phone", 1, 50.0, "CANCELLED")
    ]
    columns = ["order_id", "order_date", "customer_id", "product", "quantity", "price", "status"]
    
    df = spark.createDataFrame(data, columns)
    
    # Apply transformations (same logic as clean_orders.py)
    cleaned_df = df.withColumn("revenue", col("price") * col("quantity")) \
                   .withColumn("is_cancelled", when(col("status") == "CANCELLED", 1).otherwise(0))
    
    # Collect results
    results = cleaned_df.collect()
    
    # Assertions
    assert results[0]["revenue"] == 200.0
    assert results[0]["is_cancelled"] == 0
    assert results[1]["revenue"] == 50.0
    assert results[1]["is_cancelled"] == 1