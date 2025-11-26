import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, length, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_silver_trim_string_columns(spark, silver_distributor_data):
    df = silver_distributor_data
    
    string_cols = ["distributor_name", "city", "state", "country"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    
    test_data = [
        (1, "  Metro Food  ", "  Chicago  ", "  Illinois  ", "  United States  ", "60609"),
    ]
    test_schema = StructType([
        StructField("distributor_id", IntegerType()),
        StructField("distributor_name", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("postal_code", StringType())
    ])
    test_df = spark.createDataFrame(test_data, test_schema)
    
    for c in string_cols:
        test_df = test_df.withColumn(c, trim(col(c)))
    
    result = test_df.collect()[0]
    assert result["distributor_name"] == "Metro Food"
    assert result["city"] == "Chicago"
    assert result["state"] == "Illinois"
    assert result["country"] == "United States"


def test_silver_postal_code_validation_valid(spark):
    data = [
        (1, "123456"),
        (2, "123 456"),
        (3, "  123456  "),
    ]
    schema = StructType([
        StructField("distributor_id", IntegerType()),
        StructField("postal_code", StringType())
    ])
    df = spark.createDataFrame(data, schema)
    
    df = df.withColumn(
        "postal_code",
        when(
            length(regexp_replace(col("postal_code"), r"\s+", "")) == 6,
            regexp_replace(col("postal_code"), r"\s+", "")
        ).otherwise("Invalid")
    )
    
    results = df.collect()
    assert results[0]["postal_code"] == "123456"
    assert results[1]["postal_code"] == "123456"
    assert results[2]["postal_code"] == "123456"


def test_silver_postal_code_validation_invalid(spark):
    data = [
        (1, "12345"),
        (2, "1234567"),
        (3, "ABC123"),
        (4, ""),
    ]
    schema = StructType([
        StructField("distributor_id", IntegerType()),
        StructField("postal_code", StringType())
    ])
    df = spark.createDataFrame(data, schema)
    
    df = df.withColumn(
        "postal_code",
        when(
            length(regexp_replace(col("postal_code"), r"\s+", "")) == 6,
            regexp_replace(col("postal_code"), r"\s+", "")
        ).otherwise("Invalid")
    )
    
    results = df.collect()
    for row in results:
        assert row["postal_code"] == "Invalid"


def test_silver_expect_or_drop_null_distributor_id(spark):
    data = [
        (1, "Valid Distributor"),
        (None, "Invalid Distributor"),
        (3, "Another Valid"),
    ]
    schema = StructType([
        StructField("distributor_id", IntegerType()),
        StructField("distributor_name", StringType())
    ])
    df = spark.createDataFrame(data, schema)
    
    filtered_df = df.filter(col("distributor_id").isNotNull())
    
    results = filtered_df.collect()
    assert len(results) == 2
    assert all(row["distributor_id"] is not None for row in results)


def test_silver_transformation_pipeline(spark):
    data = [
        (1, "  Metro Food  ", "  60609  ", "  Chicago  ", "  Illinois  ", "  United States  "),
        (2, "Coastal Goods", "12345", "Los Angeles", "California", "United States"),
        (3, "Valid Name", "123456", "Atlanta", "Georgia", "United States"),
    ]
    schema = StructType([
        StructField("distributor_id", IntegerType()),
        StructField("distributor_name", StringType()),
        StructField("postal_code", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType())
    ])
    df = spark.createDataFrame(data, schema)
    
    string_cols = ["distributor_name", "city", "state", "country"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    
    df = df.withColumn(
        "postal_code",
        when(
            length(regexp_replace(col("postal_code"), r"\s+", "")) == 6,
            regexp_replace(col("postal_code"), r"\s+", "")
        ).otherwise("Invalid")
    )
    
    df = df.filter(col("distributor_id").isNotNull())
    
    results = df.collect()
    assert len(results) == 3
    assert results[0]["distributor_name"] == "Metro Food"
    assert results[0]["postal_code"] == "60609"
    assert results[1]["postal_code"] == "Invalid"
    assert results[2]["postal_code"] == "123456"

