import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from distributor.distributor_silver.utilities.utils import is_valid_email


def test_is_valid_email_valid_emails(spark):
    test_cases = [
        "test@example.com",
        "user.name@domain.co.uk",
        "user+tag@example.org",
        "user_name@test-domain.com",
        "123@456.789"
    ]
    
    schema = StructType([StructField("email", StringType(), True)])
    data = [(email,) for email in test_cases]
    df = spark.createDataFrame(data, schema)
    
    result_df = df.withColumn("is_valid", is_valid_email("email"))
    results = result_df.collect()
    
    for row in results:
        assert row["is_valid"] == True, f"Email {row['email']} should be valid"


def test_is_valid_email_invalid_emails(spark):
    test_cases = [
        "invalid.email",
        "@example.com",
        "user@",
        "user@domain",
        "user name@example.com",
        "user@domain..com",
        ""
    ]
    
    schema = StructType([StructField("email", StringType(), True)])
    data = [(email,) for email in test_cases]
    df = spark.createDataFrame(data, schema)
    
    result_df = df.withColumn("is_valid", is_valid_email("email"))
    results = result_df.collect()
    
    for row in results:
        assert row["is_valid"] == False, f"Email {row['email']} should be invalid"


def test_is_valid_email_null(spark):
    schema = StructType([StructField("email", StringType(), True)])
    data = [(None,)]
    df = spark.createDataFrame(data, schema)
    
    result_df = df.withColumn("is_valid", is_valid_email("email"))
    result = result_df.collect()[0]
    
    assert result["is_valid"] == False

