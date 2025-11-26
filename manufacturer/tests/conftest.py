import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType


@pytest.fixture(scope="session")
def spark():
    try:
        spark_session = SparkSession.getActiveSession()
        if spark_session is not None:
            return spark_session
    except:
        pass
    
    spark_session = SparkSession.builder \
        .appName("distributor_tests") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    
    yield spark_session
    
    try:
        if SparkSession.getActiveSession() != spark_session:
            spark_session.stop()
    except:
        pass


@pytest.fixture
def sample_distributor_data():
    return [
        {
            "distributor_id": 1,
            "distributor_name": "Metro Food & Beverage Distribution",
            "phone_number": "(312) 555-0201",
            "street_address": "2500 Distribution Center Drive",
            "postal_code": "60609",
            "city": "Chicago",
            "state": "Illinois",
            "country": "United States",
            "operation": "insert"
        },
        {
            "distributor_id": 2,
            "distributor_name": "Coastal Consumer Goods Distribution",
            "phone_number": "(213) 555-0202",
            "street_address": "8800 Harbor Boulevard",
            "postal_code": "90001",
            "city": "Los Angeles",
            "state": "California",
            "country": "United States",
            "operation": "insert"
        }
    ]


@pytest.fixture
def distributor_schema():
    return StructType([
        StructField("distributor_id", IntegerType()),
        StructField("distributor_name", StringType()),
        StructField("phone_number", StringType()),
        StructField("street_address", StringType()),
        StructField("postal_code", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("operation", StringType())
    ])


@pytest.fixture
def bronze_distributor_data_with_metadata(spark):
    data = [
        (1, "Metro Food", "(312) 555-0201", "2500 Drive", "60609", "Chicago", "Illinois", "United States", "insert", "2024-01-01 10:00:00", None, "2024-01-01 10:00:00"),
        (2, "Coastal Goods", "(213) 555-0202", "8800 Boulevard", "90001", "Los Angeles", "California", "United States", "insert", "2024-01-01 11:00:00", None, "2024-01-01 11:00:00"),
        (1, "Metro Food Updated", "(312) 555-0201", "2500 Drive", "60609", "Chicago", "Illinois", "United States", "update", "2024-01-01 12:00:00", "2024-01-01 12:00:00", "2024-01-01 10:00:00"),
    ]
    schema = StructType([
        StructField("distributor_id", IntegerType()),
        StructField("distributor_name", StringType()),
        StructField("phone_number", StringType()),
        StructField("street_address", StringType()),
        StructField("postal_code", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("operation", StringType()),
        StructField("ingestion_time", StringType()),
        StructField("__END_AT", StringType()),
        StructField("__START_AT", StringType())
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def silver_distributor_data(spark):
    data = [
        (1, "Metro Food", "60609", "Chicago", "Illinois", "United States"),
        (2, "Coastal Goods", "90001", "Los Angeles", "California", "United States"),
        (3, None, "30309", "Atlanta", "Georgia", "United States"),
    ]
    schema = StructType([
        StructField("distributor_id", IntegerType()),
        StructField("distributor_name", StringType()),
        StructField("postal_code", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType())
    ])
    return spark.createDataFrame(data, schema)

