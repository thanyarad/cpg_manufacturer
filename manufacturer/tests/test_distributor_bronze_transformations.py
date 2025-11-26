import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_bronze_schema_definition():
    from distributor.distributor_bronze.transformations.landing import distributor_schema
    
    expected_fields = [
        "distributor_id", "distributor_name", "phone_number",
        "street_address", "postal_code", "city", "state", "country", "operation"
    ]
    
    assert distributor_schema is not None
    field_names = [field.name for field in distributor_schema.fields]
    
    for field_name in expected_fields:
        assert field_name in field_names, f"Field {field_name} missing from schema"
    
    assert distributor_schema["distributor_id"].dataType.typeName() == "integer"
    assert distributor_schema["distributor_name"].dataType.typeName() == "string"


def test_bronze_filter_active_records(spark, bronze_distributor_data_with_metadata):
    df = bronze_distributor_data_with_metadata
    
    cols = [c for c in df.columns if not c.startswith("__")]
    result_df = df.select(*cols).filter(col("__END_AT").isNull()).drop("operation", "ingestion_time")
    
    results = result_df.collect()
    
    assert len(results) == 2
    
    distributor_ids = [row["distributor_id"] for row in results]
    assert 1 in distributor_ids
    assert 2 in distributor_ids
    
    for row in results:
        assert "operation" not in row.asDict()
        assert "ingestion_time" not in row.asDict()
        assert "__END_AT" not in row.asDict()
        assert "__START_AT" not in row.asDict()


def test_bronze_excludes_metadata_columns(spark, bronze_distributor_data_with_metadata):
    df = bronze_distributor_data_with_metadata
    
    cols = [c for c in df.columns if not c.startswith("__")]
    result_df = df.select(*cols).filter(col("__END_AT").isNull())
    
    result_columns = result_df.columns
    
    assert "__END_AT" not in result_columns
    assert "__START_AT" not in result_columns
    assert "distributor_id" in result_columns
    assert "distributor_name" in result_columns

