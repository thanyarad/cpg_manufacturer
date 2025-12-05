from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim, to_date, to_timestamp, lit
from pyspark.sql.types import IntegerType, BooleanType
from manufacturer.package.schema import get_schema
from manufacturer.utils.transform_utils import (
    trim_string_columns,
    normalize_phone,
    validate_email,
    convert_to_date,
    validate_non_negative,
    validate_positive_integer
)
# catalog = "dev"
# from_schema = "01_bronze"
# to_schema = "02_silver"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
inventory_schema=get_schema("inventory",schema_path)

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.inventory_mv")
@dp.expect_or_drop("valid_inventory", "inventory_id IS NOT NULL")
def inventory_mv():
    df = spark.read.table("inventory_mv")

    # Trim common string columns
    string_cols = [
        "location_type", "location_name", "location_code", "address",
        "city", "state", "country", "phone", "email", "inventory_status"
    ]
    df = trim_string_columns(df, string_cols)
    df = normalize_phone(df, "phone")
    df = validate_email(df, "email")

    numeric_cols = [
        "product_id",
        "quantity_on_hand",
        "reorder_level",
        "reorder_quantity",
        "safety_stock_level"
    ]
    df = validate_non_negative(df, numeric_cols)
    df = convert_to_date(df, "last_restock_date", "yyyy-MM-dd")
    df = df.withColumn("last_updated", 
                       to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss"))

    
    df = df.withColumn("location_is_active", col("location_is_active").cast(BooleanType()))

   
    # -------------------------------------------------------------------
    df = df.filter(col("location_is_active") == True)

    df = df.withColumn(
        "is_below_reorder",
        (col("quantity_on_hand") < col("reorder_level"))
    )

    return df
