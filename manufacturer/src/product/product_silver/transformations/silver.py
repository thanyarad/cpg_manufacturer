from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim, to_date, lit
from pyspark.sql.types import DoubleType
from manufacturer.package.schema import get_schema
from manufacturer.utils.transform_utils import (
    trim_string_columns,
    normalize_digits,
    validate_numeric,
    convert_to_date,
)
# catalog="dev"
# from_schema="01_bronze"
# to_schema="02_silver"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
product_schema=get_schema("product",schema_path)

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.product_mv")
@dp.expect_or_drop("valid_product", "product_id IS NOT NULL")
def product_mv():
    df = spark.read.table("product_mv")

    # Trim
    string_cols = [
        "product_name", "brand", "manufacturer", "category", "department",
        "description", "sku_id", "upc", "gtin",
        "unit_of_measurement", "product_status"
    ]
    df = trim_string_columns(df, string_cols)

    # Normalize UPC/GTIN
    df = normalize_digits(df, "upc")
    df = normalize_digits(df, "gtin")

    # Validate numeric columns
    df = validate_numeric(df, ["unit_price", "retail_price"])

    # Convert date
    df = convert_to_date(df, "release_date")

    return df