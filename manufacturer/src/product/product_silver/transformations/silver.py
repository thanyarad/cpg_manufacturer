from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim, to_date, lit
from pyspark.sql.types import DoubleType

# catalog="dev"
# from_schema="01_bronze"
# to_schema="02_silver"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.product_mv")
@dp.expect_or_drop("valid_product", "product_id IS NOT NULL")
def product_mv():
    df = spark.read.table("product_mv")

    # Trim common string columns from the product JSON
    string_cols = [
        "product_name", "brand", "manufacturer", "category",
        "department", "description", "sku_id", "upc", "gtin",
        "unit_of_measurement", "product_status"
    ]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))

    # Normalize UPC/GTIN to digits only; set to null if too short
    df = df.withColumn("upc_digits", regexp_replace(col("upc"), r"[^\d]", ""))
    df = df.withColumn("gtin_digits", regexp_replace(col("gtin"), r"[^\d]", ""))
    df = df.withColumn("upc", when(length(col("upc_digits")) >= 8, col("upc_digits")).otherwise(lit(None))).drop("upc_digits")
    df = df.withColumn("gtin", when(length(col("gtin_digits")) >= 8, col("gtin_digits")).otherwise(lit(None))).drop("gtin_digits")

    # Ensure numeric prices are doubles and non-negative
    df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
    df = df.withColumn("retail_price", col("retail_price").cast(DoubleType()))
    df = df.withColumn("unit_price", when(col("unit_price") >= 0, col("unit_price")).otherwise(lit(None)))
    df = df.withColumn("retail_price", when(col("retail_price") >= 0, col("retail_price")).otherwise(lit(None)))

    # Convert release_date to date
    df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

    # Filter to active products only
    # df = df.filter(col("product_status") == "Active")

    return df
