from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    regexp_replace,
    when,
    length,
    trim,
    to_date,
    to_timestamp,
    lit
)
from pyspark.sql.types import IntegerType, BooleanType

catalog = "dev"
from_schema = "01_bronze"
to_schema = "02_silver"


@dp.materialized_view(name=f"{catalog}.{to_schema}.inventory_mv")
@dp.expect_or_drop("valid_inventory", "inventory_id IS NOT NULL")
def inventory_mv():
    df = spark.read.table(f"{catalog}.{from_schema}.inventory_mv")

    # Trim common string columns
    string_cols = [
        "location_type",
        "location_name",
        "location_code",
        "address",
        "city",
        "state",
        "country",
        "phone",
        "email",
        "inventory_status"
    ]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))

    # Normalize phone: keep digits only, null if too short
    df = df.withColumn("phone_digits", regexp_replace(col("phone"), r"[^\d]", ""))
    df = df.withColumn("phone", when(length(col("phone_digits")) >= 7, col("phone_digits")).otherwise(lit(None))).drop("phone_digits")

    # Simple email normalization/validation: lowercase and set null if missing '@'
    df = df.withColumn("email", trim(col("email")))
    df = df.withColumn("email", when(col("email").isNotNull() & (col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")), col("email")).otherwise(lit(None)))

    # Cast numeric inventory fields and enforce non-negative
    int_cols = ["product_id", "quantity_on_hand", "reorder_level", "reorder_quantity", "safety_stock_level"]
    for c in int_cols:
        df = df.withColumn(c, col(c).cast(IntegerType()))
        df = df.withColumn(c, when(col(c) >= 0, col(c)).otherwise(lit(None)))

    # Ensure location_is_active is boolean
    df = df.withColumn("location_is_active", col("location_is_active").cast(BooleanType()))

    # Convert dates/timestamps
    df = df.withColumn("last_restock_date", to_date(col("last_restock_date"), "yyyy-MM-dd"))
    df = df.withColumn("last_updated", to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss"))

    # Filter: only active locations
    df = df.filter(col("location_is_active") == True)

    # Compute flag: is_below_reorder
    df = df.withColumn(
        "is_below_reorder",
        when(col("quantity_on_hand").isNotNull() & col("reorder_level").isNotNull() & (col("quantity_on_hand") < col("reorder_level")), lit(True)).otherwise(lit(False))
    )

    return df
