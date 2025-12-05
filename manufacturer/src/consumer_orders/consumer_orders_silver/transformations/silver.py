from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, to_date, when
from manufacturer.package.schema import get_schema
from manufacturer.utils.transform_utils import (
    trim_columns,
    convert_order_date,
    validate_non_negative,
    validate_positive_integer
)

# catalog="dev"
# from_schema="01_bronze"
# to_schema="02_silver"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")
# metadata_config=spark.conf.get("metadata_path")

# schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
# consumer_orders_schema=get_schema("consumer_orders",schema_path)

@dp.expect_all_or_drop({
    "valid_order": "order_id IS NOT NULL",
    "valid_consumer": "consumer_id IS NOT NULL"
})
@dp.temporary_view(name="consumer_order_temp")
def consumer_order_temp():
    df = spark.read.table("consumer_order_mv")
    # Trim columns
    df = trim_columns(df, [
        "order_status", "currency", "payment_method",
        "channel", "billing_address", "shipping_address"
    ])

    # Convert order_date
    df = convert_order_date(df, "order_date")

    # Validate total_amount
    df = validate_non_negative(df, ["total_amount"])

    return df

@dp.expect_all_or_drop({
    "valid_order": "order_id IS NOT NULL",
    "valid_order_item": "order_item_id IS NOT NULL"
})
@dp.temporary_view(name="consumer_order_item_temp")
def consumer_order_item_temp():
    df = spark.read.table("consumer_order_items_mv")
     df = validate_positive_integer(df, "quantity")
    df = validate_non_negative(df, ["unit_price", "total_price"])
    return df

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.consumer_orders_mv")
def consumer_orders_silver_mv():
    consumer_order_temp()
    consumer_order_item_temp()
    joined_df = spark.sql(f"""
        SELECT 
            i.order_item_id,
            i.order_id,
            i.product_id,
            i.quantity,
            i.unit_price,
            i.total_price,
            o.consumer_id,
            o.order_date,
            o.order_status,
            o.total_amount AS order_total_amount,
            o.currency,
            o.payment_method,
            o.channel,
            o.billing_address,
            o.shipping_address
        FROM consumer_order_item_temp i
        LEFT JOIN consumer_order_temp o
        ON i.order_id = o.order_id
    """)
    return joined_df

