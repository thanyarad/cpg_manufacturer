from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, to_date, when
from manufacturer.package.schema import get_schema

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
    string_cols = ["order_status","currency","payment_method","channel","billing_address","shipping_address"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    df = df.withColumn(
        "total_amount",
        when(col("total_amount").isNotNull() & (col("total_amount") >= 0), col("total_amount"))
        .otherwise(None)
    )
    return df

@dp.expect_all_or_drop({
    "valid_order": "order_id IS NOT NULL",
    "valid_order_item": "order_item_id IS NOT NULL"
})
@dp.temporary_view(name="consumer_order_item_temp")
def consumer_order_item_temp():
    df = spark.read.table("consumer_order_items_mv")
    df = df.withColumn(
        "quantity",
        when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))
        .otherwise(None)
    )
    df = df.withColumn(
        "unit_price",
        when(col("unit_price").isNotNull() & (col("unit_price") >= 0), col("unit_price"))
        .otherwise(None)
    )
    df = df.withColumn(
        "total_price",
        when(col("total_price").isNotNull() & (col("total_price") >= 0), col("total_price"))
        .otherwise(None)
    )
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

