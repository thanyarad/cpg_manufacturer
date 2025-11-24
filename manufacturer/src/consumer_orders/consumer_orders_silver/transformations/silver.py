from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, to_date, when

catalog="dev"
from_schema="01_bronze"
to_schema="02_silver"

@dp.materialized_view(name=f"{catalog}.{to_schema}.consumer_orders_silver_mv")
@dp.expect_or_drop("valid_order", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_order_item", "order_item_id IS NOT NULL")
def consumer_orders_silver_mv():
    orders_df = spark.read.table(f"{catalog}.{from_schema}.consumer_order_mv")
    items_df = spark.read.table(f"{catalog}.{from_schema}.consumer_order_items_mv")
    
    string_cols = ["order_status","currency","payment_method","channel","billing_address","shipping_address"]
    for c in string_cols:
        orders_df = orders_df.withColumn(c, trim(col(c)))
    
    orders_df = orders_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    orders_df = orders_df.withColumn(
        "total_amount",
        when(col("total_amount").isNotNull() & (col("total_amount") >= 0), col("total_amount"))
        .otherwise(None)
    )
    
    items_df = items_df.withColumn(
        "quantity",
        when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))
        .otherwise(None)
    )
    items_df = items_df.withColumn(
        "unit_price",
        when(col("unit_price").isNotNull() & (col("unit_price") >= 0), col("unit_price"))
        .otherwise(None)
    )
    items_df = items_df.withColumn(
        "total_price",
        when(col("total_price").isNotNull() & (col("total_price") >= 0), col("total_price"))
        .otherwise(None)
    )
    
    joined_df = items_df.join(
        orders_df,
        items_df["order_id"] == orders_df["order_id"],
        "inner"
    ).select(
        items_df["order_item_id"],
        items_df["order_id"],
        items_df["product_id"],
        items_df["quantity"],
        items_df["unit_price"],
        items_df["total_price"],
        orders_df["consumer_id"],
        orders_df["order_date"],
        orders_df["order_status"],
        orders_df["total_amount"].alias("order_total_amount"),
        orders_df["currency"],
        orders_df["payment_method"],
        orders_df["channel"],
        orders_df["billing_address"],
        orders_df["shipping_address"]
    )
    
    return joined_df

