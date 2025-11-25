from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim, to_date, lit

catalog="dev"
from_schema="01_bronze"
to_schema="02_silver"

@dp.expect_all_or_drop({
    "valid_distributor": "distributor_id IS NOT NULL",
    "valid_sales_order" : "sales_order_id IS NOT NULL"
})
@dp.temporary_view(name=f"sale_order_temp")
def sale_order_temp():
    df=spark.read.table(f"{catalog}.{from_schema}.distributor_sale_order_mv")
    df=df.withColumn("order_date",to_date(col("order_date"),"yyyy-MM-dd"))
    df=df.withColumn("expected_delivery_date",to_date(col("expected_delivery_date"),"yyyy-MM-dd"))
    return df

@dp.expect_all_or_drop({
    "valid_sales_order" : "sales_order_id is NOT NULL",
    "valid_sales_order_item": "sales_order_item_no is NOT NULL"
})
@dp.temporary_view(name=f"sale_order_item_temp")
def sale_order_item_temp():
    df=spark.read.table(f"{catalog}.{from_schema}.distributor_sale_order_item_mv")
    df=df.filter(col("order_quantity") >= 1)
    df=df.withColumn(
        "order_quantity_uom",
        trim(when(col("order_quantity_uom").isNull(), lit("NA")).otherwise(col("order_quantity_uom")))
    )
    df=df.withColumn(
        "item_total_amount",
        when(
            col("order_quantity") * col("unit_price") == col("item_total_amount"),
            col("item_total_amount")
        ).otherwise(col("order_quantity") * col("unit_price"))
    )
    return df

@dp.materialized_view(name=f"{catalog}.{to_schema}.distributor_sale_order_mv")
def distributor_mv():
    sale_order_temp()
    sale_order_item_temp()
    df_joined=spark.sql(f""" 
        select 
        so.sales_order_id as order_id,
        so.distributor_id, 
        so.order_date, 
        so.expected_delivery_date,  
        so.currency, 
        soi.sales_order_item_no as item_no, 
        soi.product_id, 
        soi.order_quantity, 
        soi.order_quantity_uom as unit_measure, 
        soi.unit_price, 
        soi.item_total_amount
        from sale_order_item_temp soi
        LEFT JOIN sale_order_temp so
        ON so.sales_order_id = soi.sales_order_id
    """)
    return df_joined

# sale order
# {
#         "sales_order_id": 1,
#         "distributor_id": 1,
#         "order_date": "2024-11-09",
#         "expected_delivery_date": "2024-11-25",
#         "order_total_amount": 1975.0,
#         "currency": "USD",
#         "operation": "insert"
# }
# sale order item
# "sales_order_id": 1,
#         "sales_order_item_no": 1,
#         "product_id": 1,
#         "order_quantity": 500,
#         "order_quantity_uom": "units",
#         "unit_price": 1.89,
#         "item_total_amount": 945.0,
#         "operation": "insert"