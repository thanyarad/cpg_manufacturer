from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, trim, to_date, lit
from manufacturer.package.schema import get_schema
from manufacturer.utils.transform_utils import (
    convert_to_date,
    trim_string_columns,
    validate_positive,
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
# distributor_sale_order_schema=get_schema("distributor_sale_order",schema_path)

@dp.expect_all_or_drop({
    "valid_distributor": "distributor_id IS NOT NULL",
    "valid_sales_order" : "sales_order_id IS NOT NULL"
})
@dp.temporary_view(name="sale_order_temp")
def sale_order_temp():
    df=spark.read.table("distributor_sale_order_mv")
    # reusable: convert date columns
    df = convert_to_date(df, "order_date")
    df = convert_to_date(df, "expected_delivery_date")

    # trim string columns
    df = trim_string_columns(df, ["currency"])

    return df

@dp.expect_all_or_drop({
    "valid_sales_order" : "sales_order_id is NOT NULL",
    "valid_sales_order_item": "sales_order_item_no is NOT NULL"
})
@dp.temporary_view(name="sale_order_item_temp")
def sale_order_item_temp():
    df=spark.read.table("distributor_sale_order_item_mv")
    # validate quantity > 0
    df = validate_positive_integer(df, "order_quantity")

    # validate unit price positive
    df = validate_positive(df, ["unit_price"])

    # validate amount non negative
    df = validate_non_negative(df, ["item_total_amount"])

    # default trim on UOM
    df = trim_string_columns(df, ["order_quantity_uom"])
    df=df.withColumn(
        "item_total_amount",
        when(
            col("order_quantity") * col("unit_price") == col("item_total_amount"),
            col("item_total_amount")
        ).otherwise(col("order_quantity") * col("unit_price"))
    )
    return df

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.distributor_sale_order_mv")
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