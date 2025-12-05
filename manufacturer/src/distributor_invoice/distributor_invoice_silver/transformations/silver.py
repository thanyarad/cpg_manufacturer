from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, trim, to_date, lit
from manufacturer.package.schema import get_schema
from manufacturer.utils.transform_utils import convert_to_date, validate_positive, validate_non_negative
# catalog="dev"
# from_schema="01_bronze"
# to_schema="02_silver"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")
# metadata_config=spark.conf.get("metadata_path")

# schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
# distributor_invoice_schema=get_schema("distributor_invoice",schema_path)

@dp.expect_or_drop("valid_invoice","invoice_id IS NOT NULL")
@dp.expect_or_drop("positive_amounts", "invoice_total_amount > 0 AND tax_amount >= 0")
@dp.temporary_view(name="invoice_temp")
def invoice_temp():
    df=spark.read.table("distributor_invoice_mv")
    
    # Reusable date validation
    df = convert_to_date(df, "invoice_date")


    # Amount validations
    df = validate_positive(df, ["invoice_total_amount"])
    df = validate_non_negative(df, ["tax_amount"])

@dp.expect_all_or_drop({
    "valid_invoice" : "invoice_id is NOT NULL",
    "valid_invoice_item": "invoice_item_no is NOT NULL",
    "valid_product" : "product_id is not null",
    "valid_sales_order" : "sales_order_id is not null",
    "valid_sales_order_item" : "sales_order_item_no is not null"
})
@dp.temporary_view(name=f"invoice_item_temp")
def invoice_item_temp():
    df=spark.read.table("distributor_invoice_item_mv")
    # Validate item amount
    df = validate_positive_amount(df, ["invoice_item_total_amount"])
    df=df.withColumn("invoiced_quantity_uom", trim(when(col("invoiced_quantity_uom").isNull(), lit("NA")).otherwise(col("invoiced_quantity_uom"))))
    return df

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.distributor_invoice_mv")
def distributor_invoice_mv():
    invoice_temp()
    invoice_item_temp()
    df_joined=spark.sql(f""" 
        select 
        iit.invoice_id,
        iit.invoice_item_no,
        iit.product_id,
        iit.invoiced_quantity,
        iit.invoiced_quantity_uom as primary_unit,
        iit.invoice_item_total_amount,
        iit.sales_order_id,
        iit.sales_order_item_no,
        it.invoice_date,
        it.currency,
        -- it.tax_amount,
        it.invoice_type
        from invoice_item_temp iit
        LEFT JOIN invoice_temp it
        ON iit.invoice_id=it.invoice_id
    """)
    return df_joined