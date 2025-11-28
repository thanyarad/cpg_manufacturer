from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, col, expr

# distributor invoice item
# {
# "invoice_id": 1,
# "invoice_item_no": 1,
# "product_id": 1,
# "invoiced_quantity": 500,
# "invoiced_quantity_uom": "units",
# "invoice_item_total_amount": 945.0,
# "sales_order_id": 1,
# "sales_order_item_no": 1,
# "operation": "insert"
# }

distributor_invoice_item_schema=StructType([
    StructField("invoice_id", IntegerType()),
    StructField("invoice_item_no", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("invoiced_quantity", IntegerType()),
    StructField("invoiced_quantity_uom", StringType()),
    StructField("invoice_item_total_amount", DoubleType()),
    StructField("sales_order_id", IntegerType()),
    StructField("sales_order_item_no", IntegerType()),
    StructField("operation", StringType())
])

# input_file_path=r"/Volumes/dev/00_landing/data/distributor_invoice/invoice_item/"
# catalog="dev"
# schema="00_landing"
catalog_config=spark.conf.get("catalog")
schema_config=spark.conf.get("pipeline_schema")
volume_config=spark.conf.get("volume")

input_file_path=f"/Volumes/{catalog_config}/{schema_config}/{volume_config}/distributor_invoice/invoice_item/"

dp.create_streaming_table("distributor_invoice_item_raw_stream")
@dp.append_flow(target="distributor_invoice_item_raw_stream")
def distributor_invoice_item_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(distributor_invoice_item_schema)
        # .option("multiLine", "true")
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table("distributor_invoice_item_cdc_stream")
dp.create_auto_cdc_flow(
    source="distributor_invoice_item_raw_stream",
    target="distributor_invoice_item_cdc_stream",
    name="cdc_distributor_invoice_item_flow",  
    keys=["invoice_id","invoice_item_no","product_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)