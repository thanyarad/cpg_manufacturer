from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, col, expr
from manufacturer.package.schema import get_schema

# distributor_sale_order_item_schema=StructType([
#     StructField("sales_order_id", IntegerType()),
#     StructField("sales_order_item_no", IntegerType()),
#     StructField("product_id", IntegerType()),
#     StructField("order_quantity", IntegerType()),
#     StructField("order_quantity_uom", StringType()),
#     StructField("unit_price", DoubleType()),
#     StructField("item_total_amount", DoubleType()),
#     StructField("operation", StringType())
# ])

# input_file_path=r"/Volumes/dev/00_landing/data/distributor_sales_order/sale_order_item/"
# catalog="dev"
# schema="00_landing"
catalog_config=spark.conf.get("catalog")
schema_config=spark.conf.get("pipeline_schema")
volume_config=spark.conf.get("volume")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
distributor_sale_order_item_schema=get_schema("distributor_sale_order_item",schema_path)

input_file_path=f"/Volumes/{catalog_config}/{schema_config}/{volume_config}/distributor_sale_order/sale_order_item/"


dp.create_streaming_table(name="distributor_sale_order_item_raw_stream")
@dp.append_flow(target="distributor_sale_order_item_raw_stream")
def distributor_sale_order_item_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(distributor_sale_order_item_schema)
        # .option("multiLine", "true")
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(name="distributor_sale_order_item_cdc_stream")
dp.create_auto_cdc_flow(
    source="distributor_sale_order_item_raw_stream",
    target="distributor_sale_order_item_cdc_stream",
    name="cdc_distributor_sale_order_item_flow",  
    keys=["sales_order_id","sales_order_item_no","product_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)