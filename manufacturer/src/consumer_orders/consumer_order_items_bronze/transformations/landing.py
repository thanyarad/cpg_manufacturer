from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, col, expr

consumer_order_items_schema=StructType([
    StructField("order_item_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_price", DoubleType()),
    StructField("operation", StringType())
])

# input_file_path=r"/Volumes/dev/00_landing/data/consumer_orders/consumer_order_items/"
# catalog="dev"
# schema="00_landing"
catalog_config=spark.conf.get("catalog")
schema_config=spark.conf.get("pipeline_schema")
volume_config=spark.conf.get("volume")

input_file_path=f"/Volumes/{catalog_config}/{schema_config}/{volume_config}/consumer_orders/consumer_order_items/"

dp.create_streaming_table(name="consumer_order_items_raw_stream")
@dp.append_flow(target="consumer_order_items_raw_stream")
def consumer_order_items_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(consumer_order_items_schema)
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table("consumer_order_items_cdc_stream")
dp.create_auto_cdc_flow(
    source="consumer_order_items_raw_stream",
    target="consumer_order_items_cdc_stream",
    name="cdc_consumer_order_items_flow",  
    keys=["order_item_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)

