from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import current_timestamp, col, expr

inventory_schema = StructType([
  StructField("inventory_id", IntegerType()),
  StructField("product_id", IntegerType()),
  StructField("location_type", StringType()),
  StructField("location_name", StringType()),
  StructField("location_code", StringType()),
  StructField("address", StringType()),
  StructField("city", StringType()),
  StructField("state", StringType()),
  StructField("country", StringType()),
  StructField("phone", StringType()),
  StructField("email", StringType()),
  StructField("location_is_active", BooleanType()),
  StructField("quantity_on_hand", IntegerType()),
  StructField("reorder_level", IntegerType()),
  StructField("reorder_quantity", IntegerType()),
  StructField("safety_stock_level", IntegerType()),
  StructField("inventory_status", StringType()),
  StructField("last_restock_date", StringType()),
  StructField("last_updated", StringType()),
  StructField("operation", StringType())
])

input_file_path = r"/Volumes/dev/00_landing/data/inventory/"
catalog="dev"
schema="00_landing"

dp.create_streaming_table(name=f"{catalog}.{schema}.inventory_raw_stream")
@dp.append_flow(target="inventory_raw_stream")
def inventory_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(inventory_schema)
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(f"{catalog}.{schema}.inventory_cdc_stream")
dp.create_auto_cdc_flow(
    source="inventory_raw_stream",
    target="inventory_cdc_stream",
    name="cdc_inventory_flow",  
    keys=["inventory_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)
