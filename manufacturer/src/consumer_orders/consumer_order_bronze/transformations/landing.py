from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, col, expr

consumer_order_schema=StructType([
    StructField("order_id", IntegerType()),
    StructField("consumer_id", IntegerType()),
    StructField("order_date", StringType()),
    StructField("order_status", StringType()),
    StructField("total_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("channel", StringType()),
    StructField("billing_address", StringType()),
    StructField("shipping_address", StringType()),
    StructField("operation", StringType())
])

input_file_path=r"/Volumes/dev/00_landing/data/consumer_orders/consumer_order/"
catalog="dev"
schema="00_landing"

dp.create_streaming_table(name=f"{catalog}.{schema}.consumer_order_raw_stream")
@dp.append_flow(target="consumer_order_raw_stream")
def consumer_order_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(consumer_order_schema)
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(f"{catalog}.{schema}.consumer_order_cdc_stream")
dp.create_auto_cdc_flow(
    source="consumer_order_raw_stream",
    target="consumer_order_cdc_stream",
    name="cdc_consumer_order_flow",  
    keys=["order_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)

