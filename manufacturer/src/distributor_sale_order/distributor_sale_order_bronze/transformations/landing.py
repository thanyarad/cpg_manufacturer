from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, col, expr

distributor_sale_order_schema=StructType([
    StructField("sales_order_id", IntegerType()),
    StructField("distributor_id", IntegerType()),
    StructField("order_date", StringType()),
    StructField("expected_delivery_date", StringType()),
    StructField("order_total_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("operation", StringType())
])

input_file_path=r"/Volumes/dev/00_landing/data/distributor_sales_order/sale_order/"
catalog="dev"
schema="00_landing"

dp.create_streaming_table(name=f"{catalog}.{schema}.distributor_sale_order_raw_stream")
@dp.append_flow(target="distributor_sale_order_raw_stream")
def distributor_sale_order_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(distributor_sale_order_schema)
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(f"{catalog}.{schema}.distributor_sale_order_cdc_stream")
dp.create_auto_cdc_flow(
    source="distributor_sale_order_raw_stream",
    target="distributor_sale_order_cdc_stream",
    name="cdc_distributor_sale_order_flow",  
    keys=["sales_order_id","distributor_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)