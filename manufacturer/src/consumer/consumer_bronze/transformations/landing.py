from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import current_timestamp, col, expr

consumer_schema=StructType([
    StructField("consumer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("country", StringType()),
    StructField("registration_date", StringType()),
    StructField("is_active", BooleanType()),
    StructField("operation", StringType())
])

input_file_path=r"/Volumes/dev/00_landing/data/consumer/"
catalog="dev"
schema="00_landing"

dp.create_streaming_table(name=f"{catalog}.{schema}.consumer_raw_stream")
@dp.append_flow(target="consumer_raw_stream")
def consumer_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(consumer_schema)
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(f"{catalog}.{schema}.consumer_cdc_stream")
dp.create_auto_cdc_flow(
    source="consumer_raw_stream",
    target="consumer_cdc_stream",
    name="cdc_consumer_flow",  
    keys=["consumer_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)

