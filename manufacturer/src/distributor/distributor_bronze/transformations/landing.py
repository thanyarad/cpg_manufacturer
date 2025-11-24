from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, col, expr

distributor_schema=StructType([
    StructField("distributor_id", IntegerType()),
    StructField("distributor_name", StringType()),
    StructField("phone_number", StringType()),
    StructField("street_address", StringType()),
    StructField("postal_code", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("operation", StringType())
])

input_file_path=r"/Volumes/dev/00_landing/data/distributor/"
catalog="dev"
schema="00_landing"

dp.create_streaming_table(name=f"{catalog}.{schema}.distributor_raw_stream")
@dp.append_flow(target="distributor_raw_stream")
def distributor_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(distributor_schema)
        # .option("multiLine", "true")
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(f"{catalog}.{schema}.distributor_cdc_stream")
dp.create_auto_cdc_flow(
    source="distributor_raw_stream",
    target="distributor_cdc_stream",
    name="cdc_distributor_flow",  
    keys=["distributor_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)