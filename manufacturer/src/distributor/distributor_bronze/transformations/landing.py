from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, col, expr
from manufacturer.package.schema import get_schema

# distributor_schema=StructType([
#     StructField("distributor_id", IntegerType()),
#     StructField("distributor_name", StringType()),
#     StructField("phone_number", StringType()),
#     StructField("street_address", StringType()),
#     StructField("postal_code", StringType()),
#     StructField("city", StringType()),
#     StructField("state", StringType()),
#     StructField("country", StringType()),
#     StructField("operation", StringType())
# ])
catalog_config=spark.conf.get("catalog")
schema_config=spark.conf.get("pipeline_schema")
volume_config=spark.conf.get("volume")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
distributor_schema=get_schema("distributor",schema_path)

input_file_path=f"/Volumes/{catalog_config}/{schema_config}/{volume_config}/distributor/"
# catalog="dev"
# schema="00_landing"

dp.create_streaming_table(name="distributor_raw_stream")
@dp.append_flow(target="distributor_raw_stream")
def distributor_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(distributor_schema)
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(name="distributor_cdc_stream")
dp.create_auto_cdc_flow(
    source="distributor_raw_stream",
    target="distributor_cdc_stream",
    name="cdc_distributor_flow",  
    keys=["distributor_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)