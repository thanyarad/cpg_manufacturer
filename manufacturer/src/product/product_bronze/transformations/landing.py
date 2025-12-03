from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from pyspark.sql.functions import current_timestamp, col, expr
from manufacturer.package.schema import get_schema

# product_schema = StructType([
#   StructField("product_id", IntegerType()),
#   StructField("product_name", StringType()),
#   StructField("brand", StringType()),
#   StructField("manufacturer", StringType()),
#   StructField("category", StringType()),
#   StructField("department", StringType()),
#   StructField("description", StringType()),
#   StructField("sku_id", StringType()),
#   StructField("upc", StringType()),
#   StructField("gtin", StringType()),
#   StructField("unit_price", DoubleType()),
#   StructField("retail_price", DoubleType()),
#   StructField("unit_of_measurement", StringType()),
#   StructField("expiration_days", IntegerType()),
#   StructField("product_status", StringType()),
#   StructField("release_date", StringType()),
#   StructField("operation", StringType())
# ])

# input_file_path=r"/Volumes/dev/00_landing/data/product/"
# catalog="dev"
# schema="00_landing"
catalog_config=spark.conf.get("catalog")
schema_config=spark.conf.get("pipeline_schema")
volume_config=spark.conf.get("volume")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
product_schema=get_schema("product",schema_path)

input_file_path=f"/Volumes/{catalog_config}/{schema_config}/{volume_config}/product/"


dp.create_streaming_table(name="product_raw_stream")
@dp.append_flow(target="product_raw_stream")
def product_raw_stream():
  return (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(product_schema)
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(name="product_cdc_stream")
dp.create_auto_cdc_flow(
    source="product_raw_stream",
    target="product_cdc_stream",
    name="cdc_product_flow",  
    keys=["product_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)
