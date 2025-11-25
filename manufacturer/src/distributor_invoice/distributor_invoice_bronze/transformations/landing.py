from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, col, expr

# distributor invoice
# {
#         "invoice_id": 1,
#         "invoice_date": "2024-11-26",
#         "invoice_total_amount": 1975.0,
#         "currency": "USD",
#         "tax_amount": 158.0,
#         "invoice_type": "Standard",
#         "operation": "insert"
# }

distributor_invoice_schema=StructType([
    StructField("invoice_id", IntegerType()),
    StructField("invoice_date", StringType()),
    StructField("invoice_total_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("tax_amount", DoubleType()),
    StructField("invoice_type", StringType()),
    StructField("operation", StringType())
])

input_file_path=r"/Volumes/dev/00_landing/data/distributor_invoice/invoice/"
catalog="dev"
schema="00_landing"

dp.create_streaming_table(name=f"{catalog}.{schema}.distributor_invoice_raw_stream")
@dp.append_flow(target="distributor_invoice_raw_stream")
def distributor_invoice_raw_stream():
  return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(distributor_invoice_schema)
        # .option("multiLine", "true")
        .load(input_file_path)
        .withColumn("ingestion_time", current_timestamp())
  )

dp.create_streaming_table(f"{catalog}.{schema}.distributor_invoice_cdc_stream")
dp.create_auto_cdc_flow(
    source="distributor_invoice_raw_stream",
    target="distributor_invoice_cdc_stream",
    name="cdc_distributor_invoice_flow",  
    keys=["invoice_id"],
    sequence_by=col("ingestion_time"),
    apply_as_deletes=expr("operation = 'delete'"),
    stored_as_scd_type="2"
)