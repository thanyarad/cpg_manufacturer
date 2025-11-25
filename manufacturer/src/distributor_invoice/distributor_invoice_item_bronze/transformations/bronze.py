from pyspark import pipelines as dp
from pyspark.sql.functions import col

catalog="dev"
from_schema="00_landing"
to_schema="01_bronze"

@dp.materialized_view(name=f"{catalog}.{to_schema}.distributor_invoice_item_mv")
def distributor_mv():
    df = spark.read.table(f"{catalog}.{from_schema}.distributor_invoice_item_cdc_stream")
    cols = [c for c in df.columns if not c.startswith("__")]
    return df.select(*cols).filter(col("__END_AT").isNull()).drop("operation", "ingestion_time")