from pyspark import pipelines as dp
from pyspark.sql.functions import col

# catalog="dev"
# from_schema="00_landing"
# to_schema="01_bronze"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.distributor_sale_order_mv")
def distributor_mv():
    df = spark.read.table("distributor_sale_order_cdc_stream")
    cols = [c for c in df.columns if not c.startswith("__")]
    return df.select(*cols).filter(col("__END_AT").isNull()).drop("operation", "ingestion_time")