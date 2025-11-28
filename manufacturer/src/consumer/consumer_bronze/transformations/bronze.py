from pyspark import pipelines as dp
from pyspark.sql.functions import col

# catalog="dev"
# from_schema="00_landing"
# to_schema="01_bronze"
catalog = spark.conf.get("catalog", "dev")
schema = spark.conf.get("target_schema", "01_bronze")

@dp.materialized_view(name=f"{catalog}.{schema}.consumer_mv")
def consumer_mv():
    df = spark.read.table("consumer_cdc_stream")
    cols = [c for c in df.columns if not c.startswith("__")]
    return df.select(*cols).filter(col("__END_AT").isNull()).drop("operation", "ingestion_time")

