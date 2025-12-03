from pyspark import pipelines as dp
from pyspark.sql.functions import col
from manufacturer.package.schema import get_schema

# catalog="dev"
# from_schema="00_landing"
# to_schema="01_bronze"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
product_schema=get_schema("product",schema_path)

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.product_mv")
def product_mv():
    df = spark.read.table("product_cdc_stream")
    cols = [c for c in df.columns if not c.startswith("__")]
    return df.select(*cols).filter(col("__END_AT").isNull()).drop("operation", "ingestion_time")
