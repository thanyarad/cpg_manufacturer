from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim
from manufacturer.package.schema import get_schema

# catalog="dev"
# from_schema="01_bronze"
# to_schema="02_silver"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
distributor_schema=get_schema("distributor",schema_path)

@dp.materialized_view(name=f"{catalog}.{to_schema}.distributor_mv")
@dp.expect_or_drop("valid_distributor", "distributor_id IS NOT NULL")
def distributor_mv():
    df = spark.read.table(f"{catalog}.{from_schema}.distributor_mv")
    string_cols = ["distributor_name","city","state","country"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    df = df.withColumn(
        "postal_code",
        when(
            length(regexp_replace(col("postal_code"), r"\s+", "")) == 6,
            regexp_replace(col("postal_code"), r"\s+", "")
        ).otherwise("Invalid")
    )
    return df