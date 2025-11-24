from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim

catalog="dev"
from_schema="01_bronze"
to_schema="02_silver"

@dp.materialized_view(name=f"{catalog}.{to_schema}.distributor_mv")
@dp.expect_or_drop("valid_distributor", "distributor_id IS NOT NULL")
def distributor_mv():
    df = spark.read.table(f"{catalog}.{from_schema}.distributor_mv")
    string_cols = ["distributor_name","city","country"]
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