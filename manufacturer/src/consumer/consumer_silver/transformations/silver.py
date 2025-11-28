from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim, to_date

# catalog="dev"
# from_schema="01_bronze"
# to_schema="02_silver"
catalog = spark.conf.get("catalog", "dev")
schema = spark.conf.get("target_schema", "02_silver")

@dp.materialized_view(name=f"{catalog}.{schema}.consumer_mv")
@dp.expect_or_drop("valid_consumer", "consumer_id IS NOT NULL")
def consumer_mv():
    df = spark.read.table("consumer_mv")
    string_cols = ["name","gender","email","phone","address","city","state","country"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    df = df.withColumn(
        "phone",
        when(
            length(regexp_replace(col("phone"), r"[^\d]", "")) >= 10,
            regexp_replace(col("phone"), r"[^\d]", "")
        ).otherwise("Invalid")
    )
    df = df.withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))
    return df

