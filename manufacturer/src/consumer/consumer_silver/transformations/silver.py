from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim, to_date
from manufacturer.package.schema import get_schema
from manufacturer.utils.transform_utils import (
    trim_columns,
    normalize_phone,
    validate_email,
    convert_date
)
# catalog="dev"
# from_schema="01_bronze"
# to_schema="02_silver"
catalog_config = spark.conf.get("catalog")
schema_config = spark.conf.get("target_schema")
metadata_config=spark.conf.get("metadata_path")

schema_path=f"/Volumes/{catalog_config}/{schema_config}/{metadata_config}"
consumer_schema=get_schema("consumer",schema_path)

@dp.materialized_view(name=f"{catalog_config}.{schema_config}.consumer_mv")
@dp.expect_or_drop("valid_consumer", "consumer_id IS NOT NULL")
def consumer_mv():
    df = spark.read.table("consumer_mv")
    # Trim string columns
    df = trim_columns(df, ["name", "gender", "email", "phone", "address", "city", "state", "country"])

    # Validate email
    df = validate_email(df, "email")

    # Normalize & validate phone
    df = normalize_phone(df, "phone")

    # Convert registration_date
    df = convert_date(df, "registration_date")

    return df

