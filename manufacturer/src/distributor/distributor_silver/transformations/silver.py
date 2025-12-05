from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, when, length, trim
from manufacturer.package.schema import get_schema
from manufacturer.utils.transform_utils import trim_string_columns, clean_postal_code, normalize_phone

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
    # Clean string columns
    df = trim_string_columns(
        df,
        ["distributor_name", "city", "state", "country"]
    )

    # Clean postal code 
    df = clean_postal_code(df, "postal_code")

    # Clean phone number
    df = normalize_phone(df, "phone_number")

    return df