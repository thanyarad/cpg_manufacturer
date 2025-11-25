from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, regexp_replace, when, length, trim, to_date, to_timestamp, lit, lower
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from manufacturer.metadata_loader import MetadataLoader

def create_silver_pipeline(entity_name: str):
    loader = MetadataLoader()
    catalog = loader.get_catalog()
    from_schema = loader.get_schema_name("bronze")
    to_schema = loader.get_schema_name("silver")
    transformations = loader.get_silver_transformations(entity_name)
    
    mv_name = f"{catalog}.{to_schema}.{entity_name}_mv"
    source_table = f"{catalog}.{from_schema}.{entity_name}_mv"
    
    expectations = transformations.get("expectations", [])
    
    @dp.materialized_view(name=mv_name)
    def silver_mv():
        df = spark.read.table(source_table)
        
        if transformations.get("trim_columns"):
            for col_name in transformations["trim_columns"]:
                if col_name in df.columns:
                    df = df.withColumn(col_name, trim(col(col_name)))
        
        if transformations.get("normalize_phone"):
            phone_cols = [c for c in df.columns if "phone" in c.lower()]
            for col_name in phone_cols:
                df = df.withColumn(f"{col_name}_digits", regexp_replace(col(col_name), r"[^\d]", ""))
                df = df.withColumn(col_name, when(length(col(f"{col_name}_digits")) >= 7, col(f"{col_name}_digits")).otherwise(lit(None))).drop(f"{col_name}_digits")
        
        if transformations.get("normalize_email"):
            email_cols = [c for c in df.columns if "email" in c.lower()]
            for col_name in email_cols:
                df = df.withColumn(col_name, trim(col(col_name)))
                df = df.withColumn(col_name, when(
                    col(col_name).isNotNull() & col(col_name).rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
                    col(col_name)
                ).otherwise(lit(None)))
        
        if transformations.get("normalize_upc_gtin"):
            for col_name in ["upc", "gtin"]:
                if col_name in df.columns:
                    df = df.withColumn(f"{col_name}_digits", regexp_replace(col(col_name), r"[^\d]", ""))
                    df = df.withColumn(col_name, when(length(col(f"{col_name}_digits")) >= 8, col(f"{col_name}_digits")).otherwise(lit(None))).drop(f"{col_name}_digits")
        
        if transformations.get("cast_numeric"):
            for col_name in transformations["cast_numeric"]:
                if col_name in df.columns:
                    if any(x in col_name.lower() for x in ["price", "amount", "total"]):
                        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
                        df = df.withColumn(col_name, when(col(col_name) >= 0, col(col_name)).otherwise(lit(None)))
                    else:
                        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
                        df = df.withColumn(col_name, when(col(col_name) >= 0, col(col_name)).otherwise(lit(None)))
        
        if transformations.get("date_columns"):
            for date_col in transformations["date_columns"]:
                col_name = date_col["name"]
                date_format = date_col.get("format", "yyyy-MM-dd")
                if col_name in df.columns:
                    if "HH:mm:ss" in date_format or "HHmmss" in date_format:
                        df = df.withColumn(col_name, to_timestamp(col(col_name), date_format))
                    else:
                        df = df.withColumn(col_name, to_date(col(col_name), date_format))
        
        boolean_cols = [c for c in df.columns if "is_active" in c.lower() or "is_" in c.lower()]
        for col_name in boolean_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(BooleanType()))
        
        return df
    
    for i, expectation in enumerate(expectations):
        silver_mv = dp.expect_or_drop(f"valid_{entity_name}_{i}", expectation)(silver_mv)

