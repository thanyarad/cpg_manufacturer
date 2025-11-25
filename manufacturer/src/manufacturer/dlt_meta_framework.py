from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, expr, trim, regexp_replace, when, length, to_date, lit, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from manufacturer.metadata import METADATA

def get_spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark

def create_landing_pipeline(entity_key: str, catalog: str = "dev", schema: str = "00_landing"):
    entity_meta = METADATA[entity_key]
    entity_name = entity_meta["entity_name"]
    input_path = entity_meta["input_path"]
    entity_schema = entity_meta["schema"]
    primary_key = entity_meta["primary_key"]
    
    if isinstance(primary_key, list):
        keys = primary_key
    else:
        keys = [primary_key]
    
    raw_stream_name = f"{catalog}.{schema}.{entity_name}_raw_stream"
    cdc_stream_name = f"{catalog}.{schema}.{entity_name}_cdc_stream"
    
    dp.create_streaming_table(name=raw_stream_name)
    
    @dp.append_flow(target=raw_stream_name)
    def raw_stream():
        spark = get_spark()
        return (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .schema(entity_schema)
                .load(input_path)
                .withColumn("ingestion_time", current_timestamp())
        )
    
    dp.create_streaming_table(cdc_stream_name)
    dp.create_auto_cdc_flow(
        source=raw_stream_name,
        target=cdc_stream_name,
        name=f"cdc_{entity_name}_flow",
        keys=keys,
        sequence_by=col("ingestion_time"),
        apply_as_deletes=expr("operation = 'delete'"),
        stored_as_scd_type="2"
    )

def create_bronze_pipeline(entity_key: str, catalog: str = "dev", from_schema: str = "00_landing", to_schema: str = "01_bronze"):
    entity_meta = METADATA[entity_key]
    entity_name = entity_meta["entity_name"]
    
    mv_name = f"{catalog}.{to_schema}.{entity_name}_mv"
    source_table = f"{catalog}.{from_schema}.{entity_name}_cdc_stream"
    
    @dp.materialized_view(name=mv_name)
    def bronze_mv():
        spark = get_spark()
        df = spark.read.table(source_table)
        cols = [c for c in df.columns if not c.startswith("__")]
        return df.select(*cols).filter(col("__END_AT").isNull()).drop("operation", "ingestion_time")

def create_silver_pipeline(entity_key: str, catalog: str = "dev", from_schema: str = "01_bronze", to_schema: str = "02_silver"):
    entity_meta = METADATA[entity_key]
    entity_name = entity_meta["entity_name"]
    transformations = entity_meta.get("silver_transformations", {})
    quality_checks = transformations.get("quality_checks", [])
    
    mv_name = f"{catalog}.{to_schema}.{entity_name}_mv"
    source_table = f"{catalog}.{from_schema}.{entity_name}_mv"
    
    decorators = []
    if quality_checks:
        if len(quality_checks) == 1:
            decorators.append(dp.expect_or_drop(f"valid_{entity_name}", quality_checks[0]))
        else:
            checks_dict = {f"valid_{entity_name}_{i}": check for i, check in enumerate(quality_checks)}
            decorators.append(dp.expect_all_or_drop(checks_dict))
    
    def apply_decorators(func):
        for decorator in reversed(decorators):
            func = decorator(func)
        return func
    
    @apply_decorators
    @dp.materialized_view(name=mv_name)
    def silver_mv():
        spark = get_spark()
        df = spark.read.table(source_table)
        
        if "trim_columns" in transformations:
            for col_name in transformations["trim_columns"]:
                if col_name in df.columns:
                    df = df.withColumn(col_name, trim(when(col(col_name).isNull(), lit("NA")).otherwise(col(col_name))))
        
        if "date_columns" in transformations:
            for col_name, date_format in transformations["date_columns"].items():
                if col_name in df.columns:
                    df = df.withColumn(col_name, to_date(col(col_name), date_format))
        
        if "phone_validation" in transformations:
            phone_config = transformations["phone_validation"]
            phone_col = phone_config["column"]
            min_length = phone_config.get("min_length", 10)
            if phone_col in df.columns:
                phone_digits_col = f"{phone_col}_digits"
                df = df.withColumn(phone_digits_col, regexp_replace(col(phone_col), r"[^\d]", ""))
                df = df.withColumn(
                    phone_col,
                    when(length(col(phone_digits_col)) >= min_length, col(phone_digits_col)).otherwise(lit(None))
                ).drop(phone_digits_col)
        
        if "postal_code_validation" in transformations:
            postal_config = transformations["postal_code_validation"]
            postal_col = postal_config["column"]
            expected_length = postal_config.get("length", 6)
            if postal_col in df.columns:
                df = df.withColumn(
                    postal_col,
                    when(
                        length(regexp_replace(col(postal_col), r"\s+", "")) == expected_length,
                        regexp_replace(col(postal_col), r"\s+", "")
                    ).otherwise("Invalid")
                )
        
        if "code_validation" in transformations:
            for code_col, code_config in transformations["code_validation"].items():
                if code_col in df.columns:
                    min_length = code_config.get("min_length", 8)
                    digits_col = f"{code_col}_digits"
                    df = df.withColumn(digits_col, regexp_replace(col(code_col), r"[^\d]", ""))
                    df = df.withColumn(
                        code_col,
                        when(length(col(digits_col)) >= min_length, col(digits_col)).otherwise(lit(None))
                    ).drop(digits_col)
        
        if "numeric_validation" in transformations:
            for num_col, num_config in transformations["numeric_validation"].items():
                if num_col in df.columns:
                    col_type = num_config.get("type", "double")
                    min_val = num_config.get("min", 0)
                    
                    if col_type == "double":
                        df = df.withColumn(num_col, col(num_col).cast(DoubleType()))
                        df = df.withColumn(
                            num_col,
                            when(col(num_col).isNotNull() & (col(num_col) >= min_val), col(num_col)).otherwise(lit(None))
                        )
                    elif col_type == "integer":
                        df = df.withColumn(num_col, col(num_col).cast(IntegerType()))
                        df = df.withColumn(
                            num_col,
                            when(col(num_col).isNotNull() & (col(num_col) >= min_val), col(num_col)).otherwise(lit(None))
                        )
        
        if "email_validation" in transformations and transformations["email_validation"]:
            email_col = transformations.get("email_column", "email")
            if email_col in df.columns:
                df = df.withColumn(email_col, trim(col(email_col)))
                df = df.withColumn(
                    email_col,
                    when(
                        col(email_col).isNotNull() & (col(email_col).rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")),
                        col(email_col)
                    ).otherwise(lit(None))
                )
        
        if "integer_columns" in transformations:
            for int_col in transformations["integer_columns"]:
                if int_col in df.columns:
                    df = df.withColumn(int_col, col(int_col).cast(IntegerType()))
                    df = df.withColumn(int_col, when(col(int_col) >= 0, col(int_col)).otherwise(lit(None)))
        
        if "boolean_columns" in transformations:
            for bool_col in transformations["boolean_columns"]:
                if bool_col in df.columns:
                    df = df.withColumn(bool_col, col(bool_col).cast(BooleanType()))
        
        if "timestamp_columns" in transformations:
            for ts_col, ts_format in transformations["timestamp_columns"].items():
                if ts_col in df.columns:
                    df = df.withColumn(ts_col, to_timestamp(col(ts_col), ts_format))
        
        if "filters" in transformations:
            for filter_expr in transformations["filters"]:
                df = df.filter(expr(filter_expr))
        
        if "computed_columns" in transformations:
            for comp_col, comp_expr in transformations["computed_columns"].items():
                df = df.withColumn(comp_col, expr(comp_expr))
        
        return df
    
    return silver_mv

def create_joined_silver_pipeline(entity_keys: list, join_config: dict, catalog: str = "dev", from_schema: str = "01_bronze", to_schema: str = "02_silver"):
    temp_view_funcs = []
    
    for entity_key in entity_keys:
        entity_meta = METADATA[entity_key]
        entity_name = entity_meta["entity_name"]
        transformations = entity_meta.get("silver_transformations", {})
        quality_checks = transformations.get("quality_checks", [])
        
        source_table = f"{catalog}.{from_schema}.{entity_name}_mv"
        temp_view_name = f"{entity_name}_temp"
        
        decorators = []
        if quality_checks:
            if len(quality_checks) == 1:
                decorators.append(dp.expect_or_drop(f"valid_{entity_name}", quality_checks[0]))
            else:
                checks_dict = {f"valid_{entity_name}_{i}": check for i, check in enumerate(quality_checks)}
                decorators.append(dp.expect_all_or_drop(checks_dict))
        
        def make_temp_view(entity_name, source_table, transformations, decorators):
            def apply_decorators(func):
                for decorator in reversed(decorators):
                    func = decorator(func)
                return func
            
            @apply_decorators
            @dp.temporary_view(name=f"{entity_name}_temp")
            def temp_view():
                spark = get_spark()
                df = spark.read.table(source_table)
                
                if "trim_columns" in transformations:
                    for col_name in transformations["trim_columns"]:
                        if col_name in df.columns:
                            df = df.withColumn(col_name, trim(when(col(col_name).isNull(), lit("NA")).otherwise(col(col_name))))
                
                if "date_columns" in transformations:
                    for col_name, date_format in transformations["date_columns"].items():
                        if col_name in df.columns:
                            df = df.withColumn(col_name, to_date(col(col_name), date_format))
                
                if "numeric_validation" in transformations:
                    for num_col, num_config in transformations["numeric_validation"].items():
                        if num_col in df.columns:
                            col_type = num_config.get("type", "double")
                            min_val = num_config.get("min", 0)
                            
                            if col_type == "double":
                                df = df.withColumn(num_col, col(num_col).cast(DoubleType()))
                                df = df.withColumn(
                                    num_col,
                                    when(col(num_col).isNotNull() & (col(num_col) >= min_val), col(num_col)).otherwise(lit(None))
                                )
                            elif col_type == "integer":
                                df = df.withColumn(num_col, col(num_col).cast(IntegerType()))
                                df = df.withColumn(
                                    num_col,
                                    when(col(num_col).isNotNull() & (col(num_col) >= min_val), col(num_col)).otherwise(lit(None))
                                )
                
                if "filters" in transformations:
                    for filter_expr in transformations["filters"]:
                        df = df.filter(expr(filter_expr))
                
                return df
            
            return temp_view
        
        temp_view_func = make_temp_view(entity_name, source_table, transformations, decorators)
        temp_view_funcs.append(temp_view_func)
    
    mv_name = f"{catalog}.{to_schema}.{join_config['output_entity']}_mv"
    
    @dp.materialized_view(name=mv_name)
    def joined_mv():
        spark = get_spark()
        for temp_view_func in temp_view_funcs:
            temp_view_func()
        joined_df = spark.sql(join_config["join_sql"])
        return joined_df
    
    return joined_mv

