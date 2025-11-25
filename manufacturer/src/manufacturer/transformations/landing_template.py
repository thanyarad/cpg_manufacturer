from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col, expr
from manufacturer.metadata_loader import MetadataLoader

def create_landing_pipeline(entity_name: str):
    loader = MetadataLoader()
    entity = loader.get_entity(entity_name)
    catalog = loader.get_catalog()
    schema = loader.get_schema_name("landing")
    input_path = loader.get_input_path(entity_name)
    primary_key = loader.get_primary_key(entity_name)
    schema_struct = loader.build_schema(entity_name)
    
    raw_stream_name = f"{catalog}.{schema}.{entity_name}_raw_stream"
    cdc_stream_name = f"{catalog}.{schema}.{entity_name}_cdc_stream"
    
    dp.create_streaming_table(name=raw_stream_name)
    
    @dp.append_flow(target=raw_stream_name)
    def raw_stream():
        return (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .schema(schema_struct)
                .load(input_path)
                .withColumn("ingestion_time", current_timestamp())
        )
    
    dp.create_streaming_table(cdc_stream_name)
    dp.create_auto_cdc_flow(
        source=raw_stream_name,
        target=cdc_stream_name,
        name=f"cdc_{entity_name}_flow",
        keys=primary_key,
        sequence_by=col("ingestion_time"),
        apply_as_deletes=expr("operation = 'delete'"),
        stored_as_scd_type="2"
    )

