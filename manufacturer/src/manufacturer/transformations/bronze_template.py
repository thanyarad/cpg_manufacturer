from pyspark import pipelines as dp
from pyspark.sql.functions import col
from manufacturer.metadata_loader import MetadataLoader

def create_bronze_pipeline(entity_name: str):
    loader = MetadataLoader()
    catalog = loader.get_catalog()
    from_schema = loader.get_schema_name("landing")
    to_schema = loader.get_schema_name("bronze")
    
    mv_name = f"{catalog}.{to_schema}.{entity_name}_mv"
    source_table = f"{catalog}.{from_schema}.{entity_name}_cdc_stream"
    
    @dp.materialized_view(name=mv_name)
    def bronze_mv():
        df = spark.read.table(source_table)
        cols = [c for c in df.columns if not c.startswith("__")]
        return df.select(*cols).filter(col("__END_AT").isNull()).drop("operation", "ingestion_time")

