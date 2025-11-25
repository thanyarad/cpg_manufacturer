from manufacturer.dlt_meta_framework import create_bronze_pipeline

create_bronze_pipeline("distributor", catalog="dev", from_schema="00_landing", to_schema="01_bronze")