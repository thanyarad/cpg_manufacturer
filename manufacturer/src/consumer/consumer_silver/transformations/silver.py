from manufacturer.dlt_meta_framework import create_silver_pipeline

create_silver_pipeline("consumer", catalog="dev", from_schema="01_bronze", to_schema="02_silver")

