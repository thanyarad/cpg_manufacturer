from manufacturer.dlt_meta_framework import create_bronze_pipeline

create_bronze_pipeline("distributor_invoice_item", catalog="dev", from_schema="00_landing", to_schema="01_bronze")