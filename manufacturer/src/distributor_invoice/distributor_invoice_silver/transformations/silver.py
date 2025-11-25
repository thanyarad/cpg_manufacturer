from manufacturer.dlt_meta_framework import create_joined_silver_pipeline
from manufacturer.join_configs import JOIN_CONFIGS

create_joined_silver_pipeline(
    ["distributor_invoice", "distributor_invoice_item"],
    JOIN_CONFIGS["distributor_invoice"],
    catalog="dev",
    from_schema="01_bronze",
    to_schema="02_silver"
)