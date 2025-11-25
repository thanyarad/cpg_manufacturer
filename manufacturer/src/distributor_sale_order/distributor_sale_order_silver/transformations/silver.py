from manufacturer.dlt_meta_framework import create_joined_silver_pipeline
from manufacturer.join_configs import JOIN_CONFIGS

create_joined_silver_pipeline(
    ["distributor_sale_order", "distributor_sale_order_item"],
    JOIN_CONFIGS["distributor_sale_order"],
    catalog="dev",
    from_schema="01_bronze",
    to_schema="02_silver"
)