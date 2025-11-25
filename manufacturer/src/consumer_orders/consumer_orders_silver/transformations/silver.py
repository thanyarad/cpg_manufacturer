from manufacturer.dlt_meta_framework import create_joined_silver_pipeline
from manufacturer.join_configs import JOIN_CONFIGS

create_joined_silver_pipeline(
    ["consumer_order", "consumer_order_items"],
    JOIN_CONFIGS["consumer_orders"],
    catalog="dev",
    from_schema="01_bronze",
    to_schema="02_silver"
)

