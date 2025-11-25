import sys
from pathlib import Path
from manufacturer.transformations.silver_template import create_silver_pipeline

entity_name = Path(__file__).parent.parent.parent.name
if "_" in entity_name:
    parts = entity_name.split("_")
    if parts[-1] in ["bronze", "silver"]:
        entity_name = "_".join(parts[:-1])

create_silver_pipeline(entity_name)

