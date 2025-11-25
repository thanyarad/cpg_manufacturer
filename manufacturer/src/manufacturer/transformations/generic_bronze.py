import sys
from pathlib import Path
from manufacturer.transformations.bronze_template import create_bronze_pipeline

entity_name = Path(__file__).parent.parent.parent.name
if "_" in entity_name:
    parts = entity_name.split("_")
    if parts[-1] in ["bronze", "silver"]:
        entity_name = "_".join(parts[:-1])

create_bronze_pipeline(entity_name)

