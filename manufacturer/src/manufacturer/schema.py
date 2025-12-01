from pyspark.sql.types import StructType
import json
import os

def read_data(entity):
    base_dir = os.path.dirname(__file__)
    with open(os.path.join(base_dir, "input_schema.json")) as f:
        data = json.load(f)
    if entity in data:
        return data[entity]
    else:
        raise RuntimeError(f"Schema for entity {entity} not found")

def get_schema(entity):
    schema=read_data(entity)
    schema=StructType.fromDDL(",".join([f"{col[0]} {col[1]}" for col in schema]))
    return schema

# get_schema("consumer")