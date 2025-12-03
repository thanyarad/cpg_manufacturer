from pyspark.sql.types import StructType
import json

def get_schema(entity,path):
    data=json.load(open(path))
    
    if entity in data:
        schema=data[entity]
        schema=StructType.fromDDL(",".join([f"{col[0]} {col[1]}" for col in schema]))
        return schema
    else:
        raise RuntimeError(f"Schema for entity {entity} not found")

# testing
if __name__ == "__main__":
    path="/Volumes/dev/00_landing/meta_data/input_schema.json"
    print(get_schema("consumer",path))