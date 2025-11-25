import json
import os
from pathlib import Path
from typing import Dict, List, Any
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

TYPE_MAPPING = {
    "StringType": StringType,
    "IntegerType": IntegerType,
    "BooleanType": BooleanType,
    "DoubleType": DoubleType
}

class MetadataLoader:
    def __init__(self, metadata_path: str = None):
        if metadata_path is None:
            base_path = Path(__file__).parent.parent.parent
            metadata_path = base_path / "metadata" / "entities.json"
        self.metadata_path = metadata_path
        self._metadata = None
        self._load_metadata()
    
    def _load_metadata(self):
        with open(self.metadata_path, 'r') as f:
            self._metadata = json.load(f)
    
    def get_config(self) -> Dict[str, Any]:
        return self._metadata.get("config", {})
    
    def get_entity(self, entity_name: str) -> Dict[str, Any]:
        for entity in self._metadata["entities"]:
            if entity["name"] == entity_name:
                return entity
        raise ValueError(f"Entity '{entity_name}' not found in metadata")
    
    def get_all_entities(self) -> List[Dict[str, Any]]:
        return self._metadata["entities"]
    
    def build_schema(self, entity_name: str) -> StructType:
        entity = self.get_entity(entity_name)
        fields = []
        for field_def in entity["schema"]:
            field_name = field_def["name"]
            field_type_str = field_def["type"]
            field_type = TYPE_MAPPING.get(field_type_str, StringType)
            fields.append(StructField(field_name, field_type(), True))
        return StructType(fields)
    
    def get_primary_key(self, entity_name: str) -> List[str]:
        entity = self.get_entity(entity_name)
        return entity.get("primary_key", [])
    
    def get_input_path(self, entity_name: str) -> str:
        entity = self.get_entity(entity_name)
        return entity.get("input_path", "")
    
    def get_silver_transformations(self, entity_name: str) -> Dict[str, Any]:
        entity = self.get_entity(entity_name)
        return entity.get("silver_transformations", {})
    
    def get_catalog(self) -> str:
        config = self.get_config()
        catalog = config.get("catalog", "dev")
        if catalog.startswith("${") and catalog.endswith("}"):
            import os
            var_name = catalog[2:-1].replace("var.", "")
            catalog = os.getenv(var_name.upper(), "dev")
        return catalog
    
    def get_schema_name(self, layer: str) -> str:
        config = self.get_config()
        schema_map = {
            "landing": config.get("landing_schema", "00_landing"),
            "bronze": config.get("bronze_schema", "01_bronze"),
            "silver": config.get("silver_schema", "02_silver")
        }
        return schema_map.get(layer, "00_landing")

