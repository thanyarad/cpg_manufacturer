import json
from pathlib import Path
from typing import Dict, Any

def load_metadata(metadata_path: str = None) -> Dict[str, Any]:
    if metadata_path is None:
        base_path = Path(__file__).parent.parent
        metadata_path = base_path / "metadata" / "entities.json"
    with open(metadata_path, 'r') as f:
        return json.load(f)

def generate_landing_file(entity_name: str, output_path: Path):
    content = f"""from manufacturer.transformations.landing_template import create_landing_pipeline

create_landing_pipeline("{entity_name}")
"""
    output_path.write_text(content)

def generate_bronze_file(entity_name: str, output_path: Path):
    content = f"""from manufacturer.transformations.bronze_template import create_bronze_pipeline

create_bronze_pipeline("{entity_name}")
"""
    output_path.write_text(content)

def generate_silver_file(entity_name: str, output_path: Path):
    content = f"""from manufacturer.transformations.silver_template import create_silver_pipeline

create_silver_pipeline("{entity_name}")
"""
    output_path.write_text(content)

def generate_all_transformations(metadata: Dict[str, Any], base_path: Path):
    src_dir = base_path / "src"
    
    for entity in metadata["entities"]:
        entity_name = entity["name"]
        
        bronze_dir = src_dir / entity_name / f"{entity_name}_bronze" / "transformations"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        generate_landing_file(entity_name, bronze_dir / "landing.py")
        generate_bronze_file(entity_name, bronze_dir / "bronze.py")
        
        silver_dir = src_dir / entity_name / f"{entity_name}_silver" / "transformations"
        silver_dir.mkdir(parents=True, exist_ok=True)
        generate_silver_file(entity_name, silver_dir / "silver.py")
        
        print(f"Generated transformations for {entity_name}")

def main():
    base_path = Path(__file__).parent.parent
    metadata = load_metadata()
    generate_all_transformations(metadata, base_path)
    print("Generated all transformation files from metadata")

if __name__ == "__main__":
    main()

