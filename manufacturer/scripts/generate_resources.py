import json
import yaml
from pathlib import Path
from typing import Dict, Any

def load_metadata(metadata_path: str = None) -> Dict[str, Any]:
    if metadata_path is None:
        base_path = Path(__file__).parent.parent
        metadata_path = base_path / "metadata" / "entities.json"
    with open(metadata_path, 'r') as f:
        return json.load(f)

def generate_pipeline_yaml(entity_name: str, layer: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_key = f"{entity_name}_{layer}"
    pipeline_name = f"{entity_name}_{layer}"
    
    if layer == "bronze":
        src_path = f"../../../src/{entity_name}/{entity_name}_bronze"
        include_pattern = f"../../../src/{entity_name}/{entity_name}_bronze/transformations/**"
    elif layer == "silver":
        src_path = f"../../../src/{entity_name}/{entity_name}_silver"
        include_pattern = f"../../../src/{entity_name}/{entity_name}_silver/transformations/**"
    else:
        src_path = f"../../../src/{entity_name}/{entity_name}_bronze"
        include_pattern = f"../../../src/{entity_name}/{entity_name}_bronze/transformations/**"
    
    config = metadata["config"]
    schema = config.get("landing_schema", "00_landing") if layer == "bronze" else config.get("bronze_schema", "01_bronze")
    
    return {
        "resources": {
            "pipelines": {
                pipeline_key: {
                    "name": pipeline_name,
                    "libraries": [
                        {
                            "glob": {
                                "include": include_pattern
                            }
                        }
                    ],
                    "serverless": True,
                    "catalog": "${var.catalog}",
                    "schema": schema,
                    "root_path": src_path
                }
            }
        }
    }

def generate_job_yaml(entity_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    entity = next((e for e in metadata["entities"] if e["name"] == entity_name), None)
    if not entity:
        raise ValueError(f"Entity {entity_name} not found in metadata")
    
    config = metadata["config"]
    input_path = entity.get("input_path", "")
    
    job_config = {
        "resources": {
            "jobs": {
                entity_name: {
                    "name": entity_name,
                    "trigger": {
                        "pause_status": "PAUSED",
                        "file_arrival": {
                            "url": input_path
                        }
                    },
                    "tasks": [
                        {
                            "task_key": f"{entity_name}_bronze",
                            "pipeline_task": {
                                "pipeline_id": "${resources.pipelines." + f"{entity_name}_bronze" + ".id}",
                                "full_refresh": False
                            }
                        },
                        {
                            "task_key": f"{entity_name}_silver",
                            "depends_on": [
                                {
                                    "task_key": f"{entity_name}_bronze"
                                }
                            ],
                            "pipeline_task": {
                                "pipeline_id": "${resources.pipelines." + f"{entity_name}_silver" + ".id}"
                            }
                        }
                    ],
                    "queue": {
                        "enabled": True
                    },
                    "performance_target": "PERFORMANCE_OPTIMIZED"
                }
            }
        }
    }
    
    return job_config

def generate_all_pipelines(metadata: Dict[str, Any], output_dir: Path):
    pipelines_dir = output_dir / "pipelines"
    
    for entity in metadata["entities"]:
        entity_name = entity["name"]
        entity_dir = pipelines_dir / entity_name
        
        for layer in ["bronze", "silver"]:
            if layer == "bronze":
                layer_dir = entity_dir
            else:
                layer_dir = entity_dir / f"{entity_name}_silver"
            
            layer_dir.mkdir(parents=True, exist_ok=True)
            
            pipeline_yaml = generate_pipeline_yaml(entity_name, layer, metadata)
            yaml_file = layer_dir / f"{entity_name}_{layer}.pipeline.yml"
            
            with open(yaml_file, 'w') as f:
                yaml.dump(pipeline_yaml, f, default_flow_style=False, sort_keys=False)

def generate_all_jobs(metadata: Dict[str, Any], output_dir: Path):
    jobs_dir = output_dir / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    
    for entity in metadata["entities"]:
        entity_name = entity["name"]
        job_yaml = generate_job_yaml(entity_name, metadata)
        yaml_file = jobs_dir / f"{entity_name}.job.yml"
        
        with open(yaml_file, 'w') as f:
            yaml.dump(job_yaml, f, default_flow_style=False, sort_keys=False)

def main():
    base_path = Path(__file__).parent.parent
    metadata = load_metadata()
    resources_dir = base_path / "resources"
    
    generate_all_pipelines(metadata, resources_dir)
    generate_all_jobs(metadata, resources_dir)
    
    print("Generated all pipeline and job YAML files from metadata")

if __name__ == "__main__":
    main()

