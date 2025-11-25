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
    
    config = metadata["config"]
    
    if layer == "bronze":
        schema = config.get("landing_schema", "00_landing")
        include_pattern = f"../../../src/{entity_name}/{entity_name}_bronze/transformations/**"
        src_path = f"../../../src/{entity_name}/{entity_name}_bronze"
    elif layer == "silver":
        schema = config.get("silver_schema", "02_silver")
        if entity_name in ["consumer_orders", "distributor_invoice", "distributor_sale_order"]:
            include_pattern = f"../../../src/{entity_name}/{entity_name}_silver/transformations/**"
            src_path = f"../../../src/{entity_name}/{entity_name}_silver"
        else:
            include_pattern = f"../../../src/{entity_name}/{entity_name}_silver/transformations/**"
            src_path = f"../../../src/{entity_name}/{entity_name}_silver"
    else:
        schema = config.get("landing_schema", "00_landing")
        include_pattern = f"../../../src/{entity_name}/{entity_name}_bronze/transformations/**"
        src_path = f"../../../src/{entity_name}/{entity_name}_bronze"
    
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

def generate_job_yaml(job_name: str, entity_names: list, metadata: Dict[str, Any]) -> Dict[str, Any]:
    config = metadata["config"]
    
    primary_entity = next((e for e in metadata["entities"] if e["name"] == entity_names[0]), None)
    if not primary_entity:
        raise ValueError(f"Entity {entity_names[0]} not found in metadata")
    
    input_path = primary_entity.get("input_path", "")
    if "/" in input_path:
        parts = input_path.split("/")
        if len(parts) > 1:
            base_path = "/".join(parts[:-1])
            if base_path:
                input_path = base_path
    
    tasks = []
    bronze_tasks = []
    
    for entity_name in entity_names:
        entity = next((e for e in metadata["entities"] if e["name"] == entity_name), None)
        if not entity:
            continue
        
        bronze_task = {
            "task_key": f"{entity_name}_bronze",
            "pipeline_task": {
                "pipeline_id": "${resources.pipelines." + f"{entity_name}_bronze" + ".id}",
                "full_refresh": False
            }
        }
        tasks.append(bronze_task)
        bronze_tasks.append(f"{entity_name}_bronze")
    
    if len(entity_names) == 1:
        silver_entity = entity_names[0]
        silver_task = {
            "task_key": f"{silver_entity}_silver",
            "depends_on": [{"task_key": f"{silver_entity}_bronze"}],
            "pipeline_task": {
                "pipeline_id": "${resources.pipelines." + f"{silver_entity}_silver" + ".id}"
            }
        }
        tasks.append(silver_task)
    else:
        silver_job_name = job_name
        silver_task = {
            "task_key": f"{silver_job_name}_silver",
            "depends_on": [{"task_key": task} for task in bronze_tasks],
            "pipeline_task": {
                "pipeline_id": "${resources.pipelines." + f"{silver_job_name}_silver" + ".id}"
            }
        }
        tasks.append(silver_task)
    
    job_config = {
        "resources": {
            "jobs": {
                job_name: {
                    "name": job_name,
                    "trigger": {
                        "pause_status": "PAUSED",
                        "file_arrival": {
                            "url": input_path
                        }
                    },
                    "tasks": tasks,
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
        entity_dir.mkdir(parents=True, exist_ok=True)
        
        bronze_yaml = generate_pipeline_yaml(entity_name, "bronze", metadata)
        bronze_file = entity_dir / f"{entity_name}_bronze.pipeline.yml"
        with open(bronze_file, 'w') as f:
            yaml.dump(bronze_yaml, f, default_flow_style=False, sort_keys=False)
    
    silver_groups = {
        "consumer_orders": ["consumer_order", "consumer_order_items"],
        "distributor_invoice": ["distributor_invoice", "distributor_invoice_item"],
        "distributor_sale_order": ["distributor_sale_order", "distributor_sale_order_item"]
    }
    
    processed_entities = set()
    
    for silver_name, entity_names in silver_groups.items():
        silver_dir = pipelines_dir / silver_name
        silver_dir.mkdir(parents=True, exist_ok=True)
        
        silver_yaml = generate_pipeline_yaml(silver_name, "silver", metadata)
        silver_file = silver_dir / f"{silver_name}_silver.pipeline.yml"
        with open(silver_file, 'w') as f:
            yaml.dump(silver_yaml, f, default_flow_style=False, sort_keys=False)
        
        for entity_name in entity_names:
            processed_entities.add(entity_name)
    
    for entity in metadata["entities"]:
        entity_name = entity["name"]
        if entity_name not in processed_entities:
            silver_dir = pipelines_dir / entity_name / f"{entity_name}_silver"
            silver_dir.mkdir(parents=True, exist_ok=True)
            
            silver_yaml = generate_pipeline_yaml(entity_name, "silver", metadata)
            silver_file = silver_dir / f"{entity_name}_silver.pipeline.yml"
            with open(silver_file, 'w') as f:
                yaml.dump(silver_yaml, f, default_flow_style=False, sort_keys=False)

def generate_all_jobs(metadata: Dict[str, Any], output_dir: Path):
    jobs_dir = output_dir / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    
    job_mappings = {
        "consumer_orders": ["consumer_order", "consumer_order_items"],
        "distributor_invoice": ["distributor_invoice", "distributor_invoice_item"],
        "distributor_sale_order": ["distributor_sale_order", "distributor_sale_order_item"]
    }
    
    processed_entities = set()
    
    for job_name, entity_names in job_mappings.items():
        job_yaml = generate_job_yaml(job_name, entity_names, metadata)
        job_yaml_file = jobs_dir / f"{job_name}.job.yml"
        
        with open(job_yaml_file, 'w') as f:
            yaml.dump(job_yaml, f, default_flow_style=False, sort_keys=False)
        
        for entity_name in entity_names:
            processed_entities.add(entity_name)
    
    for entity in metadata["entities"]:
        entity_name = entity["name"]
        if entity_name not in processed_entities:
            job_yaml = generate_job_yaml(entity_name, [entity_name], metadata)
            job_yaml_file = jobs_dir / f"{entity_name}.job.yml"
            
            with open(job_yaml_file, 'w') as f:
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

