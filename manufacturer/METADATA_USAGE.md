# Metadata-Driven Databricks Pipeline Guide

This project uses a metadata-driven approach to manage Databricks pipelines, eliminating hardcoded values and repetitive code.

## Overview

The metadata-driven architecture consists of:

1. **Metadata Configuration** (`metadata/entities.json`) - Centralized definition of all entities, schemas, transformations, and configurations
2. **Metadata Loader** (`src/manufacturer/metadata_loader.py`) - Utility to load and access metadata
3. **Transformation Templates** (`src/manufacturer/transformations/`) - Reusable transformation functions
4. **Generator Scripts** (`scripts/`) - Tools to generate pipeline/job YAML files and transformation code

## Key Benefits

- **Single Source of Truth**: All entity definitions in one JSON file
- **No Hardcoded Values**: Catalog, schemas, paths, and schemas come from metadata
- **Consistent Patterns**: All pipelines follow the same structure
- **Easy Maintenance**: Add new entities by updating metadata, then regenerate code
- **Type Safety**: Schema definitions ensure data consistency

## Metadata Structure

The `metadata/entities.json` file contains:

- **entities**: Array of entity definitions
  - `name`: Entity identifier (used in table names, paths)
  - `display_name`: Human-readable name
  - `primary_key`: Array of primary key columns
  - `input_path`: Source data path in Volumes
  - `schema`: Array of field definitions with name and type
  - `silver_transformations`: Transformation rules for silver layer
- **config**: Global configuration
  - `catalog`: Catalog name (supports variables like `${var.catalog}`)
  - `landing_schema`, `bronze_schema`, `silver_schema`: Schema names for each layer
  - `file_format`: Input file format (default: "json")
  - `cdc_scd_type`: CDC SCD type (default: "2")

## Usage

### 1. Adding a New Entity

1. Add entity definition to `metadata/entities.json`:
```json
{
  "name": "new_entity",
  "display_name": "New Entity",
  "primary_key": ["entity_id"],
  "input_path": "/Volumes/dev/00_landing/data/new_entity/",
  "schema": [
    {"name": "entity_id", "type": "IntegerType"},
    {"name": "name", "type": "StringType"},
    {"name": "operation", "type": "StringType"}
  ],
  "silver_transformations": {
    "trim_columns": ["name"],
    "expectations": ["entity_id IS NOT NULL"]
  }
}
```

2. Generate transformation files:
```bash
python scripts/generate_transformations.py
```

3. Generate pipeline/job YAML files:
```bash
python scripts/generate_resources.py
```

### 2. Using Metadata in Transformations

Transformations use the metadata loader:

```python
from manufacturer.transformations.landing_template import create_landing_pipeline

create_landing_pipeline("product")
```

Or use the generated transformation files that already call the templates.

### 3. Custom Transformations

For complex transformations (like combining multiple entities in silver layer), you can:

1. Use the metadata loader directly:
```python
from manufacturer.metadata_loader import MetadataLoader

loader = MetadataLoader()
entity = loader.get_entity("product")
schema = loader.build_schema("product")
```

2. Create custom transformation files that extend the templates

## Transformation Templates

### Landing Template (`landing_template.py`)
- Creates raw streaming table
- Sets up CDC flow with SCD Type 2
- Uses metadata for schema, paths, and primary keys

### Bronze Template (`bronze_template.py`)
- Creates materialized view from CDC stream
- Filters to current records (removes SCD Type 2 history)
- Drops operational columns

### Silver Template (`silver_template.py`)
- Applies data quality expectations
- Performs common transformations:
  - Trimming string columns
  - Normalizing phone numbers and emails
  - Normalizing UPC/GTIN codes
  - Casting numeric types with validation
  - Converting date/timestamp columns
  - Boolean type casting

## Special Cases

Some entities have combined silver transformations:
- `consumer_orders`: Combines `consumer_order` and `consumer_order_items`
- `distributor_sale_order`: Combines `distributor_sale_order` and `distributor_sale_order_item`
- `distributor_invoice`: Combines `distributor_invoice` and `distributor_invoice_item`

These require custom silver transformation files that join multiple bronze tables.

## Generator Scripts

### `generate_transformations.py`
Generates transformation files for all entities:
- `{entity}_bronze/transformations/landing.py`
- `{entity}_bronze/transformations/bronze.py`
- `{entity}_silver/transformations/silver.py`

### `generate_resources.py`
Generates Databricks resource YAML files:
- Pipeline definitions in `resources/pipelines/{entity}/`
- Job definitions in `resources/jobs/`

## Migration Path

To migrate existing pipelines to metadata-driven:

1. Extract entity definitions from existing code to `metadata/entities.json`
2. Run generator scripts to create new transformation files
3. Test with one entity first
4. Gradually migrate remaining entities
5. Remove old hardcoded transformation files

## Best Practices

1. **Always update metadata first** when adding/modifying entities
2. **Regenerate files** after metadata changes
3. **Version control metadata** - it's the source of truth
4. **Use variables** for environment-specific values (catalog, paths)
5. **Document custom transformations** in code comments
6. **Test transformations** after regeneration

## Troubleshooting

**Issue**: Transformation fails with "Entity not found"
- **Solution**: Check entity name in metadata matches the name used in code

**Issue**: Schema mismatch errors
- **Solution**: Verify schema definition in metadata matches actual data structure

**Issue**: Path not found errors
- **Solution**: Check `input_path` in metadata matches actual Volume path

**Issue**: Primary key errors in CDC
- **Solution**: Verify `primary_key` array in metadata matches actual key columns

