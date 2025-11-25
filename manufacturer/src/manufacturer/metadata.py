from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

METADATA = {
    "consumer": {
        "entity_name": "consumer",
        "primary_key": "consumer_id",
        "input_path": "/Volumes/dev/00_landing/data/consumer/",
        "schema": StructType([
            StructField("consumer_id", IntegerType()),
            StructField("name", StringType()),
            StructField("age", IntegerType()),
            StructField("gender", StringType()),
            StructField("email", StringType()),
            StructField("phone", StringType()),
            StructField("address", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("country", StringType()),
            StructField("registration_date", StringType()),
            StructField("is_active", BooleanType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["name", "gender", "email", "phone", "address", "city", "state", "country"],
            "date_columns": {"registration_date": "yyyy-MM-dd"},
            "phone_validation": {"column": "phone", "min_length": 10},
            "quality_checks": ["consumer_id IS NOT NULL"]
        }
    },
    "distributor": {
        "entity_name": "distributor",
        "primary_key": "distributor_id",
        "input_path": "/Volumes/dev/00_landing/data/distributor/",
        "schema": StructType([
            StructField("distributor_id", IntegerType()),
            StructField("distributor_name", StringType()),
            StructField("phone_number", StringType()),
            StructField("street_address", StringType()),
            StructField("postal_code", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("country", StringType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["distributor_name", "city", "state", "country"],
            "postal_code_validation": {"column": "postal_code", "length": 6},
            "quality_checks": ["distributor_id IS NOT NULL"]
        }
    },
    "product": {
        "entity_name": "product",
        "primary_key": "product_id",
        "input_path": "/Volumes/dev/00_landing/data/product/",
        "schema": StructType([
            StructField("product_id", IntegerType()),
            StructField("product_name", StringType()),
            StructField("brand", StringType()),
            StructField("manufacturer", StringType()),
            StructField("category", StringType()),
            StructField("department", StringType()),
            StructField("description", StringType()),
            StructField("sku_id", StringType()),
            StructField("upc", StringType()),
            StructField("gtin", StringType()),
            StructField("unit_price", DoubleType()),
            StructField("retail_price", DoubleType()),
            StructField("unit_of_measurement", StringType()),
            StructField("expiration_days", IntegerType()),
            StructField("product_status", StringType()),
            StructField("release_date", StringType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["product_name", "brand", "manufacturer", "category", "department", "description", "sku_id", "upc", "gtin", "unit_of_measurement", "product_status"],
            "date_columns": {"release_date": "yyyy-MM-dd"},
            "numeric_validation": {
                "unit_price": {"min": 0, "type": "double"},
                "retail_price": {"min": 0, "type": "double"}
            },
            "code_validation": {
                "upc": {"min_length": 8},
                "gtin": {"min_length": 8}
            },
            "quality_checks": ["product_id IS NOT NULL"]
        }
    },
    "inventory": {
        "entity_name": "inventory",
        "primary_key": "inventory_id",
        "input_path": "/Volumes/dev/00_landing/data/inventory/",
        "schema": StructType([
            StructField("inventory_id", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("location_type", StringType()),
            StructField("location_name", StringType()),
            StructField("location_code", StringType()),
            StructField("address", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("country", StringType()),
            StructField("phone", StringType()),
            StructField("email", StringType()),
            StructField("location_is_active", BooleanType()),
            StructField("quantity_on_hand", IntegerType()),
            StructField("reorder_level", IntegerType()),
            StructField("reorder_quantity", IntegerType()),
            StructField("safety_stock_level", IntegerType()),
            StructField("inventory_status", StringType()),
            StructField("last_restock_date", StringType()),
            StructField("last_updated", StringType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["location_type", "location_name", "location_code", "address", "city", "state", "country", "phone", "email", "inventory_status"],
            "date_columns": {"last_restock_date": "yyyy-MM-dd"},
            "timestamp_columns": {"last_updated": "yyyy-MM-dd HH:mm:ss"},
            "phone_validation": {"column": "phone", "min_length": 7},
            "email_validation": True,
            "email_column": "email",
            "integer_columns": ["product_id", "quantity_on_hand", "reorder_level", "reorder_quantity", "safety_stock_level"],
            "boolean_columns": ["location_is_active"],
            "filters": ["location_is_active == True"],
            "computed_columns": {
                "is_below_reorder": "CASE WHEN quantity_on_hand IS NOT NULL AND reorder_level IS NOT NULL AND quantity_on_hand < reorder_level THEN True ELSE False END"
            },
            "quality_checks": ["inventory_id IS NOT NULL"]
        }
    },
    "consumer_order": {
        "entity_name": "consumer_order",
        "primary_key": "order_id",
        "input_path": "/Volumes/dev/00_landing/data/consumer_orders/consumer_order/",
        "schema": StructType([
            StructField("order_id", IntegerType()),
            StructField("consumer_id", IntegerType()),
            StructField("order_date", StringType()),
            StructField("order_status", StringType()),
            StructField("total_amount", DoubleType()),
            StructField("currency", StringType()),
            StructField("payment_method", StringType()),
            StructField("channel", StringType()),
            StructField("billing_address", StringType()),
            StructField("shipping_address", StringType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["order_status", "currency", "payment_method", "channel", "billing_address", "shipping_address"],
            "date_columns": {"order_date": "yyyy-MM-dd"},
            "numeric_validation": {
                "total_amount": {"min": 0, "type": "double"}
            },
            "quality_checks": ["order_id IS NOT NULL", "consumer_id IS NOT NULL"],
            "is_joined": True
        }
    },
    "consumer_order_items": {
        "entity_name": "consumer_order_items",
        "primary_key": "order_item_id",
        "input_path": "/Volumes/dev/00_landing/data/consumer_orders/consumer_order_items/",
        "schema": StructType([
            StructField("order_item_id", IntegerType()),
            StructField("order_id", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("total_price", DoubleType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "numeric_validation": {
                "quantity": {"min": 1, "type": "integer"},
                "unit_price": {"min": 0, "type": "double"},
                "total_price": {"min": 0, "type": "double"}
            },
            "quality_checks": ["order_item_id IS NOT NULL", "order_id IS NOT NULL"],
            "is_joined": True
        }
    },
    "distributor_sale_order": {
        "entity_name": "distributor_sale_order",
        "primary_key": ["sales_order_id", "distributor_id"],
        "input_path": "/Volumes/dev/00_landing/data/distributor_sales_order/sale_order/",
        "schema": StructType([
            StructField("sales_order_id", IntegerType()),
            StructField("distributor_id", IntegerType()),
            StructField("order_date", StringType()),
            StructField("expected_delivery_date", StringType()),
            StructField("order_total_amount", DoubleType()),
            StructField("currency", StringType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "date_columns": {"order_date": "yyyy-MM-dd", "expected_delivery_date": "yyyy-MM-dd"},
            "quality_checks": ["distributor_id IS NOT NULL", "sales_order_id IS NOT NULL"],
            "is_joined": True
        }
    },
    "distributor_sale_order_item": {
        "entity_name": "distributor_sale_order_item",
        "primary_key": ["sales_order_id", "sales_order_item_no", "product_id"],
        "input_path": "/Volumes/dev/00_landing/data/distributor_sales_order/sale_order_item/",
        "schema": StructType([
            StructField("sales_order_id", IntegerType()),
            StructField("sales_order_item_no", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("order_quantity", IntegerType()),
            StructField("order_quantity_uom", StringType()),
            StructField("unit_price", DoubleType()),
            StructField("item_total_amount", DoubleType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["order_quantity_uom"],
            "numeric_validation": {
                "order_quantity": {"min": 1, "type": "integer"},
                "unit_price": {"min": 0, "type": "double"},
                "item_total_amount": {"min": 0, "type": "double"}
            },
            "filters": ["order_quantity >= 1"],
            "quality_checks": ["sales_order_id IS NOT NULL", "sales_order_item_no IS NOT NULL"],
            "is_joined": True
        }
    },
    "distributor_invoice": {
        "entity_name": "distributor_invoice",
        "primary_key": "invoice_id",
        "input_path": "/Volumes/dev/00_landing/data/distributor_invoice/invoice/",
        "schema": StructType([
            StructField("invoice_id", IntegerType()),
            StructField("invoice_date", StringType()),
            StructField("invoice_total_amount", DoubleType()),
            StructField("currency", StringType()),
            StructField("tax_amount", DoubleType()),
            StructField("invoice_type", StringType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["currency", "invoice_type"],
            "date_columns": {"invoice_date": "yyyy-MM-dd"},
            "numeric_validation": {
                "invoice_total_amount": {"min": 0, "type": "double"},
                "tax_amount": {"min": 0, "type": "double"}
            },
            "quality_checks": ["invoice_id IS NOT NULL", "invoice_total_amount > 0 AND tax_amount >= 0"],
            "is_joined": True
        }
    },
    "distributor_invoice_item": {
        "entity_name": "distributor_invoice_item",
        "primary_key": ["invoice_id", "invoice_item_no", "product_id"],
        "input_path": "/Volumes/dev/00_landing/data/distributor_invoice/invoice_item/",
        "schema": StructType([
            StructField("invoice_id", IntegerType()),
            StructField("invoice_item_no", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("invoiced_quantity", IntegerType()),
            StructField("invoiced_quantity_uom", StringType()),
            StructField("invoice_item_total_amount", DoubleType()),
            StructField("sales_order_id", IntegerType()),
            StructField("sales_order_item_no", IntegerType()),
            StructField("operation", StringType())
        ]),
        "silver_transformations": {
            "trim_columns": ["invoiced_quantity_uom"],
            "numeric_validation": {
                "invoiced_quantity": {"min": 1, "type": "integer"},
                "invoice_item_total_amount": {"min": 0, "type": "double"}
            },
            "filters": ["invoiced_quantity >= 1", "invoice_item_total_amount > 0"],
            "quality_checks": ["invoice_id IS NOT NULL", "invoice_item_no IS NOT NULL", "product_id IS NOT NULL", "sales_order_id IS NOT NULL", "sales_order_item_no IS NOT NULL"],
            "is_joined": True
        }
    }
}

