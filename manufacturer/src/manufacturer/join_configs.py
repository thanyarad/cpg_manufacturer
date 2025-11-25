JOIN_CONFIGS = {
    "consumer_orders": {
        "output_entity": "consumer_orders",
        "join_sql": """
            SELECT 
                i.order_item_id,
                i.order_id,
                i.product_id,
                i.quantity,
                i.unit_price,
                i.total_price,
                o.consumer_id,
                o.order_date,
                o.order_status,
                o.total_amount AS order_total_amount,
                o.currency,
                o.payment_method,
                o.channel,
                o.billing_address,
                o.shipping_address
            FROM consumer_order_items_temp i
            LEFT JOIN consumer_order_temp o
            ON i.order_id = o.order_id
        """
    },
    "distributor_sale_order": {
        "output_entity": "distributor_sale_order",
        "join_sql": """
            SELECT 
                so.sales_order_id as order_id,
                so.distributor_id, 
                so.order_date, 
                so.expected_delivery_date,  
                so.currency, 
                soi.sales_order_item_no as item_no, 
                soi.product_id, 
                soi.order_quantity, 
                soi.order_quantity_uom as unit_measure, 
                soi.unit_price, 
                soi.item_total_amount
            FROM distributor_sale_order_item_temp soi
            LEFT JOIN distributor_sale_order_temp so
            ON so.sales_order_id = soi.sales_order_id
        """
    },
    "distributor_invoice": {
        "output_entity": "distributor_invoice",
        "join_sql": """
            SELECT 
                iit.invoice_id,
                iit.invoice_item_no,
                iit.product_id,
                iit.invoiced_quantity,
                iit.invoiced_quantity_uom as primary_unit,
                iit.invoice_item_total_amount,
                iit.sales_order_id,
                iit.sales_order_item_no,
                it.invoice_date,
                it.currency,
                it.invoice_type
            FROM distributor_invoice_item_temp iit
            LEFT JOIN distributor_invoice_temp it
            ON iit.invoice_id=it.invoice_id
        """
    }
}

