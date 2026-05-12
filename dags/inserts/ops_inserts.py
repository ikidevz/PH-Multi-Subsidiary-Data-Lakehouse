def insert_inventory_movement(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.inventory_movements (
            subsidiary_id, movement_date, movement_type, item_code, quantity,
            unit_cost, total_cost, warehouse_location, reference_doc, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("movement_date"),
            record.get("movement_type"),
            record.get("item_code"),
            record.get("quantity"),
            record.get("unit_cost"),
            record.get("total_cost"),
            record.get("warehouse_location"),
            record.get("reference_doc"),
        ),
    )


def insert_purchase_order(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.purchase_orders (
            subsidiary_id, po_number, vendor_id, order_date,
            expected_date, status, total_amount, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("po_number"),
            record.get("vendor_id"),
            record.get("order_date"),
            record.get("expected_date"),
            record.get("status"),
            record.get("total_amount"),
        ),
    )


def insert_sales_order(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.sales_orders (
            subsidiary_id, so_number, customer_id, order_date,
            ship_date, status, total_amount, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("so_number"),
            record.get("customer_id"),
            record.get("order_date"),
            record.get("ship_date"),
            record.get("status"),
            record.get("total_amount"),
        ),
    )
