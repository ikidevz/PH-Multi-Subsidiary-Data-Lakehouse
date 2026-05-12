def insert_vendor(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.vendors (
            subsidiary_id, vendor_id, vendor_name, vendor_tin,
            vendor_type, payment_terms, is_accredited, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("vendor_id"),
            record.get("vendor_name"),
            record.get("vendor_tin"),
            record.get("vendor_type"),
            record.get("payment_terms"),
            record.get("is_accredited"),
        ),
    )


def insert_purchase_order(cur, record):
    """Procurement-side purchase order insert (same schema as ops)."""
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


def insert_wht_certificate(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.wht_certificates (
            subsidiary_id, cert_number, vendor_id, atc_code,
            income_payment, tax_rate, wht_amount, issued_date, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("cert_number"),
            record.get("vendor_id"),
            record.get("atc_code"),
            record.get("income_payment"),
            record.get("tax_rate"),
            record.get("wht_amount"),
            record.get("issued_date"),
        ),
    )
