def insert_sales_record(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.sales_transactions (
            subsidiary_id, invoice_number, customer_name, transaction_date,
            gross_amount, discount_amount, vat_amount, net_amount,
            vat_classification, is_interco, counterpart_sub_id, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("invoice_number"),
            record.get("customer_name"),
            record.get("transaction_date"),
            record.get("gross_amount"),
            record.get("discount_amount"),
            record.get("vat_amount"),
            record.get("net_amount"),
            record.get("vat_classification"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
        ),
    )


def insert_customer(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.customers (
            subsidiary_id, customer_id, customer_name, customer_tin,
            contact_email, phone, is_active, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("customer_id"),
            record.get("customer_name"),
            record.get("tin"),
            record.get("contact_email"),
            record.get("phone"),
            record.get("is_active", True),
        ),
    )


def insert_campaign(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.campaigns (
            subsidiary_id, campaign_id, campaign_name, campaign_type, channel,
            start_date, end_date, budget_amount, actual_spend, leads_generated,
            conversions, revenue_attributed, status, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("campaign_id"),
            record.get("campaign_name"),
            record.get("campaign_type"),
            record.get("channel"),
            record.get("start_date"),
            record.get("end_date"),
            record.get("budget_amount"),
            record.get("actual_spend"),
            record.get("leads_generated"),
            record.get("conversions"),
            record.get("revenue_attributed"),
            record.get("status"),
        ),
    )
