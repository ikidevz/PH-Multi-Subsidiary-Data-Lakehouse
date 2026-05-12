def insert_journal_entry(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.journal_entries (
            subsidiary_id, entry_date, entry_number, account_code, account_name,
            debit_amount, credit_amount, description, reference_doc, is_interco,
            counterpart_sub_id, cost_center, posted_by, approved_by, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("entry_date"),
            record.get("entry_number"),
            record.get("account_code"),
            record.get("account_name"),
            record.get("debit_amount"),
            record.get("credit_amount"),
            record.get("description"),
            record.get("reference_doc"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
            record.get("cost_center"),
            record.get("posted_by"),
            record.get("approved_by"),
        ),
    )


def insert_ap_invoice(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.ap_invoices (
            subsidiary_id, invoice_number, vendor_id, vendor_name, vendor_tin,
            invoice_date, due_date, gross_amount, vat_amount, net_amount, wht_amount,
            payment_amount, payment_date, status, is_interco, counterpart_sub_id, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("invoice_number"),
            record.get("vendor_id"),
            record.get("vendor_name"),
            record.get("vendor_tin"),
            record.get("invoice_date"),
            record.get("due_date"),
            record.get("gross_amount"),
            record.get("vat_amount"),
            record.get("net_amount"),
            record.get("wht_amount"),
            record.get("payment_amount"),
            record.get("payment_date"),
            record.get("status"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
        ),
    )


def insert_ar_ledger(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.ar_ledger (
            subsidiary_id, invoice_number, customer_id, customer_name, customer_tin,
            invoice_date, due_date, gross_amount, vat_amount, net_amount,
            collected_amount, collection_date, status, is_interco, counterpart_sub_id, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("invoice_number"),
            record.get("customer_id"),
            record.get("customer_name"),
            record.get("customer_tin"),
            record.get("invoice_date"),
            record.get("due_date"),
            record.get("gross_amount"),
            record.get("vat_amount"),
            record.get("net_amount"),
            record.get("collected_amount"),
            record.get("collection_date"),
            record.get("status"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
        ),
    )
