def insert_vat_return(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.vat_returns (
            subsidiary_id, period_month, period_year, form_type, gross_sales,
            exempt_sales, zero_rated_sales, taxable_sales, output_vat, input_vat,
            vat_payable, filing_date, efps_confirmation, status, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("period_month"),
            record.get("period_year"),
            record.get("form_type"),
            record.get("gross_sales"),
            record.get("exempt_sales", 0),
            record.get("zero_rated_sales", 0),
            record.get("taxable_sales"),
            record.get("output_vat"),
            record.get("input_vat"),
            record.get("vat_payable"),
            record.get("filing_date"),
            record.get("efps_confirmation"),
            record.get("status"),
        ),
    )


def insert_wht_filing(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.wht_filings (
            subsidiary_id, form_type, period_month, period_year, payee_tin, payee_name,
            income_payment, atc_code, tax_rate, wht_amount, filing_date,
            efps_confirmation, status, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("form_type"),
            record.get("period_month"),
            record.get("period_year"),
            record.get("payee_tin"),
            record.get("taxpayer_name"),
            record.get("income_payment"),
            record.get("atc_code"),
            record.get("tax_rate"),
            record.get("wht_amount"),
            record.get("issued_date"),
            record.get("efps_reference"),
            record.get("status"),
        ),
    )


def insert_bir_filing_log(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.bir_filings_log (
            subsidiary_id, form_type, period_covered, due_date, filing_date,
            amount_paid, surcharge_amount, interest_amount, compromise_amount,
            payment_mode, efps_ref, status, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("filing_type"),
            record.get("period_covered"),
            record.get("due_date"),
            record.get("submitted_at"),
            record.get("amount_paid"),
            record.get("surcharge_amount", 0),
            record.get("interest_amount", 0),
            record.get("compromise_amount", 0),
            record.get("payment_mode"),
            record.get("efps_reference"),
            record.get("status"),
        ),
    )
