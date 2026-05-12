def insert_sec_filing(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.sec_filings (
            subsidiary_id, filing_type, due_date, filing_date,
            status, reference_number, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("filing_type"),
            record.get("due_date"),
            record.get("filing_date"),
            record.get("status"),
            record.get("reference_number"),
        ),
    )


def insert_stockholder(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.stockholders (
            subsidiary_id, stockholder_id, name, ownership_pct,
            is_foreign, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("stockholder_id"),
            record.get("name"),
            record.get("ownership_pct"),
            record.get("is_foreign", False),
        ),
    )


def insert_board_resolution(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.board_resolutions (
            subsidiary_id, resolution_number, resolution_date,
            title, enacted_by, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("resolution_number"),
            record.get("resolution_date"),
            record.get("title"),
            record.get("enacted_by"),
        ),
    )


def insert_officer(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.officers (
            subsidiary_id, officer_id, name, position,
            start_date, end_date, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("officer_id"),
            record.get("name"),
            record.get("position"),
            record.get("start_date"),
            record.get("end_date"),
        ),
    )
