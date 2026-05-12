def insert_audit_event(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.audit_log (
            subsidiary_id, event_timestamp, user_id,
            action, result, risk_level, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("event_timestamp"),
            record.get("user_id"),
            record.get("action"),
            record.get("result"),
            record.get("risk_level"),
        ),
    )


def insert_access_event(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.access_events (
            subsidiary_id, event_timestamp, user_id, action,
            resource, resource_id, ip_address, result, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("event_timestamp"),
            record.get("user_id"),
            record.get("action"),
            record.get("resource"),
            record.get("resource_id"),
            record.get("ip_address"),
            record.get("result"),
        ),
    )


def insert_system_incident(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.system_incidents (
            subsidiary_id, incident_id, incident_date, incident_type,
            severity, description, resolved_date, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("incident_id"),
            record.get("incident_date"),
            record.get("incident_type"),
            record.get("severity"),
            record.get("description"),
            record.get("resolved_date"),
        ),
    )
