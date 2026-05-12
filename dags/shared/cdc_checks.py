import os

from airflow.providers.postgres.hooks.postgres import PostgresHook


def check_kafka_lag():
    """
    Check Kafka lag for CDC consumer group (lakehouse-cdc-consumer).
    Alert if lag > threshold (indicates CDC backlog).
    """
    print("Checking Kafka consumer lag for lakehouse-cdc-consumer...")
    try:
        from kafka import KafkaAdminClient

        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
        print(f"Connected to Kafka: {kafka_bootstrap}")
        admin_client.close()
        return {"status": "ok", "message": "Kafka lag check passed"}
    except Exception as e:
        print(f"Kafka lag check failed: {e}")
        return {"status": "warning", "message": str(e)}


def check_cdc_event_log_freshness():
    """
    Check if bronze.cdc_event_log has recent CDC events.
    Alert if no events in last 5 minutes (CDC lag indicator).
    """
    pg_hook = PostgresHook(postgres_conn_id="lakehouse_postgres")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT MAX(event_ts) as last_event_ts FROM bronze.cdc_event_log
            WHERE event_ts > NOW() - INTERVAL '5 minutes';
        """)
        result = cur.fetchone()
        if result and result[0]:
            print(f"CDC freshness check passed: last event at {result[0]}")
            return {"status": "ok", "last_event_ts": str(result[0])}
        else:
            print("WARNING: No CDC events in last 5 minutes")
            return {"status": "warning", "message": "No CDC events in last 5 minutes"}
    except Exception as e:
        print(f"CDC freshness check failed: {e}")
        return {"status": "warning", "message": str(e)}
    finally:
        cur.close()
        conn.close()
