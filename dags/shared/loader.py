import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_records(url, params):
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    return payload if isinstance(payload, list) else []


def load_records(url, insert_fn, params=None):
    params = params or {}
    hook = PostgresHook(postgres_conn_id="lakehouse_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()
    records = fetch_records(url, params)

    for record in records:
        insert_fn(cur, record)

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


def ingest_endpoint(url, insert_fn, params=None):
    loaded = load_records(url=url, insert_fn=insert_fn, params=params)
    return f"Inserted {loaded} record(s)"
