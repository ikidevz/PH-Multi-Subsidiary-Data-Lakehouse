# Debezium Connector Setup

This directory contains tools to generate and register Debezium PostgreSQL connectors for CDC ingestion.

## Generate connector definitions

```bash
python3 debezium/generate_connectors.py --output-dir debezium/connectors
```

## Register connectors with Kafka Connect

```bash
bash debezium/register_connectors.sh
```

## Default parameters

- `CONNECT_URL`: URL to Kafka Connect (default: http://kafka-connect:8083)
- `POSTGRES_HOST`: Postgres host (default: postgres-central)
- `POSTGRES_PORT`: Postgres port (default: 5432)
- `POSTGRES_USER`: Postgres user (default: postgres)
- `POSTGRES_PASSWORD`: Postgres password (default: changeme_strong_password)
- `POSTGRES_DB`: Postgres database (default: lakehouse)
