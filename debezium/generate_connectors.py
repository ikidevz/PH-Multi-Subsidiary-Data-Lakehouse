import argparse
import json
from pathlib import Path

DEPARTMENTS = {
    "finance": ["journal_entries", "ap_invoices", "ar_ledger"],
    "tax": ["vat_returns", "wht_filings", "bir_filings_log"],
    "hr": ["employees", "payroll_runs", "statutory_contributions"],
    "ops": ["inventory_movements", "purchase_orders", "sales_orders"],
    "sales": ["sales_transactions", "customers", "campaigns"],
    "procurement": ["vendors", "purchase_orders", "wht_certificates"],
    "legal": ["sec_filings", "stockholders", "board_resolutions", "officers"],
    "it_audit": ["audit_log", "access_events", "system_incidents"],
}

SUBSIDIARIES = ["abc", "xyz", "rtl"]
KAFKA_BOOTSTRAP_SERVERS = "datahub-kafka:9092"


def build_config(subsidiary: str, department: str, tables: list[str], args):
    name = f"debezium-{department}-{subsidiary}"
    topic_prefix = f"{subsidiary}.{department}"
    table_list = ",".join(f"public.{t}" for t in tables)

    dept_db = f"dept_{department}_{subsidiary}"

    return {
        "name": name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name": "pgoutput",
            "database.hostname": args.postgres_host,
            "database.port": str(args.postgres_port),
            "database.user": args.postgres_user,
            "database.password": args.postgres_password,
            "database.dbname": dept_db,
            "database.server.name": topic_prefix,
            "table.include.list": table_list,
            "slot.name": f"debezium_{subsidiary}_{department}",
            "publication.name": f"dbz_{subsidiary}_{department}",
            "snapshot.mode": "initial",
            "decimal.handling.mode": "double",
            "tombstones.on.delete": "true",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "true",
            "transforms.unwrap.add.fields": "op,ts_ms,db",
            "schema.history.internal.kafka.topic": f"schema-changes.{subsidiary}.{department}",
            "schema.history.internal.kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate Debezium connector configs for all subsidiaries and departments")
    parser.add_argument("--output-dir", default="./connectors",
                        help="Directory to write connector JSON files")
    parser.add_argument("--postgres-host", default="postgres-dept",
                        help="Postgres hostname (must be postgres-dept)")
    parser.add_argument("--postgres-port", type=int, default=5432,
                        help="Postgres port (5432 inside Docker network)")
    parser.add_argument("--postgres-user", default="postgres",
                        help="Postgres user")
    parser.add_argument("--postgres-password", default="changeme_strong_password",
                        help="Postgres password (set via DEBEZIUM_POSTGRES_PASSWORD in .env)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for subsidiary in SUBSIDIARIES:
        for department, tables in DEPARTMENTS.items():
            config = build_config(subsidiary, department, tables, args)
            file_name = output_dir / f"dept-{department}-{subsidiary}.json"
            with open(file_name, "w", encoding="utf-8") as fh:
                json.dump(config, fh, indent=2)
            print(f"Created {file_name}  (db=dept_{department}_{subsidiary})")

    print(f"\n24 connector configs written to {output_dir}")


if __name__ == "__main__":
    main()
