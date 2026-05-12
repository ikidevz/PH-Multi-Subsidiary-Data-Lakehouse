# PH Multi-Subsidiary Data Lakehouse

A production-grade Philippine conglomerate data platform with real CRUD operations, change data capture (CDC), and medallion architecture. Eight department microservices (FastAPI) with persistent PostgreSQL databases. Debezium captures all changes via Kafka CDC. Airflow orchestrates medallion transformations (bronze → silver → gold). Apache Superset provides BI dashboards.

**Architecture:** Medallion + CDC (OLTP Source → Kafka → OLAP Warehouse)  
**Deployment:** Docker Compose (24 department services + 3 subsidiaries)  
**Data Flow:** Department CRUD → PostgreSQL WAL → Debezium → Kafka → CDC Consumer → Central Warehouse  
**Compliance:** Soft deletes, immutable audit log, full change history tracking

---

![img](https://tdhghaslnufgtzjybhhf.supabase.co/storage/v1/object/public/content/Data%20Engineering/PH%20Multi-Subsidiary%20Data%20Lakehouse/Architecture%20Sketch.png)

## What Changed in This Version

This version introduces a **2-instance PostgreSQL topology** replacing the previous single `postgres-central` instance. All fixes, network corrections, and init-script splits documented below.

### PostgreSQL Instances

| Instance           | Container        | Port     | Hosts                                            |
| ------------------ | ---------------- | -------- | ------------------------------------------------ |
| `postgres-dept`    | postgres-dept    | **5433** | All 24 `dept_*` OLTP databases                   |
| `postgres-central` | postgres-central | **5432** | `lakehouse` + `airflow` + `superset` + `datahub` |

**Why split:** Debezium reads WAL from the dept source databases. Keeping them on a separate instance means CDC replication slots and WAL pressure are fully isolated from the analytics workload (Airflow dbt runs, Superset queries) on `postgres-central`. The separation that matters is **OLTP source vs everything else**.

### Init Scripts Split

```
init-scripts/
├── dept/                              → mounts into postgres-dept
│   ├── 00_create_extensions.sql       pgcrypto + dblink
│   ├── 01_create_dept_databases.sql   all 24 dept_* DBs (abc/xyz/rtl)
│   └── 02_create_roles.sql            abc_dept_user, xyz_dept_user, rtl_dept_user, debezium_user
│
└── central/                           → mounts into postgres-central
    ├── 00_create_extensions.sql       pgcrypto + dblink
    ├── 01_create_central_databases.sql lakehouse + airflow + superset + datahub
    ├── 02_create_roles.sql            superset_reader, lakehouse_writer, abc/xyz/retail_user
    ├── 03_create_schemas.sql          bronze / silver / gold schemas
    ├── 04_grant_permissions.sql       all grants + default privileges
    ├── 05_create_bronze_tables.sql       all 23 bronze.* tables
    ├── 06_add_soft_delete_columns.sql is_deleted + deleted_at on all bronze tables
    └── 07_seed_data.sql               subsidiaries seed data
```

**Removed from dept scripts:** `ALTER SYSTEM SET wal_level = logical` — replaced by docker CMD flags on `postgres-dept` container (`-c wal_level=logical -c max_replication_slots=30 -c max_wal_senders=30`). No restart needed.

### Network Fixes

| Network               | Members                                                                                                          |
| --------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `lakehouse-net`       | `postgres-dept`, all 24 `dept-*` containers, `kafka-connect`                                                     |
| `multi-subsidary-net` | `postgres-central`, `airflow-*`, `cdc-consumer`, `superset`, `datahub-*`, `kafka`, `zookeeper`, `redis`, `minio` |

`kafka-connect` joins **both networks** — it reads WAL from `postgres-dept` (lakehouse-net) and publishes to Kafka (multi-subsidary-net).

`pgadmin` also joins both networks so it can connect to either instance.

### .env Restructure

Variables are now grouped by instance:

```
POSTGRES_DEPT_HOST / POSTGRES_DEPT_PORT / POSTGRES_DEPT_USER / POSTGRES_DEPT_PASSWORD
POSTGRES_CENTRAL_HOST / POSTGRES_CENTRAL_PORT / POSTGRES_CENTRAL_USER / POSTGRES_CENTRAL_PASSWORD
```

`AIRFLOW_CONN_LAKEHOUSE_POSTGRES` — Airflow DAG connection to the lakehouse (on `postgres-central`)  
`LAKEHOUSE_POSTGRES_DSN` — CDC consumer DSN to `postgres-central/lakehouse` (separate from Airflow)  
`DEBEZIUM_POSTGRES_HOST` — points to `postgres-dept` (not central)

---

## Quick Start

```bash
# 1. Setup environment
cp .env.template .env
# Edit .env — fill in POSTGRES_DEPT_PASSWORD, POSTGRES_CENTRAL_PASSWORD,
# FERNET_KEY, SUPERSET_SECRET_KEY with strong values

# 2. Start core infrastructure
#    postgres-dept (5433) + postgres-central (5432) + Redis + Airflow + MinIO + Superset
docker compose up --build -d

# 3. Start CDC infrastructure
docker compose up -d datahub-kafka datahub-zookeeper kafka-connect debezium-ui cdc-consumer

# 4. Start department services (ABC — 8 containers)
docker compose -f docker-compose.yml -f docker-compose.dept.yml up --build -d

# 5. Register Debezium CDC connectors (24 total: 8 depts × 3 subs)
#    Connectors point to postgres-dept automatically
bash debezium/register_connectors.sh

# 6. Verify all services
curl http://localhost:8001/health   # dept-finance-abc
curl http://localhost:8083/connectors | jq '.[] | .name'  # Debezium connectors

# 7. Test CRUD + CDC
curl -X POST http://localhost:8001/journal-entries \
  -H "Content-Type: application/json" \
  -d '{"entry_date":"2026-05-11","entry_number":"JE-001","account_code":"1010","debit_amount":50000}'

# 8. Trigger Airflow DAG (wait 5 min for CDC warm-up)
# Login: http://localhost:8080  (user: airflow_multi_subsidiary_user)
# Trigger: lakehouse_bronze_ingestion_and_transform

# 9. Explore dashboards
# Superset:     http://localhost:8088
# Debezium UI:  http://localhost:8084
# MinIO:        http://localhost:9001
# DataHub:      http://localhost:9002
# pgAdmin:      http://localhost:5050  (connects to both instances)
```

---

## Services & Ports

| Service                       | Port     | Role                                    | DB Instance                            |
| ----------------------------- | -------- | --------------------------------------- | -------------------------------------- |
| **Databases**                 |          |                                         |                                        |
| postgres-dept                 | **5433** | OLTP source — 24 dept DBs               | —                                      |
| postgres-central              | **5432** | Lakehouse + platform metadata           | —                                      |
| **Core**                      |          |                                         |                                        |
| Redis                         | 6379     | Celery broker for Airflow               | —                                      |
| Airflow WebUI                 | 8080     | DAG orchestration & monitoring          | postgres-central/airflow               |
| pgAdmin                       | 5050     | DB admin (both instances)               | both                                   |
| **CDC Pipeline**              |          |                                         |                                        |
| Kafka                         | 9092     | CDC event streaming broker              | —                                      |
| Zookeeper                     | 2181     | Kafka coordination                      | —                                      |
| Kafka Connect                 | 8083     | Debezium connector manager              | postgres-dept (WAL)                    |
| Debezium UI                   | 8084     | CDC connector monitoring                | —                                      |
| CDC Consumer                  | —        | Kafka → bronze layer ingestion          | postgres-central/lakehouse             |
| **Analytics**                 |          |                                         |                                        |
| Superset                      | 8088     | BI dashboards (gold layer)              | postgres-central/lakehouse + /superset |
| MinIO S3 API                  | 9000     | Object storage                          | —                                      |
| MinIO Console                 | 9001     | MinIO Web UI                            | —                                      |
| DataHub GMS                   | 8085     | Data catalog & lineage                  | postgres-central/datahub               |
| DataHub Frontend              | 9002     | Data governance UI                      | —                                      |
| **Department Services (ABC)** |          |                                         |                                        |
| dept-finance-abc              | 8001     | Finance API (journal entries, invoices) | postgres-dept/dept_finance_abc         |
| dept-tax-abc                  | 8002     | Tax API (VAT, WHT, BIR filings)         | postgres-dept/dept_tax_abc             |
| dept-hr-abc                   | 8003     | HR API (employees, payroll)             | postgres-dept/dept_hr_abc              |
| dept-ops-abc                  | 8004     | Ops API (inventory, POs)                | postgres-dept/dept_ops_abc             |
| dept-sales-abc                | 8005     | Sales API (transactions, customers)     | postgres-dept/dept_sales_abc           |
| dept-procurement-abc          | 8006     | Procurement API (vendors, WHT certs)    | postgres-dept/dept_procurement_abc     |
| dept-legal-abc                | 8007     | Legal API (SEC filings, officers)       | postgres-dept/dept_legal_abc           |
| dept-it-audit-abc             | 8008     | IT Audit API (audit log, access events) | postgres-dept/dept_it_audit_abc        |

> XYZ (8011–8018) and RTL (8021–8028) dept services are defined as templates in `docker-compose.xyz.yml` and `docker-compose.retail.yml`. Copy service blocks from `docker-compose.dept.yml` and update `SUBSIDIARY_ID`.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│ OPERATIONAL TIER — postgres-DEPT (:5433)                             │
│ 8 FastAPI services × 3 subsidiaries = 24 containers                 │
│ CRUD Endpoints: POST (INSERT), PATCH (UPDATE), DELETE (soft)         │
│ Databases: dept_finance_abc, dept_tax_xyz, dept_hr_rtl … (24 total) │
│ Audit Columns: created_at, updated_at, is_deleted, deleted_at        │
│ WAL level: logical (set via container CMD flags)                     │
│ Network: lakehouse-net                                               │
└───────────────────────────┬──────────────────────────────────────────┘
                            ↓ PostgreSQL WAL
┌──────────────────────────────────────────────────────────────────────┐
│ CDC LAYER — kafka-connect (both networks)                            │
│ 24 Debezium connectors reading from postgres-dept WAL               │
│ Kafka Topics: abc.finance.journal_entries, xyz.tax.vat_* …          │
│ Events: INSERT (op=c), UPDATE (op=u), DELETE (op=d)                 │
│ CDC Consumer: Debezium envelope → bronze UPSERT + cdc_event_log        │
└───────────────────────────┬──────────────────────────────────────────┘
                            ↓ Kafka → cdc-consumer → UPSERT
┌──────────────────────────────────────────────────────────────────────┐
│ WAREHOUSE TIER — postgres-CENTRAL (:5432) / lakehouse DB            │
│ bronze.*     : CDC upserts (23 tables + cdc_event_log)   [bronze]      │
│ silver.*  : dbt — deduplicated, SCD Type 2 (8 models) [silver]      │
│ gold.*    : KPIs, scorecards, aggregates (5 models)   [gold]        │
│ Also on postgres-central: airflow DB, superset DB, datahub DB       │
│ Network: multi-subsidary-net                                         │
└───────────────────────────┬──────────────────────────────────────────┘
                            ↓ gold.* queries (read-only)
┌──────────────────────────────────────────────────────────────────────┐
│ ANALYTICS — Superset (:8088) · DataHub (:9002) · pgAdmin (:5050)    │
└──────────────────────────────────────────────────────────────────────┘
```

## Data Model Sketch

![img](https://tdhghaslnufgtzjybhhf.supabase.co/storage/v1/object/public/content/Data%20Engineering/PH%20Multi-Subsidiary%20Data%20Lakehouse/Database%20Sketch.png)

---

## Database Layout

### postgres-dept (:5433) — 24 OLTP source databases

| Subsidiary | Databases                                                                                                                                 |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| ABC        | dept_finance_abc · dept_tax_abc · dept_hr_abc · dept_ops_abc · dept_sales_abc · dept_procurement_abc · dept_legal_abc · dept_it_audit_abc |
| XYZ        | dept_finance_xyz · dept_tax_xyz · dept_hr_xyz · dept_ops_xyz · dept_sales_xyz · dept_procurement_xyz · dept_legal_xyz · dept_it_audit_xyz |
| RTL        | dept_finance_rtl · dept_tax_rtl · dept_hr_rtl · dept_ops_rtl · dept_sales_rtl · dept_procurement_rtl · dept_legal_rtl · dept_it_audit_rtl |

Roles on postgres-dept: `abc_dept_user` · `xyz_dept_user` · `rtl_dept_user` · `debezium_user` (REPLICATION)

### postgres-central (:5432) — platform databases

| Database    | Used by                                                                                   |
| ----------- | ----------------------------------------------------------------------------------------- |
| `lakehouse` | cdc-consumer (write bronze._) · airflow-_ DAGs (dbt transforms) · superset (read gold.\*) |
| `airflow`   | airflow-apiserver · airflow-scheduler · airflow-worker · airflow-triggerer                |
| `superset`  | superset (internal metadata — dashboards, charts, users)                                  |
| `datahub`   | datahub-gms (catalog entities, lineage, policies)                                         |

Roles on postgres-central: `superset_reader` · `lakehouse_writer` · `abc_user` · `xyz_user` · `retail_user`

---

## CRUD Endpoints by Department

### Finance

- `POST /journal-entries` → Create JE (generates CDC INSERT)
- `PATCH /journal-entries/{id}` → Update JE (generates CDC UPDATE)
- `DELETE /journal-entries/{id}` → Soft delete (is_deleted=TRUE via CDC UPDATE)
- `POST /ap-invoices` → Create AP invoice
- `PATCH /ap-invoices/{id}` → Update AP invoice

### HR

- `POST /employees` → Create employee (hire)
- `PATCH /employees/{id}` → Update employee (promotion, salary change)

### Sales

- `POST /sales-transactions` → Create sale
- `PATCH /sales-transactions/{id}` → Update sale (discount, status)
- `POST /customers` → Create customer
- `PATCH /customers/{id}` → Update customer

### Procurement

- `POST /vendors` → Create vendor
- `PATCH /vendors/{id}` → Update vendor (accreditation status, terms)

All other departments (Tax, Ops, Legal, IT Audit) provide **read-only GET endpoints**.

---

## Data Models

### Bronze Layer — `bronze.*` (23 tables + cdc_event_log)

| Domain      | Tables                                                    |
| ----------- | --------------------------------------------------------- |
| Finance     | journal_entries · ap_invoices · ar_ledger                 |
| Sales       | sales_transactions · customers · campaigns                |
| HR          | employees · payroll_runs · statutory_contributions        |
| Operations  | inventory_movements · purchase_orders · sales_orders      |
| Procurement | vendors · wht_certificates                                |
| Tax         | vat_returns · wht_filings · bir_filings_log               |
| Legal       | sec_filings · stockholders · board_resolutions · officers |
| IT Audit    | audit_log · access_events · system_incidents              |
| Meta        | subsidiaries · pipeline_run_log · **cdc_event_log**       |

All bronze tables have `is_deleted` and `deleted_at` columns (added by `06_add_soft_delete_columns.sql`).

### Silver Layer — `silver.*` (dbt models)

`fact_sales` · `fact_ap_invoices` · `fact_ar_ledger` · `fact_finance_journal_entries` · `fact_tax_vat_returns` · `fact_inventory_movements` · `dim_employees` · `dim_vendors`

dbt macros: `mask_tin()` (SHA-256 PII masking) · `fiscal_year()` · `ar_aging_bucket()` (0–30 / 31–60 / 61–90 / 90+ days)

### Gold Layer — `gold.*` (dbt marts)

`group_pnl` · `exec_kpi` · `gold_financial_summary` · `gold_hr_headcount` · `gold_inventory_summary`

---

## CDC Data Flow Example

```bash
# 1. POST to dept-finance-abc
POST http://localhost:8001/journal-entries
{"entry_date":"2026-05-11","entry_number":"JE-001","account_code":"1010","debit_amount":50000}

# 2. Inserts into postgres-dept/dept_finance_abc
# 3. Debezium reads WAL from postgres-dept → publishes to Kafka topic abc.finance.journal_entries
# 4. cdc-consumer reads Kafka → UPSERTs bronze.journal_entries on postgres-central
#                             → appends to bronze.cdc_event_log (immutable)
# 5. Airflow DAG runs dbt → silver.fact_finance_journal_entries → gold.exec_kpi
# 6. Superset queries gold layer
```

**Verify each step:**

```bash
# Dept DB (source)
docker exec postgres-dept psql -U postgres -d dept_finance_abc \
  -c "SELECT * FROM journal_entries ORDER BY id DESC LIMIT 1;"

# Kafka topic
docker exec datahub-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic abc.finance.journal_entries --from-beginning --max-messages 1 | jq .

# Central warehouse (bronze)
docker exec postgres-central psql -U postgres -d lakehouse \
  -c "SELECT * FROM bronze.cdc_event_log ORDER BY id DESC LIMIT 1;"

# Central warehouse (gold)
docker exec postgres-central psql -U postgres -d lakehouse \
  -c "SELECT * FROM gold.exec_kpi LIMIT 5;"
```

---

## Airflow DAGs

| DAG                                        | Schedule      | Purpose                                                                |
| ------------------------------------------ | ------------- | ---------------------------------------------------------------------- |
| `lakehouse_bronze_ingestion_and_transform` | Daily @ 1 AM  | Health checks → bronze ingest → dbt silver → dbt gold → quality checks |
| `lakehouse_cdc_monitoring`                 | Every 30 min  | Kafka consumer lag monitoring · alert on backlog                       |
| `lakehouse_dbt_incremental_refresh`        | Manual        | Fast catch-up on high CDC lag · dbt snapshot + incremental             |
| `lakehouse_weekly_reconciliation`          | Monday @ 2 AM | Audit bronze vs silver row counts · compliance report                  |

Airflow uses two DB connections:

- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` → `postgres-central/airflow` (Airflow's own task state)
- `AIRFLOW_CONN_LAKEHOUSE_POSTGRES` → `postgres-central/lakehouse` (DAG queries against the warehouse)

---

## Project Structure

```
.
├── init-scripts/
│   ├── dept/                        → runs on postgres-dept at container init
│   │   ├── 00_create_extensions.sql
│   │   ├── 01_create_dept_databases.sql  (24 dept_* DBs)
│   │   └── 02_create_roles.sql           (dept + debezium roles)
│   └── central/                     → runs on postgres-central at container init
│       ├── 00_create_extensions.sql
│       ├── 01_create_central_databases.sql  (lakehouse/airflow/superset/datahub)
│       ├── 02_create_roles.sql
│       ├── 03_create_schemas.sql           (bronze/silver/gold)
│       ├── 04_grant_permissions.sql
│       ├── 05_create_bronze_tables.sql        (23 bronze.* tables)
│       ├── 06_add_soft_delete_columns.sql  (is_deleted, deleted_at)
│       └── 07_seed_data.sql               (subsidiaries seed)
├── services/
│   ├── dept/                        FastAPI dept microservice (shared image)
│   │   ├── main.py
│   │   ├── database.py              connects to postgres-dept
│   │   ├── schemas.py
│   │   ├── generators/
│   │   ├── routers/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── cdc_consumer/               Kafka → postgres-central ingestion
│       ├── cdc_consumer.py          reads LAKEHOUSE_POSTGRES_DSN
│       ├── entrypoint.sh
│       ├── Dockerfile
│       └── requirements.txt        (fixed: kafka-python)
├── debezium/
│   ├── generate_connectors.py      points connectors at postgres-dept
│   ├── register_connectors.sh      DEBEZIUM_POSTGRES_HOST=postgres-dept
│   └── README.md
├── dbt/
│   ├── models/
│   │   ├── bronze/                    23 staging models
│   │   ├── transformation/         8 fact + dim models
│   │   └── marts/                  5 gold KPI models
│   ├── macros/                     mask_tin · fiscal_year · ar_aging_bucket
│   ├── seeds/                      bir_form_codes.csv
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml                host: postgres-central (fixed)
├── dags/
│   ├── lakehouse_dags.py
│   ├── dag_ingestion.py
│   ├── dag_cdc_monitoring.py
│   ├── dag_dbt_incremental.py
│   ├── dag_reconciliation.py
│   ├── inserts/                    per-dept insert helpers
│   └── shared/                     config · loader · cdc_checks
├── superset/
│   └── superset_config.py
├── bin/
│   ├── BLUEPRINT.MD
│   ├── DDL.txt
│   └── DASHBOARD.txt
├── docker-compose.yml              core services (2 PG instances + all platform)
├── docker-compose.dept.yml         24 dept containers (ABC defined, XYZ/RTL templates)
├── docker-compose.abc.yml          ABC override template
├── docker-compose.xyz.yml          XYZ template (stub — copy from dept.yml)
├── docker-compose.retail.yml       RTL template (stub — copy from dept.yml)
├── .env                            two-instance credentials (see .env.template)
└── Dockerfile.airflow
```

---

## Key Features

✅ **2-Instance PostgreSQL topology** — OLTP source fully isolated from warehouse + metadata

✅ **Production-Grade CRUD** — Type-safe POST/PATCH/DELETE · Pydantic validation · soft deletes · audit columns

✅ **Real CDC via Debezium** — WAL logical replication · zero app impact · Kafka append-only log · immutable cdc_event_log

✅ **Medallion Architecture** — Bronze (bronze CDC) → Silver (dbt deduplicated) → Gold (KPI marts)

✅ **Philippine Compliance** — BIR forms (2550M, 1601-EQ, 1702RT) · SEC filings · SSS/PhilHealth/Pag-IBIG · TIN masking via SHA-256 · EFPS confirmation tracking · penalty auto-calculation

✅ **Audit & Security** — Soft deletes · before/after state in CDC log · subsidiary RLS roles · per-instance access control

---

## Deployment Checklist

- [ ] Fill in `.env` — `POSTGRES_DEPT_PASSWORD`, `POSTGRES_CENTRAL_PASSWORD`, `FERNET_KEY`, `SUPERSET_SECRET_KEY`
- [ ] `docker compose up --build -d` — starts both PG instances + all platform services
- [ ] `docker compose up -d datahub-kafka datahub-zookeeper kafka-connect debezium-ui cdc-consumer`
- [ ] `docker compose -f docker-compose.yml -f docker-compose.dept.yml up --build -d`
- [ ] `bash debezium/register_connectors.sh` — registers 24 Debezium connectors against `postgres-dept`
- [ ] Verify `http://localhost:8001/health` (dept-finance-abc)
- [ ] Verify `http://localhost:8083/connectors` (Debezium connectors list)
- [ ] Trigger `lakehouse_bronze_ingestion_and_transform` in Airflow (`http://localhost:8080`)
- [ ] Wait ~5 minutes for CDC warm-up then verify data in Superset (`http://localhost:8088`)

---
