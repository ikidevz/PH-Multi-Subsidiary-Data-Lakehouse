"""
Kafka CDC Consumer - Process Debezium CDC events from Kafka
Writes CDC events to bronze.cdc_event_log (immutable audit trail)
Upserts data into appropriate bronze.* tables (INSERT/UPDATE/DELETE handling)
"""

import json
import os
import re
import logging
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
# FIX 1: Use LAKEHOUSE_POSTGRES_DSN — correct env var passed by docker-compose.
#         AIRFLOW_CONN_LAKEHOUSE_POSTGRES uses postgresql+psycopg2:// prefix
#         which psycopg2.connect() does not accept.
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "datahub-kafka:9092")  # FIX 5
POSTGRES_DSN = os.environ.get("LAKEHOUSE_POSTGRES_DSN",
                              "postgresql://postgres:changeme@postgres-central:5432/lakehouse")
CONSUMER_GROUP = "lakehouse-cdc-consumer"

# FIX 2: Case-insensitive pattern, driven by env var so new subsidiaries
#         don't require a code change.
KAFKA_TOPIC_PATTERN = os.environ.get(
    "KAFKA_TOPIC_PATTERN",
    r"(?i)^(abc|xyz|rtl)\..+"
)

# Map CDC operation codes to readable names
OPERATION_MAP = {
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
    "r": "SNAPSHOT",
}

# FIX 3: Whitelist regex for safe identifier names (table names, column names).
#         Only alphanumeric + underscore, must start with letter or underscore.
_SAFE_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _safe_id(name: str) -> str:
    """
    Validate a SQL identifier (table or column name) against a whitelist.
    Raises ValueError if the name contains anything outside [a-zA-Z0-9_].
    """
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Unsafe SQL identifier rejected: {name!r}")
    return name


class CDCConsumer:
    """Kafka CDC consumer for PH Conglomerate lakehouse"""

    def __init__(self):
        """Initialize Kafka consumer and PostgreSQL connection"""
        self.consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=100,
            session_timeout_ms=30000,
        )

        self.consumer.subscribe(pattern=KAFKA_TOPIC_PATTERN)
        logger.info(f"Consumer subscribed to pattern: {KAFKA_TOPIC_PATTERN}")

        # FIX 4: Store DSN so reconnect() can reuse it.
        self._dsn = POSTGRES_DSN
        self.conn = self._new_conn()

    # ------------------------------------------------------------------
    # FIX 4: Reconnection helpers
    # ------------------------------------------------------------------

    def _new_conn(self):
        """Open a fresh psycopg2 connection."""
        conn = psycopg2.connect(self._dsn)
        conn.autocommit = False
        logger.info("PostgreSQL connection established")
        return conn

    def _get_conn(self):
        """
        Return a live connection, reconnecting transparently if the
        existing one has been closed or lost (e.g. Postgres restart,
        idle-in-transaction timeout, TCP keepalive expiry).
        """
        try:
            # psycopg2 sets conn.closed > 0 on a broken connection.
            if self.conn.closed:
                raise psycopg2.OperationalError("connection is closed")
            # A lightweight round-trip to verify the connection is alive.
            self.conn.cursor().execute("SELECT 1")
        except Exception as exc:
            logger.warning(f"Postgres connection lost ({exc}), reconnecting…")
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = self._new_conn()
        return self.conn

    # ------------------------------------------------------------------
    # CDC helpers
    # ------------------------------------------------------------------

    def extract_cdc_fields(self, message):
        """Extract CDC fields from Debezium envelope"""
        payload = message.value
        op = payload.get("op", "c")
        operation = OPERATION_MAP.get(op, "INSERT")
        before = payload.get("before")
        after = payload.get("after")
        ts_ms = payload.get("ts_ms", 0)
        event_ts = datetime.utcfromtimestamp(ts_ms / 1000)
        record_id = str((after or before or {}).get("id", ""))

        return {
            "operation":  operation,
            "before":     before,
            "after":      after,
            "event_ts":   event_ts,
            "record_id":  record_id,
            "topic":      message.topic,
            "offset":     message.offset,
            "partition":  message.partition,
        }

    def parse_topic(self, topic):
        """
        Parse Kafka topic into (subsidiary_id, dept, table_name).

        FIX 2 + FIX 3: Converts subsidiary prefix to uppercase so it
        matches regardless of how Debezium emits the server name, and
        validates the table_name component against the safe-identifier
        whitelist before it ever reaches SQL.
        """
        parts = topic.split(".")
        if len(parts) < 3:
            return None, None, None

        subsidiary_id = parts[0].upper()
        dept = parts[1]
        table_name = ".".join(parts[2:])  # handles multi-part names

        # FIX 3: Reject topics whose table segment contains unsafe characters.
        # Multi-part names (schema.table) are validated part-by-part.
        try:
            for segment in table_name.split("."):
                _safe_id(segment)
        except ValueError as exc:
            logger.warning(f"Rejected topic with unsafe table name — {exc}")
            return None, None, None

        return subsidiary_id, dept, table_name

    def write_to_cdc_log(self, cur, cdc_fields, topic, sub_id, dept, table_name):
        """Write CDC event to bronze.cdc_event_log (immutable audit trail)"""
        insert_sql = """
            INSERT INTO bronze.cdc_event_log
                (kafka_topic, kafka_offset, kafka_partition, subsidiary_id,
                 source_dept, source_table, operation, record_id,
                 before_state, after_state, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """
        try:
            cur.execute(insert_sql, (
                topic,
                cdc_fields["offset"],
                cdc_fields["partition"],
                sub_id,
                dept,
                table_name,
                cdc_fields["operation"],
                cdc_fields["record_id"],
                json.dumps(cdc_fields["before"]),
                json.dumps(cdc_fields["after"]),
                cdc_fields["event_ts"],
            ))
            logger.debug(
                f"CDC event logged: {topic} offset={cdc_fields['offset']}")
            return True
        except Exception as e:
            logger.error(f"Error writing to cdc_event_log: {e}")
            return False

    def upsert_bronze_table(self, cur, bronze_table, cdc_fields, sub_id, operation):
        """
        Upsert data into the appropriate bronze.* table.
        Handles INSERT / UPDATE / SNAPSHOT / DELETE operations.

        FIX 3: Column names from the Debezium payload are validated against
        the safe-identifier whitelist before being interpolated into SQL.
        The table name was already validated in parse_topic().
        """
        before = cdc_fields["before"]
        after = cdc_fields["after"]

        try:
            if operation in ("INSERT", "UPDATE", "SNAPSHOT") and after:
                after.update({
                    "subsidiary_id": sub_id,
                    "cdc_operation": operation,
                    "ingested_at":   datetime.utcnow().isoformat(),
                })

                # FIX 3: Validate every column name before touching SQL.
                cols = list(after.keys())
                try:
                    for col in cols:
                        _safe_id(col)
                except ValueError as exc:
                    logger.error(
                        f"Rejected upsert — unsafe column name: {exc}")
                    return False

                vals = [after[c] for c in cols]
                placeholders = ", ".join(["%s"] * len(cols))
                col_list = ", ".join(cols)
                update_cols = ", ".join(
                    f"{c} = EXCLUDED.{c}" for c in cols
                    if c not in ("id", "subsidiary_id", "ingested_at")
                )

                upsert_sql = (
                    f"INSERT INTO {bronze_table} ({col_list}) "
                    f"VALUES ({placeholders}) "
                    f"ON CONFLICT (subsidiary_id, id) DO UPDATE SET {update_cols}"
                )

                cur.execute(upsert_sql, vals)
                logger.debug(
                    f"Upserted {operation}: {bronze_table} id={after.get('id')}")

            elif operation == "DELETE" and before:
                # Soft delete: mark the row rather than physically removing it.
                delete_sql = (
                    f"UPDATE {bronze_table} "
                    f"SET is_deleted = TRUE, deleted_at = NOW() "
                    f"WHERE subsidiary_id = %s AND id = %s"
                )
                cur.execute(delete_sql, (sub_id, before["id"]))
                logger.debug(
                    f"Soft-deleted: {bronze_table} id={before.get('id')}")

            return True

        except Exception as e:
            logger.error(f"Error upserting into {bronze_table}: {e}")
            return False

    def process_message(self, message):
        """Process a single CDC message"""
        try:
            topic = message.topic
            sub_id, dept, table_name = self.parse_topic(topic)

            if not all([sub_id, dept, table_name]):
                logger.warning(f"Invalid or unsafe topic format: {topic}")
                return False

            bronze_table = f"bronze.{table_name}"
            cdc_fields = self.extract_cdc_fields(message)

            # FIX 4: Always obtain a live connection before starting the tx.
            conn = self._get_conn()

            with conn.cursor() as cur:
                if not self.write_to_cdc_log(cur, cdc_fields, topic, sub_id, dept, table_name):
                    conn.rollback()
                    return False

                if not self.upsert_bronze_table(cur, bronze_table, cdc_fields, sub_id, cdc_fields["operation"]):
                    conn.rollback()
                    return False

                conn.commit()

            logger.info(
                f"Processed: {topic} op={cdc_fields['operation']} offset={message.offset}"
            )
            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            return False

    def run(self):
        """Main consumer loop"""
        logger.info(f"Starting CDC consumer: {CONSUMER_GROUP}")

        try:
            for message in self.consumer:
                if self.process_message(message):
                    self.consumer.commit()
                else:
                    logger.warning(
                        f"Failed to process message, skipping offset={message.offset}"
                    )
                    # Do NOT commit — Kafka will re-deliver on next poll.

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error in consumer: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        try:
            self.consumer.close()
            self.conn.close()
            logger.info("CDC consumer shutdown complete")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


if __name__ == "__main__":
    consumer = CDCConsumer()
    consumer.run()
