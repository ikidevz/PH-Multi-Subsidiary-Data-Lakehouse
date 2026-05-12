"""
Kafka CDC Consumer - Process Debezium CDC events from Kafka
Writes CDC events to bronze.cdc_event_log (immutable audit trail)
Upserts data into appropriate bronze.* tables (INSERT/UPDATE/DELETE handling)

BLUEPRINT.MD v3.0 — CDC Architecture
"""

import json
import os
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
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_DSN = os.environ.get("AIRFLOW_CONN_LAKEHOUSE_POSTGRES",
                              "postgresql://airflow:changeme@postgres-central:5432/lakehouse")
CONSUMER_GROUP = "lakehouse-cdc-consumer"
KAFKA_TOPIC_PATTERN = r"^(abc|xyz|rtl)\..*"  # Subsidiary pattern

# Map CDC operation codes to readable names
OPERATION_MAP = {
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
    "r": "SNAPSHOT"
}


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

        # Subscribe to all topics matching pattern
        self.consumer.subscribe(pattern=KAFKA_TOPIC_PATTERN)
        logger.info(f"Consumer subscribed to pattern: {KAFKA_TOPIC_PATTERN}")

        # PostgreSQL connection
        self.conn = psycopg2.connect(POSTGRES_DSN)
        self.conn.autocommit = False
        logger.info("PostgreSQL connection established")

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
            "operation": operation,
            "before": before,
            "after": after,
            "event_ts": event_ts,
            "record_id": record_id,
            "topic": message.topic,
            "offset": message.offset,
            "partition": message.partition,
        }

    def parse_topic(self, topic):
        """Parse Kafka topic into subsidiary_id, dept, table_name"""
        parts = topic.split(".")
        if len(parts) >= 3:
            subsidiary_id = parts[0].upper()
            dept = parts[1]
            table_name = ".".join(parts[2:])  # Handle multi-part table names
            return subsidiary_id, dept, table_name
        return None, None, None

    def write_to_cdc_log(self, cur, cdc_fields, topic, sub_id, dept, table_name):
        """
        Write CDC event to bronze.cdc_event_log (immutable audit trail)
        """
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
        Upsert data into appropriate bronze.* table
        Handles INSERT/UPDATE/DELETE operations
        """
        before = cdc_fields["before"]
        after = cdc_fields["after"]

        try:
            if operation in ("INSERT", "UPDATE", "SNAPSHOT") and after:
                # Add audit columns
                after.update({
                    "subsidiary_id": sub_id,
                    "cdc_operation": operation,
                    "ingested_at": datetime.utcnow().isoformat()
                })

                cols = list(after.keys())
                vals = [after[c] for c in cols]
                placeholders = ",".join(["%s"] * len(cols))

                # Build UPSERT (INSERT ... ON CONFLICT ... DO UPDATE)
                col_list = ",".join(cols)
                update_cols = ",".join(
                    f"{c}=EXCLUDED.{c}" for c in cols
                    if c not in ("id", "subsidiary_id", "ingested_at")
                )

                upsert_sql = f"""
                    INSERT INTO {bronze_table} ({col_list})
                    VALUES ({placeholders})
                    ON CONFLICT (subsidiary_id, id) DO UPDATE SET {update_cols}
                """

                cur.execute(upsert_sql, vals)
                logger.debug(
                    f"Upserted {operation}: {bronze_table} id={after.get('id')}")

            elif operation == "DELETE" and before:
                # Soft delete: mark row as deleted
                delete_sql = f"""
                    UPDATE {bronze_table}
                    SET is_deleted = TRUE, deleted_at = NOW()
                    WHERE subsidiary_id = %s AND id = %s
                """

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
                logger.warning(f"Invalid topic format: {topic}")
                return False

            bronze_table = f"bronze.{table_name}"
            cdc_fields = self.extract_cdc_fields(message)

            with self.conn.cursor() as cur:
                # Write to immutable CDC log
                if not self.write_to_cdc_log(cur, cdc_fields, topic, sub_id, dept, table_name):
                    return False

                # Upsert into bronze table
                if not self.upsert_bronze_table(cur, bronze_table, cdc_fields, sub_id, cdc_fields["operation"]):
                    return False

                self.conn.commit()

            logger.info(
                f"Processed: {topic} op={cdc_fields['operation']} offset={message.offset}")
            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.conn.rollback()
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
                        f"Failed to process message, offset={message.offset}")
                    # Don't commit on failure — message will be retried

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
