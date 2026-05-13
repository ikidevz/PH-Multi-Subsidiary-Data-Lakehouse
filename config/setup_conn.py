import os
import subprocess


def add_connection(conn_id: str, command: list[str]) -> None:
    print(f"➕ Adding connection '{conn_id}'...")
    try:
        result = subprocess.run(command, check=True,
                                capture_output=True, text=True)
        print(f"✅ '{conn_id}' added successfully!")
        if result.stdout.strip():
            print(result.stdout.strip())

    except subprocess.CalledProcessError as e:
        stderr = e.stderr or ""
        if "already exists" in stderr:
            print(
                f"⚠️  '{conn_id}' already exists — deleting and recreating...")
            subprocess.run(
                ["airflow", "connections", "delete", conn_id],
                check=True,
                capture_output=True,
            )
            subprocess.run(command, check=True)
            print(f"✅ '{conn_id}' recreated successfully!")
        else:
            print(f"❌ Error adding '{conn_id}': {stderr}")
            raise


if __name__ == "__main__":
    print("=" * 60)
    print("🔌 AIRFLOW CONNECTION SETUP")
    print("=" * 60)

    lakehouse_conn_id = "lakehouse_postgres"

    add_connection(
        conn_id=lakehouse_conn_id,
        command=[
            "airflow", "connections", "add", lakehouse_conn_id,
            "--conn-type",     "postgres",
            "--conn-host",     os.environ.get(
                "POSTGRES_CENTRAL_HOST",     "postgres-central"),
            "--conn-login",    os.environ.get(
                "POSTGRES_CENTRAL_USER",     "postgres"),
            "--conn-password", os.environ.get("POSTGRES_CENTRAL_PASSWORD", ""),
            "--conn-schema",   os.environ.get("POSTGRES_DB",
                                              "lakehouse"),
            "--conn-port",     os.environ.get(
                "POSTGRES_CENTRAL_PORT",     "5432"),
        ],
    )

    print("=" * 60)
    print("✅ All connections registered.")
    print("=" * 60)
