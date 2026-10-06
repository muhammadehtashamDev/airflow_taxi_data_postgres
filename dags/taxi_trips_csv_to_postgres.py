from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from pathlib import Path
import hashlib

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping


INCOMING_DIR = Path(
    "/opt/airflow/data/taxi_data/incoming"
)

POSTGRES_CONN_ID = "postgres_default"

def calculate_checksum(file_path: Path) -> str:
    """
    Calculate SHA-256 checksum for a file.

    Reads the file in 1 MB chunks so large CSV files
    do not need to be loaded completely into memory.
    """
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:

        while chunk := file.read(1024 * 1024):
            sha256.update(chunk)

    return sha256.hexdigest()


@dag(
    dag_id="taxi_trips_csv_to_postgres_raw",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["taxi", "csv", "postgres", "raw"],
)
def taxi_trips_csv_to_postgres():

    @task
    def ingest_csv_files():

        csv_files = sorted(INCOMING_DIR.glob("*.csv"))

        if not csv_files:
            print(f"No CSV files found in {INCOMING_DIR}")
            return

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:

                for csv_file in csv_files:

                    print(f"Checking file: {csv_file.name}")

                    # -------------------------------------------------
                    # Calculate file checksum
                    # -------------------------------------------------

                    checksum = calculate_checksum(csv_file)

                    file_size = csv_file.stat().st_size

                    print(f"Checksum: {checksum}")

                    print(f"File size: {file_size} bytes")

                    # -------------------------------------------------
                    # Check whether this exact file was already loaded
                    # -------------------------------------------------

                    cursor.execute(
                        """
                        SELECT
                            file_checksum
                        FROM raw.ingested_files
                        WHERE file_name = %s
                            AND status = 'SUCCESS'
                        """,
                        (csv_file.name,)
                    )

                    existing = cursor.fetchone()

                    if existing:
                    
                        existing_checksum = existing[0]

                        if existing_checksum == checksum:

                            print(
                                f"Skipping already ingested file: "
                                f"{csv_file.name}"
                            )

                            continue

                        else:

                            print(f"File has changed: {csv_file.name}")
                            print(f"Old checksum: {existing_checksum}")
                            print(f"New checksum: {checksum}")

                            # -------------------------------------------------
                            # IMPORTANT:
                            # The existing raw data is NOT deleted here.
                            #
                            # The current implementation will load the
                            # changed file as additional data.
                            #
                            # If you want replacement/reload behavior,
                            # we can add that separately.
                            # -------------------------------------------------

                    # -------------------------------------------------
                    # Load CSV into raw table
                    # -------------------------------------------------

                    print(f"Loading: {csv_file}")

                    with csv_file.open("r", encoding="utf-8") as file:

                        with cursor.copy(
                            """
                            COPY raw.taxi_trips
                            FROM STDIN
                            WITH (
                                FORMAT CSV,
                                HEADER TRUE
                            )
                            """
                        ) as copy:

                            while data := file.read(1024 * 1024):
                                copy.write(data)

                    # -------------------------------------------------
                    # Record successful ingestion
                    # -------------------------------------------------

                    cursor.execute(
                        """
                        INSERT INTO raw.ingested_files (
                            file_name,
                            file_checksum,
                            file_size_bytes,
                            status
                        )
                        VALUES (
                            %s,
                            %s,
                            %s,
                            'SUCCESS'
                        )
                        ON CONFLICT (file_name)
                        DO UPDATE SET
                            file_checksum = EXCLUDED.file_checksum,
                            file_size_bytes = EXCLUDED.file_size_bytes,
                            ingested_at = CURRENT_TIMESTAMP,
                            status = 'SUCCESS'
                        """,
                        (
                            csv_file.name,
                            checksum,
                            file_size,
                        )
                    )

                    print(
                        f"Successfully loaded: {csv_file.name}"
                    )

            conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()

    ingest = ingest_csv_files()

    # ===========================================

    dbt = DbtTaskGroup(
        group_id="dbt_transformations",

        project_config=ProjectConfig(
            "/opt/airflow/dbt/dbt_taxi_data_postgres"
        ),

        profile_config=ProfileConfig(
            profile_name="dbt_taxi_data_postgres",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_default",
                profile_args={
                    "schema": "public"
                },
            ),
        ),

        operator_args={
            "exclude": "dim_dates"
        },
    )

    ingest >> dbt


taxi_trips_csv_to_postgres()