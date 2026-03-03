from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


FILE_PATH = "/data/incoming/Airline_Dataset.csv"


def run_sql_with_log(hook, sql, process_name):
    """
    logging affected rows and running sql script

    :param hook: hook of current task
    :param sql: run sql script with logging
    :param process_name: task name
    :return:
    """
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute(sql)

        rows = cur.rowcount

        cur.execute("""
            INSERT INTO audit.etl_log
            (process_name, rows_affected, status, run_ts)
            VALUES (%s, %s, 'SUCCESS', CURRENT_TIMESTAMP())
        """, (process_name, rows))

        conn.commit()

    except Exception:
        cur.execute("""
            INSERT INTO audit.etl_log
            (process_name, rows_affected, status, run_ts)
            VALUES (%s, 0, 'FAILED', CURRENT_TIMESTAMP())
        """, (process_name,))
        conn.commit()
        raise

    finally:
        cur.close()
        conn.close()



def check_if_empty():
    """
    rechecking existing of file and checking if it is empty

    :return: next task after branch
    """
    if not os.path.exists(FILE_PATH) or os.path.getsize(FILE_PATH) == 0:
        return "file_empty"
    return "loading_file"



def loading_file():
    """
    1-1 loading file from PATH directory into the bronze layer of DWH and replacing this file into archive

    :return:
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    file_name = os.path.basename(FILE_PATH)

    hook.run(f"""
        PUT file://{FILE_PATH}
        @stage.flight_stage
        OVERWRITE=TRUE
        AUTO_COMPRESS=TRUE;
    """)

    copy_sql = """
        COPY INTO stage.stg_flights_raw
        FROM (
            SELECT
                $2,$3,$4,$5,$6,$7,
                $8,$9,$10,$11,$12,
                $13,$14,$15,$16,$17,$18,
                CURRENT_TIMESTAMP()
            FROM @stage.flight_stage
        )
        FILE_FORMAT = (FORMAT_NAME = stage.csv_format)
        PATTERN='.*\\.csv\\.gz'
        ON_ERROR = ABORT_STATEMENT
    """

    run_sql_with_log(hook, copy_sql, "stage_load")

    archive_path = f"/data/archive/{file_name}"
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    os.rename(FILE_PATH, archive_path)



def transforming_data():
    """
    transform data and replacing it into the silver layer of DWH, cleaning bronze layer

    :return:
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    insert_sql = """
        INSERT OVERWRITE INTO transform.clean_flights
        SELECT
            NULLIF(TRIM(passenger_id), ''),
            INITCAP(TRIM(first_name)),
            INITCAP(TRIM(last_name)),
            UPPER(TRIM(gender)),
            TRY_TO_NUMBER(age),
            INITCAP(TRIM(nationality)),
            INITCAP(TRIM(airport_name)),
            UPPER(TRIM(airport_country_code)),
            INITCAP(TRIM(country_name)),
            INITCAP(TRIM(airport_continent)),
            TRY_TO_DATE(departure_date,'MM/DD/YYYY'),
            UPPER(TRIM(arrival_airport)),
            INITCAP(TRIM(pilot_name)),
            UPPER(TRIM(flight_status)),
            UPPER(TRIM(ticket_type)),
            UPPER(TRIM(passenger_status)),
            load_ts
        FROM stage.stg_flights_raw
    """

    run_sql_with_log(hook, insert_sql, "transform_clean")

    hook.run("TRUNCATE TABLE stage.stg_flights_raw")




def data_separating():
    """
    creating data marts and fact tables into the gold layer of DWH

    :return:
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    airport_sql = """
        MERGE INTO mart.dim_airport d
        USING (
            SELECT DISTINCT airport_name,country_name,continent,airport_country_code
            FROM transform.clean_flights
        ) s
        ON d.airport_name=s.airport_name
        AND d.airport_country_code=s.airport_country_code
        WHEN NOT MATCHED THEN
            INSERT (airport_name,country_name,continent,airport_country_code)
            VALUES (s.airport_name,s.country_name,s.continent,s.airport_country_code)
    """

    passenger_sql = """
        MERGE INTO mart.dim_passenger d
        USING (
            SELECT DISTINCT passenger_id,first_name,last_name,gender,age,nationality
            FROM transform.clean_flights
        ) s
        ON d.passenger_id=s.passenger_id
        WHEN NOT MATCHED THEN
            INSERT VALUES (DEFAULT,s.passenger_id,s.first_name,s.last_name,s.gender,s.age,s.nationality)
    """

    pilot_sql = """
        MERGE INTO mart.dim_pilot d
        USING (SELECT DISTINCT pilot_name FROM transform.clean_flights) s
        ON d.pilot_name=s.pilot_name
        WHEN NOT MATCHED THEN INSERT (pilot_name) VALUES (s.pilot_name)
    """

    fact_sql = """
        INSERT INTO mart.fact_flights (
            passenger_key,airport_key,pilot_key,
            ticket_type,flight_status,passenger_status,
            flight_cnt,load_ts
        )
        SELECT
            dp.passenger_key,
            da.airport_key,
            dpl.pilot_key,
            c.ticket_type,
            c.flight_status,
            c.passenger_status,
            1,
            c.load_ts
        FROM transform.clean_flights c
        JOIN mart.dim_passenger dp ON dp.passenger_id=c.passenger_id
        JOIN mart.dim_airport da ON da.airport_name=c.airport_name
            AND da.airport_country_code=c.airport_country_code
        JOIN mart.dim_pilot dpl ON dpl.pilot_name=c.pilot_name
    """

    run_sql_with_log(hook, airport_sql, "mart_dim_airport")
    run_sql_with_log(hook, passenger_sql, "mart_dim_passenger")
    run_sql_with_log(hook, pilot_sql, "mart_dim_pilot")
    run_sql_with_log(hook, fact_sql, "mart_fact_flights")


with DAG(
    dag_id="file_processing_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_PATH,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    branch = BranchPythonOperator(
        task_id="branch_on_file",
        python_callable=check_if_empty,
    )

    file_empty = BashOperator(
        task_id="file_empty",
        bash_command='echo "No file"',
    )

    loading = PythonOperator(
        task_id="loading_file",
        python_callable=loading_file
    )

    cleaning = PythonOperator(
        task_id="cleaning_data",
        python_callable=transforming_data
    )

    marts = PythonOperator(
        task_id="data_separating",
        python_callable=data_separating
    )

    wait_for_file >> branch
    branch >> file_empty
    branch >> loading
    loading >> cleaning
    cleaning >> marts