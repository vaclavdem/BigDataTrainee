from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from datetime import datetime
import os
import pandas as pd
import re

FILE_PATH = "/data/incoming/tiktok_google_play_reviews.csv"
IN_PROGRESS_PATH = "/data/in_progress/temp_processing.csv"
PROCESSED_PATH = "/data/processed/data_processed.csv"

processed_data_file = Dataset("/data/processed/data_processed.csv")


def check_if_empty():
    """
    checking existing of file

    :return: next task
    """
    if not os.path.exists(FILE_PATH):
        return "file_empty"
    return "processing.replace_nulls"


def replace_nulls():
    """
    Replacing null values with "-"

    :return: saving processed df
    """
    df = pd.read_csv(FILE_PATH)
    df = df.fillna("-")
    df = df.replace(["null", "Null", "NULL"], "-")
    os.makedirs(os.path.dirname(IN_PROGRESS_PATH), exist_ok=True)
    df.to_csv(IN_PROGRESS_PATH, index=False)


def sort_by_date():
    """
    sorting by date

    :return: saving processed df
    """
    df = pd.read_csv(IN_PROGRESS_PATH)

    target_col = 'at'
    if target_col not in df.columns:
        raise KeyError(f"Колонка '{target_col}' не найдена. Доступные: {list(df.columns)}")

    df[target_col] = pd.to_datetime(df[target_col], errors='coerce')
    df = df.sort_values(by=target_col, ascending=True)

    df.to_csv(IN_PROGRESS_PATH, index=False)


def clean_text(text):
    """
    cleaning text

    :param text: text
    :return: cleaned text
    """
    if pd.isna(text):
        return "-"
    return re.sub(r"[^a-zA-Z0-9\s.,!?;:'\"()-]", "", str(text))


def clean_content():
    """
    cleaning content

    :return: saving processed df
    """
    df = pd.read_csv(IN_PROGRESS_PATH)

    if 'content' in df.columns:
        df['content'] = df['content'].apply(clean_text)

    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)


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
        timeout=60 * 5,
        mode="reschedule",
    )

    branch = BranchPythonOperator(
        task_id="branch_on_file",
        python_callable=check_if_empty,
    )

    file_empty = BashOperator(
        task_id="file_empty",
        bash_command='echo "FILE_LOG: The file is empty or missing. Skipping..."',
    )

    with TaskGroup("processing") as processing_group:
        replace_nulls_task = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_nulls,
        )

        sort_task = PythonOperator(
            task_id="sort_by_date",
            python_callable=sort_by_date,
        )

        clean_task = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
            outlets=[processed_data_file]
        )

        replace_nulls_task >> sort_task >> clean_task

    wait_for_file >> branch
    branch >> file_empty
    branch >> processing_group