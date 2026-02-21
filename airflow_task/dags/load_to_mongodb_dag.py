from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
from datetime import datetime

processed_data_file = Dataset("/data/processed/data_processed.csv")


def load_to_mongo():
    """
    loading data into mongodb

    :return: loading data
    """
    df = pd.read_csv("/data/processed/data_processed.csv")

    df['at'] = pd.to_datetime(df['at'])

    hook = MongoHook(conn_id='mongo_default')
    client = hook.get_conn()
    db = client['tiktok_reviews_db']
    collection = db['reviews']

    collection.delete_many({})
    records = df.to_dict('records')
    collection.insert_many(records)


with DAG(
        dag_id="load_to_mongodb_dag",
        start_date=datetime(2024, 1, 1),
        schedule=[processed_data_file],  # Триггер по изменению датасета
        catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load_csv_to_mongo",
        python_callable=load_to_mongo
    )