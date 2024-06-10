import os
import pandas as pd

from datetime import datetime
from airflow import Dataset, DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task
from dotenv import load_dotenv

load_dotenv()

FILEPATH = os.getenv("FILEPATH", "not found")
DATASET = Dataset(FILEPATH)

with DAG(
    dag_id="load_to_mongo",
    schedule=[DATASET],
    description="Load file to mongodb",
    start_date=datetime(2024, 1, 1),
    tags=["python", "mongo"],
    catchup=False,
):

    @task
    def load_to_mongo() -> None:
        """
        Connect to mongodb through connection in airflow.
        Than load data to collection in mongodb.
        """
        df = pd.read_csv(DATASET.uri)
        df.reset_index(inplace=True)
        data_dict = df.to_dict("records")

        hook = MongoHook(mongo_conn_id="mongo_connection")
        client = hook.get_conn()
        db = client.airflow
        collection = db.comments
        collection.insert_many(data_dict)

    load_to_mongo()
