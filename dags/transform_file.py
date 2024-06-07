import os
import pandas as pd

from datetime import datetime
from airflow import Dataset
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import dag, task, task_group
from dotenv import load_dotenv

load_dotenv()
FILEPATH = os.getenv("FILEPATH")
DATASET = Dataset(FILEPATH)


@dag(
    dag_id="transform_file",
    schedule="@daily",
    description="Extract file from folder and transform it",
    start_date=datetime(2024, 1, 1),
    tags=["python", "pandas"],
    catchup=False,
)
def schedule_extract_transform() -> None:
    sensor_task = FileSensor(
        task_id="file_sensor_task",
        filepath="tiktok_google_play_reviews.csv",
        fs_conn_id="file_system",
    )

    @task.bash
    def process_empty_file() -> str:
        """
        Writes to the console that the file is empty.
        """
        return f'echo "File is empty"'

    @task(task_id="fill_null")
    def fill_null(ti) -> None:
        """
        Replaces null value to the - and writes to csv file.
        """
        df = pd.read_csv(FILEPATH)
        df = df.fillna("-").to_json()
        ti.to_csv(FILEPATH, index=False)

    @task
    def sort_data_by_at(ti) -> None:
        """
        Sorts data by created_date and writes to csv file.
        """
        df = pd.read_csv(FILEPATH)
        df["at"] = pd.to_datetime(df["at"])
        df = df.sort_values(by="at")
        df["at"] = (
            df["at"]
            .apply(lambda x: datetime.datetime.fromisoformat(x).timestamp())
            .to_json()
        )
        ti.to_csv(FILEPATH, index=False)

    @task(outlets=[DATASET])
    def delete_useless_symbols(ti) -> None:
        """
        Removes all unnecessary characters from the content column and saves to csv file.
        """
        df = pd.read_csv(FILEPATH)
        df["content"] = df["content"].replace(
            r'[^\w\s\,.?!+:;"*()]', "", regex=True)
        df.to_csv(FILEPATH, index=False)

    @task_group(group_id="transform_group")
    def transform_group() -> None:
        """
        File transformation group.
        """
        fill_null() >> sort_data_by_at() >> delete_useless_symbols()

    @task.branch
    def check_if_file_is_empty() -> None:
        """
        Transform file if it's not empty,
        otherwise trigger process_empty_file.
        """
        if os.path.exists(FILEPATH) and os.stat(FILEPATH).st_size > 0:
            return "transform_group.fill_null"
        else:
            return "process_empty_file"

    (
        sensor_task
        >> check_if_file_is_empty()
        >> [transform_group(), process_empty_file()]
    )


schedule_extract_transform()
