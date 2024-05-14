from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import boto3
from datetime import datetime
import pandas as pd
import requests
import csv

dag = DAG(
    dag_id="data_proceess_ready",
    start_date=datetime(2024, 5, 8),
    end_date=datetime(2024, 5, 10),
    schedule_interval="@daily",
)


def _get_data_from_openapi(day, next_day,apiKey):
    startDate = f"{day}T0000"
    endDate = f"{next_day}T000"

    selected_users = pd.read_csv(
        "./사용_파일.csv"
    )  # 이 부분을 sql로 바꿔야 한다.
    userInfoList = selected_users[["characterId", "serverId"]].values.tolist()

    save_list = []

    for characterId, serverId in userInfoList:
        url = f"https://api.neople.co.kr/df/servers/{serverId}/characters/{characterId}/timeline?startDate={startDate}&endDate={endDate}&apikey={apiKey}"
        res = requests.get(url=url).json()
        save_list.append(res)

    header = [
        "characterId",
        "characterName",
        "level",
        "jobId",
        "jobGrowId",
        "jobName",
        "jobGrowName",
        "adventureName",
        "guildId",
        "guildName",
        "timeline",
    ]
    with open(f"./users_timeline_{day}_{next_day}.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in save_list:
            if "error" in row:
                continue
            writer.writerow(row)


def _convert_csv_to_parquet(day, next_day, **kwargs):
    df = pd.read_csv("./users_timeline_{day}_{next_day}.csv")
    parquet_file = f"./users_timeline_{day}_{next_day}.parquet"
    df.to_parquet(parquet_file)
    kwargs["ti"].xcom_push(key="parquet_file_path", value=parquet_file)


def _upload_s3_files(file_name, dest_name, **kwargs):

    s3_client = boto3.client(
        "s3",
        aws_access_key_id="",
        aws_secret_access_key="",
    )
    file_name = kwargs["ti"].xcom_pull(key="parquet_file_path", task_id="converting")

    response = s3_client.upload_file(file_name, "버킷 이름", dest_name)


get_data = PythonOperator(
    task_id="get_data_from_openapi",
    python_callable=_get_data_from_openapi,
    op_kwargs={"day": "{{ ds }}", "next_day": "{{ data_interval_end | ds}}",'apiKey':'apiKey'},
    dag=dag,
)

convert_csv_to_parquet = PythonOperator(
    task_id="converting",
    python_callable=_convert_csv_to_parquet,
    op_kwargs={"day": "{{ ds }}", "next_day": "{{ data_interval_end | ds}}"},
    dag=dag,
)

upload_data = PythonOperator(
    task_id="upload_parquet", python_callable=_upload_s3_files, dag=dag
)


mailling = EmailOperator(
    task_id="emailling",
    to="대상 이메일",
    subject="{{ds}} 작업 완료했습니다..",
    html_content="""
                        {{ ds }} 작업 종료
                    """,
    dag=dag,
)


get_data >> convert_csv_to_parquet >> upload_data >> mailling
