# Airflow를 이용한 데이터 파이프라인 작성

## 앞서서
- 해당 작업은 필요한 태스크를 하나 하나 만들어 가는 과정입니다.


## 1. 완성된 워크플로

- 데이터 호출 및 저장
- 데이터를 데이터베이스에 저장
- 데이터를 원하는 작업 실행


## 2. 도식

![poster](./project_drawio.png)

## 3. 해당 프로젝트에 포함된 요소

- 점진적 데이터 처리
- xcom
- 외부 시스템과의 연결


## 4. Airflow 테스트 환경 만들기

- 필요 패키지 설치
```
sudo apt update -y
sudo apt install python3-venv
python3 -m venv venv
source venv/bin/activate
pip install apache-airflow
pip install boto3
```
- db 초기화 및 관리자 만들기

```
airflow db init 
airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.org
```

- airflow 실행

```
airflow webserver
airflow scheduler
# airflow standalone
```



## 5. 워크플로 설명

### 0) dag 설정

```
dag = DAG(
    dag_id="data_proceess_ready",
    start_date=datetime(2024, 5, 8),
    end_date=datetime(2024, 5, 10),
    schedule_interval="@daily",
)
```
  
- 현재는 테스트 중으로, 변경 소지가 많습니다. 

### 1) 데이터 호출
```
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

get_data = PythonOperator(
    task_id="get_data_from_openapi",
    python_callable=_get_data_from_openapi,
    op_kwargs={"day": "{{ ds }}", "next_day": "{{ data_interval_end | ds}}",'apiKey':'apiKey'},
    dag=dag,
)
```

- 템플릿을 사용하는 것으로 멱등성, 점진적 데이터 처리 가능
- 현재 postgres 서버 작업이 아직 되지 않아서, 필요 데이터를 csv 파일로 읽고 있음, 차후 변경 예정


### 2)  데이터 파일 타입 변경

```
def _convert_csv_to_parquet(day, next_day, **kwargs):
    df = pd.read_csv("./users_timeline_{day}_{next_day}.csv")
    parquet_file = f"./users_timeline_{day}_{next_day}.parquet"
    df.to_parquet(parquet_file)
    kwargs["ti"].xcom_push(key="parquet_file_path", value=parquet_file)

convert_csv_to_parquet = PythonOperator(
    task_id="converting",
    python_callable=_convert_csv_to_parquet,
    op_kwargs={"day": "{{ ds }}", "next_day": "{{ data_interval_end | ds}}"},
    dag=dag,
)
```

- csv 파일을 parquet으로 변경
- 온프레미스 환경에서는 .csv 파일이 서버에 남아 있을 것이나, kubernetes 환경에서는 csv 파일을 어디에 저장할 지 생각할 필요가 있음
- 파일 이름을 xcom을 사용하여 저장


### 3) aws s3로 데이터 업로드

```
def _upload_s3_files(file_name, dest_name, **kwargs):

    s3_client = boto3.client(
        "s3",
        aws_access_key_id="액세스 키",
        aws_secret_access_key="비밀 키",
    )
    file_name = kwargs["ti"].xcom_pull(key="parquet_file_path", task_id="converting")

    response = s3_client.upload_file(file_name, "버킷 이름", dest_name)

upload_data = PythonOperator(
    task_id="upload_parquet", python_callable=_upload_s3_files, dag=dag
)
```
- 로컬 컴퓨터에서 amazon airflow provider가 설치 안 되는 문제가 있어서, boto3를 사용했습니다. 
- 차후에, 아마존 훅을 사용하는 방향으로 수정할 예정입니다. 

### 4) Email 알람 태스크 만들기

- 이메일 전송을 위한 설정 수정
```
~/airflow/airflow.cfg

smtp_starttls = False
smtp_ssl = True
smtp_user = 이메일
smtp_password = 구글에서 받은 비밀번호 
smtp_port = 465
smtp_mail_from = 이메일
smtp_timeout = 30
smtp_retry_limit = 5
```

- EmailOperator 사용

```
mailling = EmailOperator(
    task_id="emailling",
    to="대상 이메일",
    subject="{{ds}} 작업 완료했습니다..",
    html_content="""
                        {{ ds }} 작업 종료
                    """,
    dag=dag,
)
```

### 5) 의존성 정의
```
get_data >> convert_csv_to_parquet >> upload_data >> mailling
```

## 6. 추가 작업

- postgres 서버 작업
- 복잡한 처리 작업(SparkSubmitOperator 사용)
- airflow executor를 CeleryExecutor로 변경
- airflow metastore를 postgres로 변경