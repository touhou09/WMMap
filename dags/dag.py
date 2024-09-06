import boto3
import logging
import os
import json
import requests

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

from utils import fetch_data_from_api

# .env 파일의 경로를 지정하고 로드
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

# 환경 변수에서 API 관련 값 가져오기
service_key = os.getenv('SERVICE_KEY')
url = os.getenv('URL')

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 현재 서버 날짜를 기반으로 start_date 설정
current_date = datetime.now().strftime('%y%m%d')

# DAG 정의
dag = DAG(
    'api_spark_s3_dynamodb',
    default_args=default_args,
    description='API 호출 -> Spark 처리 -> S3 저장 -> DynamoDB 저장',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),  # 서버 날짜 기준으로 시작일 설정
    catchup=False,
)

# 1. API 호출 Task
def api_call_task(**kwargs):
    data_list = fetch_data_from_api(
        service_key=service_key, 
        url=url, 
        start_date=current_date,  # 서버 날짜를 start_date로 전달
        num_of_rows=30  # 페이지당 개수
    )
    
    # 데이터를 XCom에 저장하여 다른 Task에서 사용 가능하도록 함
    kwargs['ti'].xcom_push(key='api_data', value=data_list)

# PythonOperator로 API 호출 Task 생성
api_call = PythonOperator(
    task_id='api_call',
    python_callable=api_call_task,
    provide_context=True,  # XCom 사용을 위해 제공
    dag=dag,
)

# 이후 Spark 작업, S3 저장, DynamoDB 저장 Task를 구성할 수 있음
# api_call >> spark_task >> save_to_s3_task >> save_to_dynamodb_task


# 1. API 호출 Task
api_call_task = PythonOperator(
    task_id='api_call',
    python_callable=fetch_data_from_api,
    op_kwargs={
        'service_key': service_key,
        'url': url,
        'start_date': '20240902',  # 예시로 고정된 날짜
        'num_of_rows': 30  # 페이지당 개수
    },
    provide_context=True,  # XCom 사용을 위해 제공
    dag=dag,
)

# 2. Spark 작업 Task
spark_task = SparkSubmitOperator(
    task_id='spark_submit',  # Task ID
    application='/path/to/spark_job.py',  # 실행할 Spark 작업 파일 경로
    conn_id='spark_default',  # Spark 연결 ID (Airflow Connections에서 설정)
    conf={'spark.executor.memory': '2g'},  # Spark 설정
    dag=dag,  # DAG 연결
)

# 3. S3 저장 Task
def save_to_s3(**kwargs):
    try:
        # S3 클라이언트 생성
        s3 = boto3.client('s3')

        # S3 버킷 및 파일 이름 설정
        bucket_name = 'my-bucket'  # 유연성을 위해 Airflow Variable로 관리 가능
        file_name = f"processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        # Spark 작업 결과 가져오기 (XCom)
        spark_output = kwargs['ti'].xcom_pull(task_ids='spark_submit')

        # 결과를 S3에 저장
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=spark_output)

        # S3 URL 생성 및 XCom에 저장
        s3_url = f"s3://{bucket_name}/{file_name}"
        logging.info(f"S3에 저장된 파일: {s3_url}")
        kwargs['ti'].xcom_push(key='s3_url', value=s3_url)
    except Exception as e:
        logging.error(f"S3 저장 중 오류 발생: {e}")
        raise

save_to_s3_task = PythonOperator(
    task_id='save_to_s3',  # Task ID
    python_callable=save_to_s3,  # 실행할 함수
    provide_context=True,  # 컨텍스트 제공
    dag=dag,  # DAG 연결
)

# 4. DynamoDB 저장 Task
def save_to_dynamodb(**kwargs):
    try:
        # XCom에서 S3 URL 가져오기
        s3_url = kwargs['ti'].xcom_pull(task_ids='save_to_s3', key='s3_url')

        # DynamoDB 리소스 생성
        dynamodb = boto3.resource('dynamodb')

        # DynamoDB 테이블 이름 설정 (Airflow Variable로 관리 가능)
        table = dynamodb.Table('my_table')

        # 데이터 저장 (S3 URL 및 현재 타임스탬프)
        table.put_item(
            Item={
                'id': str(datetime.now().timestamp()),  # 고유 ID
                's3_url': s3_url,  # S3 URL
                'timestamp': str(datetime.now())  # 현재 시간
            }
        )
        logging.info(f"DynamoDB에 저장 완료: S3 URL: {s3_url}")
    except Exception as e:
        logging.error(f"DynamoDB 저장 중 오류 발생: {e}")
        raise

save_to_dynamodb_task = PythonOperator(
    task_id='save_to_dynamodb',  # Task ID
    python_callable=save_to_dynamodb,  # 실행할 함수
    provide_context=True,  # 컨텍스트 제공
    dag=dag,  # DAG 연결
)

# Task 의존성 설정: API 호출 -> Spark 작업 -> S3 저장 -> DynamoDB 저장
api_call_task >> spark_task >> save_to_s3_task >> save_to_dynamodb_task
