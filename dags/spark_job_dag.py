from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
from dotenv import load_dotenv
from spark_jobs.spark_logic import spark_data_processing

# .env 파일 로드 (Airflow 환경에 따라 설정할 수도 있음)
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG 정의
dag = DAG(
    'spark_data_processing_dag',
    default_args=default_args,
    description='DAG to run Spark data processing job',
    schedule_interval=None,  # 필요시 스케줄 설정
)

# Airflow Task로 실행할 함수
def run_spark_task(**kwargs):

    # .env 파일의 경로를 지정하고 로드
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path=env_path)

    # 환경 변수에서 값 가져오기
    service_key = os.getenv('SERVICE_KEY')
    url = os.getenv('URL')

    if not url:
        raise ValueError("URL is not provided in the .env file.")

    # Airflow의 매개변수 혹은 .env 파일에서 값 가져오기
    service_key = os.getenv('SERVICE_KEY')
    url = os.getenv('URL')
    start_date = kwargs.get('start_date')  # Airflow에서 전달된 파라미터

    if not url or not service_key:
        raise ValueError("URL or Service Key is not provided in the .env file or Airflow variables.")

    # Spark 작업 실행
    result_json = spark_data_processing(service_key, url, start_date)
    return result_json

# PythonOperator를 사용하여 Spark 작업을 Airflow Task로 정의
spark_task = PythonOperator(
    task_id='run_spark_processing',
    python_callable=run_spark_task,
    provide_context=True,  # Airflow context에서 매개변수를 받기 위해 설정
    dag=dag,
)

spark_task
