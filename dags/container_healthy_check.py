from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import requests
import json
import os

# 로그 파일 설정
LOG_FILE = "/logs/container_health.log"

# 로그 디렉터리 생성
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# 컨테이너 상태 확인 함수
def check_container(container_name):
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--filter", "status=running"],
            capture_output=True, text=True, check=True
        )
        if container_name in result.stdout:
            log_message = f"{datetime.now()}: {container_name} is running."
            print(log_message)
            with open(LOG_FILE, "a") as log_file:
                log_file.write(log_message + "\n")
            return True
        else:
            log_message = f"{datetime.now()}: {container_name} is not running."
            print(log_message)
            with open(LOG_FILE, "a") as log_file:
                log_file.write(log_message + "\n")
            return False
    except subprocess.CalledProcessError as e:
        log_message = f"{datetime.now()}: Error checking {container_name}. Details: {e.stderr}"
        print(log_message)
        with open(LOG_FILE, "a") as log_file:
            log_file.write(log_message + "\n")
        return False

# Spark Worker 상태 확인 함수
def check_spark_worker(worker_name):
    master_url = "http://spark-master:8080"  # 필요시 IP 주소로 변경
    try:
        response = requests.get(f"{master_url}/json", timeout=5)
        response.raise_for_status()
        workers = [worker["id"] for worker in response.json().get("workers", [])]

        if worker_name in workers:
            log_message = f"{datetime.now()}: {worker_name} is registered with Spark Master."
            print(log_message)
            with open(LOG_FILE, "a") as log_file:
                log_file.write(log_message + "\n")
            return True
        else:
            log_message = f"{datetime.now()}: {worker_name} is NOT registered with Spark Master."
            print(log_message)
            with open(LOG_FILE, "a") as log_file:
                log_file.write(log_message + "\n")
            return False
    except requests.RequestException as e:
        log_message = f"{datetime.now()}: Error checking Spark Worker {worker_name}. Details: {str(e)}"
        print(log_message)
        with open(LOG_FILE, "a") as log_file:
            log_file.write(log_message + "\n")
        return False


# 전체 작업 수행 함수
def perform_health_check():
    # Redis 상태 확인
    check_container("redis")

    # Spark Master 상태 확인
    check_container("spark-master")

    # Spark Worker 상태 확인
    if check_container("spark-worker"):
        check_spark_worker("spark-worker")

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 정의
dag = DAG(
    'container_health_check_python',
    default_args=default_args,
    description='Check health of containers using PythonOperator',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2024, 12, 23),
    catchup=False,
)

# PythonOperator 작업 정의
check_health_task = PythonOperator(
    task_id='check_container_health',
    python_callable=perform_health_check,
    dag=dag,
)