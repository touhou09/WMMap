import logging
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.0 이상 호환
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# 프로젝트의 경로를 sys.path에 추가하여 모듈을 찾을 수 있도록 설정
project_path = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(project_path)

# 프로젝트의 절대 경로를 sys.path에 추가하여 spark_logic을 찾을 수 있도록 설정
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=env_path)

# 이제 spark_logic 모듈을 임포트할 수 있습니다
from spark_jobs.spark_logic import spark_data_processing

# 기본 설정에 태스크 실행 시간 제한을 추가
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # 1일 전으로 설정
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=10),
}


# DAG 정의
dag = DAG(
    'spark_data_processing_dag',
    default_args=default_args,
    description='DAG to run Spark data processing job',
    schedule_interval='*/10 * * * *',  # 10분에 한 번씩 실행
    catchup=False,  # 이전 실행들을 catchup하지 않도록 설정
)

def run_spark_task(execution_date, **kwargs):
    logger = logging.getLogger("airflow.task")
    
    # .env 파일 로드 (Airflow 환경에 따라 설정할 수도 있음)
    load_dotenv()
    
    try:
        # 환경 변수에서 값 가져오기
        service_key = os.getenv('service_key')
        bucket_name = os.getenv('bucket_name')
        aws_access_key_id = os.getenv('aws_access_key_id')
        aws_secret_access_key = os.getenv('aws_secret_access_key')
        table_name=os.getenv('table_name')
    
        logger.info(f"Service Key: {service_key}")

        # process_date는 현재 시간을 사용하여 yyyymmdd 형식으로 계산
        process_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"Process Date: {process_date}")

        # Spark 작업 실행
        result_json = spark_data_processing(
            service_key, 
            process_date,
            bucket_name,
            aws_access_key_id,
            aws_secret_access_key,
            table_name
        )
        
        logger.info(f"Spark 작업이 성공적으로 완료되었습니다: {result_json}")
        return result_json

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise

# PythonOperator를 사용하여 Spark 작업을 Airflow Task로 정의
spark_task = PythonOperator(
    task_id='run_spark_processing',
    python_callable=run_spark_task,
    op_kwargs={'execution_date': '{{ ds }}'},  # Jinja 템플릿 사용
    dag=dag,
)

spark_task
