from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

current_time = datetime.now()

# 기본 인수 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': current_time,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

# DAG 정의
dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='10분마다 Spark 작업 실행',
    schedule='*/10 * * * *',  # 10분마다 실행되는 크론 표현식
    catchup=False,
)

# Spark 작업을 실행하는 BashOperator 태스크 정의
run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit /root/projects/spark_jobs/spark_logic.py {{ ds_nodash }}',
    dag=dag,
)

run_spark_job
