from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum

# 기본 인수 설정 (start_date는 과거로 설정)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.now('Asia/Seoul').subtract(days=1),  # 1일 전으로 설정
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

# DAG 정의
dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='10분마다 Spark 작업 실행',
    schedule='*/10 * * * *',  # 10분마다 실행되는 크론 표현식
    catchup=False,
)

# BashOperator를 통해 현재 시간을 .env 파일에 저장하고 Spark 작업 실행
run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command=(
        'source /root/projects/.env && '  # .env 파일을 명시적으로 로드
        'echo "start_date=$(date +\'%y%m%d\')" >> /root/projects/spark_jobs/.env && '  # 현재 시간을 기록
        'spark-submit /root/projects/spark_jobs/spark_logic.py > /root/projects/spark_jobs/spark_job_output.log 2>&1 || '  # 성공 로그 및 오류 로그 출력
        'echo "Spark job failed. Check /root/projects/spark_jobs/spark_job_output.log for details." && exit 1'
    ),
    dag=dag,
)


run_spark_job