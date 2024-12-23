from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'container_health_check',
    default_args=default_args,
    description='Check health of containers every minute',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2024, 12, 23),
    catchup=False,
)

check_health = BashOperator(
    task_id='check_container_health',
    bash_command='/path/to/health_check.sh',
    dag=dag,
)

check_health
