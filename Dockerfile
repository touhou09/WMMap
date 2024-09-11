# Python 3.12 베이스 이미지를 사용
FROM python:3.12-slim

# 작업 디렉토리를 /projects로 설정
WORKDIR /projects

# 현재 디렉토리의 모든 파일을 /projects로 복사
COPY . /projects

# requirements.txt 파일을 이용해 필요한 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# /projects/dags/spark_job_dag.py 스크립트를 실행하도록 설정
CMD ["python", "/projects/dags/spark_job_dag.py"]