# Python 3.12 베이스 이미지를 사용
FROM python:3.12-slim

# 환경 변수를 설정해 출력 버퍼링을 비활성화 (Docker 로그에서 실시간으로 출력 확인)
ENV PYTHONUNBUFFERED=1

# 작업 디렉토리를 /projects로 설정
WORKDIR /projects

# requirements.txt만 먼저 복사해서 패키지 설치 (캐싱 최적화)
COPY requirements.txt /projects/

# requirements.txt 파일을 이용해 필요한 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# 나머지 파일을 /projects로 복사
COPY . /projects

# /projects/dags/spark_job_dag.py 스크립트를 실행하도록 설정
CMD ["python", "/projects/dags/spark_job_dag.py"]
