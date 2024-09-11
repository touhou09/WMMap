# Python 3.12 베이스 이미지를 사용
FROM python:3.12-slim

# 작업 디렉토리를 /projects로 설정
WORKDIR /projects

# 현재 디렉토리의 모든 파일을 /projects로 복사
COPY . /projects

# requirements.txt 파일을 이용해 필요한 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# Airflow 기본 설정 추가
ENV AIRFLOW_HOME=/projects

# airflow.cfg 파일이 있는 위치를 지정하거나 직접 설정할 수 있습니다.
# 여기서는 기본 설정을 사용한다고 가정

# 웹서버와 스케쥴러를 동시에 실행하도록 설정
CMD ["sh", "-c", "airflow scheduler & airflow webserver -p 8080"]
