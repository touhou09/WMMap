import os
import json

from utils import read_api
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, trim, split, when, concat_ws, collect_list, udf, struct, to_json
from pyspark.sql.types import StringType, ArrayType, StructType, StructField

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("DataProcessingApp") \
    .getOrCreate()

# .env 파일의 경로를 지정하고 로드
env_path = os.path.join(os.path.dirname(__file__), '.env')  # 수정된 부분
load_dotenv(dotenv_path=env_path)

# 환경 변수에서 값 가져오기
service_key = os.getenv('SERVICE_KEY')
url = os.getenv('URL')


# 일단 환경변수로 지정, 이후 airflow에서 서버날짜로 관리
start_date = os.getenv('start_date')


# spark-read-api 에서 완성한 api read 함수
data_list = read_api(service_key, url, start_date)

# 데이터 스키마 정의
schema = StructType([
    StructField("SN", StringType(), True),
    StructField("CRT_DT", StringType(), True),
    StructField("MSG_CN", StringType(), True),
    StructField("EMRG_STEP_NM", StringType(), True),
    StructField("DST_SE_NM", StringType(), True),
    StructField("RCPTN_RGN_NM", StringType(), True)
])

# df 만들기 함수 호출
df = create_dataframe(spark, data_list, schema)

# 지역 정보 추출 및 데이터 처리
region_df = df.withColumn("regions", split(trim(col("RCPTN_RGN_NM")), ",")) \
    .withColumn("regions", explode(col("regions"))) \
    .withColumn("primary_region", when(col("regions").contains(" "), split(col("regions"), " ")[0]).otherwise(col("regions"))) \
    .withColumn("secondary_region", when(col("regions").contains(" "), concat_ws(" ", split(col("regions"), " ").getItem(1))).otherwise("전체"))

# 데이터 그룹화 및 정렬
result_df = region_df.groupBy("primary_region", "secondary_region").agg(
    collect_list(
        struct("SN", "CRT_DT", "MSG_CN", "EMRG_STEP_NM", "DST_SE_NM")
    ).alias("data")
).orderBy("primary_region", "secondary_region")

# 데이터를 'CRT_DT' 필드를 기준으로 날짜 순으로 정렬하는 함수
def sort_by_date(data):
    sorted_data = sorted(data, key=lambda x: datetime.strptime(x['CRT_DT'], '%Y/%m/%d %H:%M:%S'))
    return sorted_data

# UDF 등록
sort_udf = udf(sort_by_date, ArrayType(StructType([
    StructField("SN", StringType(), True),
    StructField("CRT_DT", StringType(), True),
    StructField("MSG_CN", StringType(), True),
    StructField("EMRG_STEP_NM", StringType(), True),
    StructField("DST_SE_NM", StringType(), True)
])))

# 정렬된 데이터 프레임 생성
sorted_df = result_df.withColumn("data", sort_udf(col("data")))

# JSON으로 변환
json_df = sorted_df.withColumn("json_data", to_json(struct(col("*")))).select("json_data")

#나중에 S3나 DB에 저장하는 로직을 넣을 예정

# Spark 세션 종료
spark.stop() 
"""
# JSON 데이터를 수집하여 출력
json_results = json_df.collect()
for row in json_results:
    print(row.json_data)

# Spark 세션 종료
spark.stop() 
"""

"""
위의 주석 내용은 출력 확인을 위해서 분리해놓은 내용

아래 처리 후 저장 따로 작성 후 테스트 코드로 검증
"""