import os
import requests
from datetime import datetime
import logging
from dotenv import load_dotenv

from pyspark.sql.functions import col, split, trim, when, explode, concat_ws

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_api(url, service_key, start_date):
    """API를 호출하고 데이터를 수집하여 반환하는 함수"""
    
    params = {
        'serviceKey': service_key,
        'numOfRows': 30,  # 페이지당 개수 설정 (최대값으로 설정, 필요시 조정)
        'pageNo': 1,  # 페이지 번호를 1로 고정
        'crtDt': start_date,  # 조회 시작일자 (예: 2024년 9월 2일)
    }

    data_list = []

    while True:
        # API 호출
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            body_data = data.get('body', [])
            data_list.extend(body_data)

            total_count = data.get('totalCount', 0)
            current_page = params['pageNo']
            num_of_rows = params['numOfRows']

            if current_page * num_of_rows >= total_count:
                break

            params['pageNo'] += 1
        else:
            logger.error(f"요청 실패: {response.status_code}, 응답 내용: {response.text}")
            break

    return data_list

def create_dataframe(spark, data_list, schema):
    
    # spark 세션 객체를 받아서 실행하는 방식으로 구성
    
    rdd = spark.sparkContext.parallelize(data_list)
    return spark.createDataFrame(rdd, schema)

def extract_and_process_regions(df):
    
    # df에서 지역별로 나누는 과정
    
    df = df.withColumn("regions", split(trim(col("RCPTN_RGN_NM")), ",")) \
           .withColumn("regions", explode(col("regions"))) \
           .withColumn("primary_region", when(col("regions").contains(" "), split(col("regions"), " ")[0]).otherwise(col("regions"))) \
           .withColumn("secondary_region", when(col("regions").contains(" "), concat_ws(" ", split(col("regions"), " ").getItem(1))).otherwise("전체"))
    
    # RCPTN_RGN_NM 열 삭제
    return df.drop("RCPTN_RGN_NM")