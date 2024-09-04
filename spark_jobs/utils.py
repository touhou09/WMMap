import os
import requests
from datetime import datetime
import logging
from dotenv import load_dotenv

from pyspark.sql import DataFrame
from pyspark.sql.functions import split, trim, explode, when, concat_ws, col, collect_list, struct, to_json, udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField

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

def group_and_sort_data(region_df):
    
    # groupby 후 지역별로 sort
    
    return region_df.groupBy("primary_region", "secondary_region").agg(
        collect_list(
            struct("SN", "CRT_DT", "MSG_CN", "EMRG_STEP_NM", "DST_SE_NM")
        ).alias("data")
    ).orderBy("primary_region", "secondary_region")
    
def sort_data_by_date(result_df):
    # 데이터를 'CRT_DT' 필드를 기준으로 날짜 순으로 정렬
    
    def sort_by_date(data):
        sorted_data = sorted(data, key=lambda x: datetime.strptime(x['CRT_DT'], '%Y/%m/%d %H:%M:%S'))
        return sorted_data

    sort_udf = udf(sort_by_date, ArrayType(StructType([
        StructField("SN", StringType(), True),
        StructField("CRT_DT", StringType(), True),
        StructField("MSG_CN", StringType(), True),
        StructField("EMRG_STEP_NM", StringType(), True),
        StructField("DST_SE_NM", StringType(), True)
    ])))
    
    return result_df.withColumn("data", sort_udf(col("data")))


def convert_to_json(sorted_df):
    # DataFrame을 JSON 형식으로 변환하는 함수.
    
    return sorted_df.withColumn("json_data", to_json(struct(col("*")))).select("json_data")