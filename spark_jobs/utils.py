import os
import requests
from datetime import datetime
import logging
from dotenv import load_dotenv
import boto3

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, trim, explode, when, concat_ws, col, collect_list, struct, to_json, udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import requests
import logging

# 로깅 설정
logger = logging.getLogger("api_logger")

def read_api(service_key, start_date):
    """API를 호출하고 데이터를 수집하여 반환하는 함수"""
    
    params = {
        'serviceKey': service_key,
        'numOfRows': 30,  # 페이지당 개수 설정 (최대값으로 설정, 필요시 조정)
        'pageNo': 1,  # 페이지 번호를 1로 고정
        'crtDt': start_date,  # 조회 시작일자 (예: 2024년 9월 2일)
    }

    data_list = []

    try:
        while True:
            # API 호출
            
            url='https://www.safetydata.go.kr/V2/api/DSSP-IF-00247'

            
            response = requests.get(url, params=params)  # 타임아웃 추가

            if response.status_code == 200:
                data = response.json()

                # body_data가 존재하는지 확인
                body_data = data.get('body')
                if body_data is None:
                    logger.warning("응답에 'body' 데이터가 없습니다.")
                    break

                # body_data가 리스트가 아닌 경우 빈 리스트로 처리
                data_list.extend(body_data if isinstance(body_data, list) else [])

                # totalCount가 없으면 더 이상 호출하지 않음
                total_count = data.get('totalCount')
                if total_count is None:
                    logger.warning("totalCount가 응답에 없습니다. 더 이상 데이터를 가져오지 않습니다.")
                    break

                current_page = params['pageNo']
                num_of_rows = params['numOfRows']

                # 마지막 페이지인지 확인
                if current_page * num_of_rows >= total_count:
                    break

                # 페이지 번호 증가
                params['pageNo'] += 1
            else:
                logger.error(f"요청 실패: {response.status_code}, 응답 내용: {response.text}")
                break

    except requests.exceptions.RequestException as e:
        logger.error(f"API 요청 중 오류 발생: {str(e)}")

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


def convert_to_json_and_upload_s3_directly(
    sorted_df: DataFrame, 
    bucket_name: str, 
    start_date: str,
    aws_access_key_id: str, 
    aws_secret_access_key: str, 
    spark
    ):
    """
    DataFrame을 JSON으로 변환하고, 로컬 파일을 거치지 않고 S3에 직접 저장하는 함수.
    파일명이 같으면 덮어씁니다.
    
    :param sorted_df: Spark DataFrame
    :param bucket_name: S3 버킷 이름
    :param s3_key: S3에 저장할 파일 경로
    :param aws_access_key_id: AWS Access Key
    :param aws_secret_access_key: AWS Secret Key
    :param region_name: S3가 위치한 AWS 리전
    :return: S3에 저장된 파일의 링크
    """
    region_name = 'ap-northeast-2'
    
    # AWS 자격 증명을 Spark 세션에 전달
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_access_key)
    hadoop_conf.set("fs.s3a.endpoint", f"s3.{region_name}.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # DataFrame을 JSON 형식으로 변환
    json_df = sorted_df.withColumn("json_data", to_json(struct(col("*")))).select("json_data")

    
    # S3 경로 설정 (start_date를 경로에 포함)
    s3_key = f"data/{start_date}/file.json"
    s3_path = f"s3a://{bucket_name}/{s3_key}"
    
    # DataFrame을 S3에 저장 (overwrite 모드)
    json_df.coalesce(1).write.mode("overwrite").json(s3_path)

    # S3 파일 URL 반환
    file_url = f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{s3_key}"
    
    return file_url