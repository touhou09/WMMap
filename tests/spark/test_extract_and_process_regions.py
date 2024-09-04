import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
from pyspark.sql.functions import col, explode, split, trim, when, concat_ws
from spark_jobs.utils import extract_and_process_regions

@pytest.fixture(scope="module")
def spark():
    # PySpark 세션 생성
    spark = SparkSession.builder \
        .appName("Test extract_and_process_regions function") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_extract_and_process_regions(spark):
    # 테스트에 사용할 데이터 리스트와 스키마 정의
    data = [
        ("SN001", "서울특별시 전체"),
        ("SN002", "서울특별시 서초구"),
        ("SN003", "경기도 광주시"),
        ("SN004", "대구광역시 북구"),
        ("SN005", "제주특별자치도 전체")
    ]
    
    schema = StructType([
        StructField("SN", StringType(), True),
        StructField("RCPTN_RGN_NM", StringType(), True)
    ])

    # 테스트 데이터프레임 생성
    df = spark.createDataFrame(data, schema)

    # extract_and_process_regions 함수 호출하여 결과 생성
    result_df = extract_and_process_regions(df)

    # 예상 결과 정의
    expected_data = [
        Row(SN="SN001", regions="서울특별시 전체", primary_region="서울특별시", secondary_region="전체"),
        Row(SN="SN002", regions="서울특별시 서초구", primary_region="서울특별시", secondary_region="서초구"),
        Row(SN="SN003", regions="경기도 광주시", primary_region="경기도", secondary_region="광주시"),
        Row(SN="SN004", regions="대구광역시 북구", primary_region="대구광역시", secondary_region="북구"),
        Row(SN="SN005", regions="제주특별자치도 전체", primary_region="제주특별자치도", secondary_region="전체")
    ]
    
    expected_schema = StructType([
        StructField("SN", StringType(), True),
        StructField("regions", StringType(), False),  # nullable=False로 수정
        StructField("primary_region", StringType(), True),
        StructField("secondary_region", StringType(), False)  # nullable=False로 수정
    ])

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # DataFrame이 예상 결과와 일치하는지 확인
    assert result_df.collect() == expected_df.collect()
    assert result_df.schema == expected_df.schema
