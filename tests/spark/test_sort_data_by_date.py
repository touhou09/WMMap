import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import Row
from spark_jobs.utils import sort_data_by_date
from datetime import datetime

@pytest.fixture(scope="module")
def spark():
    # PySpark 세션 생성
    spark = SparkSession.builder \
        .appName("Test sort_data_by_date function") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_sort_data_by_date(spark):
    # 테스트에 사용할 데이터 리스트와 스키마 정의
    data = [
        Row(primary_region="Region1", secondary_region="SubRegionA", data=[
            Row(SN="SN001", CRT_DT="2024/09/05 14:00:00", MSG_CN="Message 1", EMRG_STEP_NM="High", DST_SE_NM="TypeA"),
            Row(SN="SN002", CRT_DT="2024/09/04 12:00:00", MSG_CN="Message 2", EMRG_STEP_NM="Medium", DST_SE_NM="TypeB"),
            Row(SN="SN003", CRT_DT="2024/09/06 08:30:00", MSG_CN="Message 3", EMRG_STEP_NM="Low", DST_SE_NM="TypeC")
        ]),
        Row(primary_region="Region2", secondary_region="SubRegionB", data=[
            Row(SN="SN004", CRT_DT="2024/09/03 10:00:00", MSG_CN="Message 4", EMRG_STEP_NM="High", DST_SE_NM="TypeD"),
            Row(SN="SN005", CRT_DT="2024/09/02 09:30:00", MSG_CN="Message 5", EMRG_STEP_NM="Medium", DST_SE_NM="TypeE")
        ])
    ]
    
    schema = StructType([
        StructField("primary_region", StringType(), True),
        StructField("secondary_region", StringType(), True),
        StructField("data", ArrayType(
            StructType([
                StructField("SN", StringType(), True),
                StructField("CRT_DT", StringType(), True),
                StructField("MSG_CN", StringType(), True),
                StructField("EMRG_STEP_NM", StringType(), True),
                StructField("DST_SE_NM", StringType(), True)
            ])
        ), True)
    ])

    # 테스트 데이터프레임 생성
    df = spark.createDataFrame(data, schema)

    # sort_data_by_date 함수 호출하여 결과 생성
    result_df = sort_data_by_date(df)

    # 예상 결과 정의
    expected_data = [
        Row(primary_region="Region1", secondary_region="SubRegionA", data=[
            Row(SN="SN002", CRT_DT="2024/09/04 12:00:00", MSG_CN="Message 2", EMRG_STEP_NM="Medium", DST_SE_NM="TypeB"),
            Row(SN="SN001", CRT_DT="2024/09/05 14:00:00", MSG_CN="Message 1", EMRG_STEP_NM="High", DST_SE_NM="TypeA"),
            Row(SN="SN003", CRT_DT="2024/09/06 08:30:00", MSG_CN="Message 3", EMRG_STEP_NM="Low", DST_SE_NM="TypeC")
        ]),
        Row(primary_region="Region2", secondary_region="SubRegionB", data=[
            Row(SN="SN005", CRT_DT="2024/09/02 09:30:00", MSG_CN="Message 5", EMRG_STEP_NM="Medium", DST_SE_NM="TypeE"),
            Row(SN="SN004", CRT_DT="2024/09/03 10:00:00", MSG_CN="Message 4", EMRG_STEP_NM="High", DST_SE_NM="TypeD")
        ])
    ]

    expected_df = spark.createDataFrame(expected_data, schema)

    # DataFrame이 예상 결과와 일치하는지 확인
    assert result_df.collect() == expected_df.collect()
    assert result_df.schema == expected_df.schema
