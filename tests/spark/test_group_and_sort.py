import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, StructType
from pyspark.sql import Row
from pyspark.sql.functions import collect_list, struct
from spark_jobs.utils import group_and_sort_data

@pytest.fixture(scope="module")
def spark():
    # PySpark 세션 생성
    spark = SparkSession.builder \
        .appName("Test group_and_sort_data function") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_group_and_sort_data(spark):
    # 테스트에 사용할 데이터 리스트와 스키마 정의
    data = [
        ("Region1", "SubRegionA", "SN001", "2024-09-04", "Message 1", "High", "TypeA"),
        ("Region1", "SubRegionA", "SN002", "2024-09-05", "Message 2", "Medium", "TypeB"),
        ("Region2", "SubRegionB", "SN003", "2024-09-06", "Message 3", "Low", "TypeC"),
        ("Region1", "SubRegionA", "SN004", "2024-09-07", "Message 4", "Medium", "TypeD"),
        ("Region2", "SubRegionC", "SN005", "2024-09-08", "Message 5", "High", "TypeE")
    ]
    
    schema = StructType([
        StructField("primary_region", StringType(), True),
        StructField("secondary_region", StringType(), True),
        StructField("SN", StringType(), True),
        StructField("CRT_DT", StringType(), True),
        StructField("MSG_CN", StringType(), True),
        StructField("EMRG_STEP_NM", StringType(), True),
        StructField("DST_SE_NM", StringType(), True)
    ])

    # 테스트 데이터프레임 생성
    region_df = spark.createDataFrame(data, schema)

    # group_and_sort_data 함수 호출하여 결과 생성
    result_df = group_and_sort_data(region_df)

    # 예상 결과 정의
    expected_data = [
        Row(primary_region="Region1", secondary_region="SubRegionA", data=[
            Row(SN="SN001", CRT_DT="2024-09-04", MSG_CN="Message 1", EMRG_STEP_NM="High", DST_SE_NM="TypeA"),
            Row(SN="SN002", CRT_DT="2024-09-05", MSG_CN="Message 2", EMRG_STEP_NM="Medium", DST_SE_NM="TypeB"),
            Row(SN="SN004", CRT_DT="2024-09-07", MSG_CN="Message 4", EMRG_STEP_NM="Medium", DST_SE_NM="TypeD")
        ]),
        Row(primary_region="Region2", secondary_region="SubRegionB", data=[
            Row(SN="SN003", CRT_DT="2024-09-06", MSG_CN="Message 3", EMRG_STEP_NM="Low", DST_SE_NM="TypeC")
        ]),
        Row(primary_region="Region2", secondary_region="SubRegionC", data=[
            Row(SN="SN005", CRT_DT="2024-09-08", MSG_CN="Message 5", EMRG_STEP_NM="High", DST_SE_NM="TypeE")
        ])
    ]
    
    expected_schema = StructType([
        StructField("primary_region", StringType(), True),
        StructField("secondary_region", StringType(), True),
        StructField("data", ArrayType(
            StructType([
                StructField("SN", StringType(), True),
                StructField("CRT_DT", StringType(), True),
                StructField("MSG_CN", StringType(), True),
                StructField("EMRG_STEP_NM", StringType(), True),
                StructField("DST_SE_NM", StringType(), True)
            ]),
            False
        ), False)
    ])


    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # DataFrame이 예상 결과와 일치하는지 확인
    assert result_df.collect() == expected_df.collect()
    assert result_df.schema == expected_df.schema