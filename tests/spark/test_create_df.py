import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from spark_jobs.utils import create_dataframe

@pytest.fixture(scope="module")
def spark():
    # PySpark 세션 생성
    spark = SparkSession.builder \
        .appName("Test create_dataframe function") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_create_dataframe(spark):
    # 테스트에 사용할 데이터 리스트와 주어진 스키마 정의
    data_list = [
        ("SN001", "2024-09-04", "Test message 1", "High", "TypeA", "Region1"),
        ("SN002", "2024-09-05", "Test message 2", "Medium", "TypeB", "Region2"),
        ("SN003", "2024-09-06", "Test message 3", "Low", "TypeC", "Region3")
    ]
    schema = StructType([
        StructField("SN", StringType(), True),
        StructField("CRT_DT", StringType(), True),
        StructField("MSG_CN", StringType(), True),
        StructField("EMRG_STEP_NM", StringType(), True),
        StructField("DST_SE_NM", StringType(), True),
        StructField("RCPTN_RGN_NM", StringType(), True)
    ])

    # create_dataframe 함수 호출하여 DataFrame 생성
    df = create_dataframe(spark, data_list, schema)

    # 예상 결과 정의
    expected_data = [
        ("SN001", "2024-09-04", "Test message 1", "High", "TypeA", "Region1"),
        ("SN002", "2024-09-05", "Test message 2", "Medium", "TypeB", "Region2"),
        ("SN003", "2024-09-06", "Test message 3", "Low", "TypeC", "Region3")
    ]
    expected_df = spark.createDataFrame(expected_data, schema)

    # DataFrame이 예상 결과와 일치하는지 확인
    assert df.collect() == expected_df.collect()
    assert df.schema == expected_df.schema
