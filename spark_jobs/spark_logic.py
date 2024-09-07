import os
from utils import read_api, create_dataframe, extract_and_process_regions, sort_data_by_date, group_and_sort_data, convert_to_json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def spark_data_processing(service_key, url, start_date):
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("DataProcessingApp") \
        .getOrCreate()

    # API로부터 데이터 가져오기
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

    # DataFrame 생성
    df = create_dataframe(spark, data_list, schema)
    
    # 지역별로 데이터 추출 및 처리
    region_df = extract_and_process_regions(df)
    
    # 데이터 그룹화 및 정렬
    result_df = group_and_sort_data(region_df)
    
    # 데이터를 'CRT_DT' 필드를 기준으로 날짜 순으로 정렬
    sorted_df = sort_data_by_date(result_df)
    
    # JSON으로 변환
    json_df = convert_to_json(sorted_df)

    # 여기에서 S3나 DB에 저장하는 로직을 추가할 수 있습니다.
    # 예: json_df.write.format("json").save("s3a://your-bucket/your-path")

    # Spark 세션 종료
    spark.stop()