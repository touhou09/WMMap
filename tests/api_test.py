import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# .env 파일의 경로를 지정하고 로드
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=env_path)

# 환경 변수에서 값 가져오기
service_key = os.getenv('service_key')

if not service_key:
    print("SERVICE_KEY가 설정되지 않았습니다.")
    exit()

params = {
    'serviceKey': service_key,
    'numOfRows': 30,  # 페이지당 개수 설정 (최대값으로 설정, 필요시 조정)
    'pageNo': 1,  # 페이지 번호를 1로 고정
    'crtDt': '20240906',  # 조회 시작일자 (예: 2024년 8월 24일)
}
region_data = {}  # 지역별 데이터를 저장할 딕셔너리

while True:
    # GET 요청 보내기
    url = 'https://www.safetydata.go.kr/V2/api/DSSP-IF-00247'
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # 응답 데이터 출력 (문제 발생 시 구조 파악을 위해)
        print("응답 데이터:", json.dumps(data, ensure_ascii=False, indent=4))
        
        # 응답에서 데이터 추출 및 날짜 정보 추가
        body_data = data.get('body', None)
        
        if body_data is None:
            print("응답에 'body' 데이터가 없습니다. 루프를 종료합니다.")
            break  # 'body' 데이터가 없을 때 반복 종료
        
        for item in body_data:
            # 필요한 필드만 선택
            processed_item = {
                'SN': item['SN'],
                'CRT_DT': item['CRT_DT'],
                'MSG_CN': item['MSG_CN'],
                'EMRG_STEP_NM': item['EMRG_STEP_NM'],
                'DST_SE_NM': item['DST_SE_NM']
            }
            
            # 수신지역명을 시, 군, 구 단위로 분리
            regions = item['RCPTN_RGN_NM'].split(',')
            for region in regions:
                region = region.strip()  # 양쪽 공백 제거
                
                # 도/특별시/광역시 추출
                if ' ' in region:
                    primary_region = region.split(' ')[0]  # 첫 번째 부분 (예: 경상북도, 울산광역시)
                else:
                    primary_region = region  # '전체'만 있는 경우
                
                # 시/군/구 추출
                if "전체" in region:
                    secondary_region = "전체"
                else:
                    region_parts = region.split(' ')
                    if len(region_parts) > 1:
                        secondary_region = ' '.join(region_parts[1:])  # 나머지 부분 (예: 포항시 남구, 남구)
                    else:
                        secondary_region = primary_region  # 지역명이 하나인 경우
                
                # 도/특별시/광역시 키가 존재하지 않으면 초기화
                if primary_region not in region_data:
                    region_data[primary_region] = {}

                # 시/군/구 키가 존재하지 않으면 초기화
                if secondary_region not in region_data[primary_region]:
                    region_data[primary_region][secondary_region] = []
                
                # 지역 키에 데이터 추가
                region_data[primary_region][secondary_region].append(processed_item)

        # 총 개수와 현재 페이지 계산
        total_count = data.get('totalCount', 0)
        current_page = params['pageNo']
        num_of_rows = params['numOfRows']
        
        # 모든 데이터를 다 가져왔는지 확인
        if current_page * num_of_rows >= total_count:
            break  # 반복 종료
        
        # 다음 페이지로 넘어가기
        params['pageNo'] += 1
    else:
        print(f"요청 실패: {response.status_code}")
        print("응답 내용:", response.text)
        print("응답 헤더:", response.headers)
        break  # 오류 발생 시 루프 종료

# 데이터를 'CRT_DT' 필드를 기준으로 날짜 순으로 정렬 (각 지역별로)
for primary_region in region_data:
    for secondary_region in region_data[primary_region]:
        region_data[primary_region][secondary_region] = sorted(
            region_data[primary_region][secondary_region], 
            key=lambda x: datetime.strptime(x['CRT_DT'], '%Y/%m/%d %H:%M:%S')
        )

# 데이터 출력
print("\n=====")
print(json.dumps(region_data, ensure_ascii=False, indent=4))
print("=====")

print(f"총 {sum(len(region_data[primary_region][secondary_region]) for primary_region in region_data for secondary_region in region_data[primary_region])}개의 데이터를 출력했습니다.")