import requests

# API 호출 함수 정의
def fetch_data_from_api(service_key, url, start_date='20240902', num_of_rows=30):
    """
    API에서 데이터를 가져와 data_list로 반환하는 함수.
    
    :param service_key: API 호출 시 필요한 서비스 키
    :param url: API 엔드포인트 URL
    :param start_date: 데이터 조회 시작일자 (예: 20240902)
    :param num_of_rows: 페이지당 요청할 데이터 개수
    :return: API에서 받은 데이터 리스트
    """
    params = {
        'serviceKey': service_key,
        'numOfRows': num_of_rows,
        'pageNo': 1,
        'crtDt': start_date,
    }
    
    data_list = []
    
    while True:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            body_data = data.get('body', [])
            
            # API로부터 받은 데이터를 리스트에 추가
            data_list.extend(body_data)
            
            # 총 개수와 현재 페이지 계산
            total_count = data.get('totalCount', 0)
            current_page = params['pageNo']
            num_of_rows = params['numOfRows']
            
            # 모든 데이터를 다 가져왔는지 확인
            if current_page * num_of_rows >= total_count:
                break
            
            # 다음 페이지로 넘어가기
            params['pageNo'] += 1
        else:
            print(f"요청 실패: {response.status_code}")
            print("응답 내용:", response.text)
            break
    
    return data_list
