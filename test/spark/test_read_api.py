import unittest
from unittest.mock import patch, MagicMock
from spark_jobs.utils import read_api
import logging

class TestDataProcessingApp(unittest.TestCase):

    @patch('spark_jobs.utils.requests.get')
    def test_read_api_success(self, mock_get):
        # Mock the API response to simulate a successful call
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'body': [
                {'SN': '123', 'CRT_DT': '2024/09/02 10:00:00', 'MSG_CN': 'Test Message', 'EMRG_STEP_NM': 'Test Step', 'DST_SE_NM': 'Test DST', 'RCPTN_RGN_NM': 'Test Region'}
            ],
            'totalCount': 1
        }
        mock_get.return_value = mock_response

        url = 'https://example.com/api'
        service_key = 'test_service_key'
        start_date = '20240902'

        data_list = read_api(url, service_key, start_date)

        self.assertEqual(len(data_list), 1)
        self.assertEqual(data_list[0]['SN'], '123')
        self.assertEqual(data_list[0]['MSG_CN'], 'Test Message')

    @patch('spark_jobs.utils.requests.get')
    def test_read_api_failure(self, mock_get):
        # Mock the API response to simulate a failure
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = 'Not Found'
        mock_get.return_value = mock_response

        url = 'https://example.com/api'
        service_key = 'test_service_key'
        start_date = '20240902'

        # 캡처된 로그를 검증
        with self.assertLogs('spark_jobs.utils', level='ERROR') as log:
            data_list = read_api(url, service_key, start_date)
            self.assertIn('요청 실패: 404, 응답 내용: Not Found', log.output[0])

        self.assertEqual(len(data_list), 0)  # 실패 시 데이터 리스트가 비어 있어야 함

if __name__ == '__main__':
    unittest.main()
