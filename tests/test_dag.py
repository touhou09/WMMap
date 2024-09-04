import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import unittest
from airflow.models import DagBag

class TestMyDAG(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id='spark_job_dag')
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 1)  # 'my_dag'가 3개의 태스크를 가질 것으로 기대

if __name__ == '__main__':
    unittest.main()