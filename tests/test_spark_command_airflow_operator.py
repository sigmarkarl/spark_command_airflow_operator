import unittest

from spark_command_airflow_operator.src.operator.spark_command_submit_operator import SparkCommandSubmitOperator


class TestCommandAirflowOperator(unittest.TestCase):
    def setUp(self):
        pass

    def test_command_submit(self):
        cmd_submit = SparkCommandSubmitOperator(task_id='random_from_spark',
                                               name='Spark Random',
                                               endpoint_url='http://localhost:5002',
                                               code_type='SQL',
                                               code='select random()',
                                               result_type='text',
                                               result_url='')

        res = cmd_submit.execute(None)
        self.assertEqual(res, b'Hello, World!')


if __name__ == '__main__':
    unittest.main()
