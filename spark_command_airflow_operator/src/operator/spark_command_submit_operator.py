import requests
import json
from airflow.models.baseoperator import BaseOperator


class SparkCommandSubmitOperator(BaseOperator):
    endpoint_url: str
    name: str
    code_type: str
    code: str
    result_type: str
    result_url: str

    def __init__(self,
                 name: str,
                 endpoint_url: str,
                 code_type: str,
                 code: str,
                 result_type: str,
                 result_url: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.name = name
        self.code_type = code_type
        self.code = code
        self.result_type = result_type
        self.result_url = result_url

    def execute(self, context):
        payload = {"type": self.code_type, "code": self.code}
        json_payload = json.dumps(payload)
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.endpoint_url, data=json_payload, headers=headers)
        return response.text


