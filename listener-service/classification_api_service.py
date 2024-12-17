import requests

from pydantic import parse_obj_as
from schemas import ClassificationResult


class ClassificationApiService:
    def __init__(self, timeout: float = 10):
        self.endpoint = "http://classification-service:5000/predict"
        self.timeout = timeout
        self.http_session = requests.Session()

    def api_call(self, text: str) -> ClassificationResult:
        json_data = {"text": text}
        response = self.http_session.post(
            url=self.endpoint, json=json_data, timeout=self.timeout
        )
        data = response.json()
        print(f"response from classification-api {data}")
        result = parse_obj_as(ClassificationResult, data)
        return result
