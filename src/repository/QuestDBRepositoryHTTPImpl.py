import json
import urllib.parse
import urllib.request
from typing import Any, Dict, List

from src.repository.IQuestDBRepository import IQuestDBRepository


class QuestDBRepositoryHTTPImpl(IQuestDBRepository):
    """QuestDBとHTTP REST APIでやりとりするリポジトリ実装"""

    def __init__(
        self,
        host: str,
        port: int,
    ):
        self.base_url = f"http://{host}:{port}"

    def write(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
    ):
        """
        データを書き込む (InfluxDB Line Protocol形式)
        :param data: 書き込むデータのリスト
        :param table_name: テーブル名
        """
        if not data:
            return

        # InfluxDB Line Protocol形式の文字列を生成
        lines = []
        for item in data:
            tags = []
            fields = []
            timestamp = ""
            for key, value in item.items():
                if key == "timestamp":
                    timestamp = f" {int(value.timestamp() * 1e9)}"  # ナノ秒
                elif isinstance(value, str):
                    fields.append(f'{key}="{value}"')
                elif isinstance(value, (int, float)):
                    fields.append(f"{key}={value}")
                elif isinstance(value, bool):
                    fields.append(f"{key}={str(value).lower()}")
                else:  # tagとして扱う
                    tags.append(f"{key}={value}")

            line = f"{table_name}"
            if tags:
                line += f",{','.join(tags)}"
            line += f" {','.join(fields)}{timestamp}"
            lines.append(line)

        payload = "\n".join(lines).encode("utf-8")

        req = urllib.request.Request(f"{self.base_url}/imp", data=payload, method="POST")
        with urllib.request.urlopen(req) as res:
            if res.status not in [200, 204]:
                raise Exception(f"Failed to write data: {res.read().decode()}")

    def read(
        self,
        query: str,
    ) -> List[Dict[str, Any]]:
        """データを読み込む"""
        params = urllib.parse.urlencode({"query": query})
        url = f"{self.base_url}/exp?{params}"

        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as res:
            response_body = res.read().decode("utf-8")
            response_json = json.loads(response_body)

            columns = [col["name"] for col in response_json["columns"]]
            dataset = response_json["dataset"]

            result = []
            for row in dataset:
                result.append(dict(zip(columns, row)))
            return result
