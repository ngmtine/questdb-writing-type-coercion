from typing import Any

import requests

from .IQuestDBRepository import IQuestDBRepository


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
        data: list[dict[str, Any]],
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
        lines: list[str] = []
        for item in data:
            tags: list[str] = []
            fields: list[str] = []
            timestamp = ""
            for key, value in item.items():
                if key == "timestamp":
                    # timestampはナノ秒精度
                    timestamp = f" {int(value.timestamp() * 1e9)}"
                elif isinstance(value, str):
                    # 文字列はダブルクォートで囲む
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

        payload = "\n".join(lines)

        # /imp エンドポイントに multipart/form-data でPOSTリクエスト
        # 'data'という名前のファイルとしてペイロードを送信
        files = {"data": ("influx_payload.txt", payload, "text/plain")}

        response = requests.post(f"{self.base_url}/imp", files=files)

        # エラーがあれば例外を発生させる
        response.raise_for_status()

    def read(
        self,
        query: str,
    ) -> list[dict[str, Any]]:
        """データを読み込む"""
        # /exp エンドポイントにGETリクエスト
        response = requests.get(f"{self.base_url}/exp", params={"query": query})
        response.raise_for_status()

        response_json = response.json()

        columns = [col["name"] for col in response_json["columns"]]
        dataset = response_json["dataset"]

        result: list[dict[str, Any]] = []
        for row in dataset:
            result.append(dict(zip(columns, row)))
        return result
