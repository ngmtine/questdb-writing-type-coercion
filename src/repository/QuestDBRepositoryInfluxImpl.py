import socket
from typing import Any, Dict, List

from src.repository.IQuestDBRepository import IQuestDBRepository


class QuestDBRepositoryInfluxImpl(IQuestDBRepository):
    """QuestDBとInfluxDBラインプロトコルでやりとりするリポジトリ実装 (書き込み専用)"""

    def __init__(
        self,
        host: str,
        port: int,
    ):
        self.host = host
        self.port = port

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

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            sock.sendall(payload)
            sock.sendall(b"\n")  # InfluxDBでは最後に改行が必要

    def read(self, query: str) -> List[Dict[str, Any]]:
        """InfluxDBラインプロトコルは書き込み専用のため、このメソッドはサポートしない"""
        raise NotImplementedError("Reading is not supported via InfluxDB line protocol.")
