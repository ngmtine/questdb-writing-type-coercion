from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import DictCursor

from .IQuestDBRepository import IQuestDBRepository


class QuestDBRepositoryPostgreSQLImpl(IQuestDBRepository):
    """QuestDBとPostgreSQLワイヤプロトコルでやりとりするリポジトリ実装"""

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
    ):
        self.conn_str = f"host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}'"

    def write(
        self,
        data: list[dict[str, Any]],
        table_name: str,
    ):
        """
        データを書き込む
        :param data: 書き込むデータのリスト
        :param table_name: 書き込み先のテーブル名
        """
        if not data:
            return

        cols = data[0].keys()

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
                values = [[item[col].isoformat() if isinstance(item[col], datetime) else item[col] for col in cols] for item in data]
                cursor.executemany(sql, values)
            conn.commit()

    def read(
        self,
        query: str,
    ) -> list[dict[str, Any]]:
        """データを読み込む"""
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)  # pyright: ignore[reportUnknownMemberType]
                results = cursor.fetchall()
                return [dict(row) for row in results]
