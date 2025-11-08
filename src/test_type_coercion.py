import os
import time
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

from repository.IQuestDBRepository import IQuestDBRepository
from repository.QuestDBRepositoryHTTPImpl import QuestDBRepositoryHTTPImpl
from repository.QuestDBRepositoryInfluxImpl import QuestDBRepositoryInfluxImpl
from repository.QuestDBRepositoryPostgreSQLImpl import QuestDBRepositoryPostgreSQLImpl

# .envファイルから環境変数を読み込む
load_dotenv()

# --- リポジトリのインスタンス化 ---
pg_repo = QuestDBRepositoryPostgreSQLImpl(
    host=os.getenv("QUESTDB_HOST"),  # pyright: ignore[reportArgumentType]
    port=int(os.getenv("QUESTDB_PG_PORT")),  # pyright: ignore[reportArgumentType]
    dbname=os.getenv("QUESTDB_DB"),  # pyright: ignore[reportArgumentType]
    user=os.getenv("QUESTDB_USER"),  # pyright: ignore[reportArgumentType]
    password=os.getenv("QUESTDB_PASSWORD"),  # pyright: ignore[reportArgumentType]
)

http_repo = QuestDBRepositoryHTTPImpl(
    host=os.getenv("QUESTDB_HOST"),  # pyright: ignore[reportArgumentType]
    port=int(os.getenv("QUESTDB_HTTP_PORT")),  # pyright: ignore[reportArgumentType]
)

influx_repo = QuestDBRepositoryInfluxImpl(
    host=os.getenv("QUESTDB_HOST"),  # pyright: ignore[reportArgumentType]
    port=int(os.getenv("QUESTDB_INFLUX_PORT")),  # pyright: ignore[reportArgumentType]
)

# --- テストデータ ---
TABLE_NAME = "type_coercion_test"

# テーブルを自動生成するためのデータ (temperature: float)
INITIAL_DATA = [
    {
        "timestamp": datetime.now(timezone.utc),
        "device_id": "device_test",
        "temperature": 25.5,
    }
]

# 型が異なるためエラーになることを期待するデータ (temperature: str)
INVALID_DATA = [
    {
        "timestamp": datetime.now(timezone.utc),
        "device_id": "device_error",
        "temperature": "high",
    }
]


def drop_table(
    repo: IQuestDBRepository,
    table_name: str,
):
    """テーブルを削除するヘルパー関数"""
    try:
        # DDLは結果を返さないので例外が発生する
        repo.read(f"DROP TABLE {table_name};")
    except psycopg2.ProgrammingError as e:
        # "no results to fetch" は正常な動作
        print(f"  -> Dropped table '{table_name}'. (Info: {e})")
    except Exception as e:
        # テーブルが存在しない場合など
        print(f"  -> Failed to drop table '{table_name}' or it did not exist. (Error: {e})")


def test_influx_write_with_invalid_type():
    """InfluxDBプロトコルでは不正な行は無視されることをテスト"""
    print("\n--- Testing InfluxDB Protocol (Ignores Invalid Rows) ---")
    try:
        # 1. 準備: InfluxDBプロトコルでテーブルを自動作成
        print(f"1. Creating table '{TABLE_NAME}' with initial data via InfluxDB...")
        influx_repo.write(INITIAL_DATA, TABLE_NAME)
        print("   -> Table created successfully.")

        # 2. 実行: 不正なデータを書き込む (例外は発生しない想定)
        print("2. Writing invalid data (temperature as string)...")
        influx_repo.write(INVALID_DATA, TABLE_NAME)
        print("   -> Write command executed (no exception expected).")

        # 3. 検証: 不正なデータが書き込まれていないことを確認
        print("3. Verifying that the invalid data was ignored...")
        time.sleep(1)  # データ反映を待つ
        data = pg_repo.read(f"SELECT * FROM {TABLE_NAME}")

        is_invalid_data_found = any(row["device_id"] == INVALID_DATA[0]["device_id"] for row in data)

        if not is_invalid_data_found:
            print(f"   [SUCCESS] Invalid data for device '{INVALID_DATA[0]['device_id']}' was not found.")
        else:
            print("   [FAILED] Invalid data was found in the table.")

    except Exception as e:
        print(f"   [FAILED] Caught an unexpected exception: {type(e).__name__}: {e}")

    finally:
        # 4. 後片付け: テーブルを削除
        print("4. Cleaning up...")
        drop_table(pg_repo, TABLE_NAME)


def test_http_write_with_invalid_type():
    """HTTPプロトコルでは不正な行は無視されることをテスト"""
    print("\n--- Testing HTTP Protocol (Ignores Invalid Rows) ---")
    try:
        # 1. 準備: InfluxDBプロトコルでテーブルを自動作成
        print(f"1. Creating table '{TABLE_NAME}' with initial data via InfluxDB...")
        influx_repo.write(INITIAL_DATA, TABLE_NAME)
        print("   -> Table created successfully.")

        # 2. 実行: 不正なデータを書き込む (例外は発生しない想定)
        print("2. Writing invalid data (temperature as string)...")
        http_repo.write(INVALID_DATA, TABLE_NAME)
        print("   -> Write command executed (no exception expected).")

        # 3. 検証: 不正なデータが書き込まれていないことを確認
        print("3. Verifying that the invalid data was ignored...")
        time.sleep(1)  # データ反映を待つ
        data = pg_repo.read(f"SELECT * FROM {TABLE_NAME}")

        is_invalid_data_found = any(row["device_id"] == INVALID_DATA[0]["device_id"] for row in data)

        if not is_invalid_data_found:
            print(f"   [SUCCESS] Invalid data for device '{INVALID_DATA[0]['device_id']}' was not found.")
        else:
            print("   [FAILED] Invalid data was found in the table.")

    except Exception as e:
        print(f"   [FAILED] Caught an unexpected exception: {type(e).__name__}: {e}")

    finally:
        # 4. 後片付け: テーブルを削除
        print("4. Cleaning up...")
        drop_table(pg_repo, TABLE_NAME)


def test_pg_write_with_invalid_type():
    """PostgreSQLプロトコルで不正な型のデータを書き込むテスト"""
    print("\n--- Testing PostgreSQL Protocol ---")
    try:
        # 1. 準備: InfluxDBプロトコルでテーブルを自動作成
        print(f"1. Creating table '{TABLE_NAME}' with initial data via InfluxDB...")
        influx_repo.write(INITIAL_DATA, TABLE_NAME)
        print("   -> Table created successfully.")

        # DEBUG: テーブルがPG経由で見えるか確認
        try:
            import time

            time.sleep(1)  # データの反映を待つ
            print("   -> Checking tables via PG protocol...")
            tables = pg_repo.read("SELECT table_name FROM tables();")
            print(f"   -> Found tables: {[t['table_name'] for t in tables]}")
        except Exception as e:
            print(f"   -> Error checking tables: {e}")

        # 2. 実行と検証: 不正なデータを書き込んでエラーが発生することを確認
        print("2. Writing invalid data (temperature as string)...")
        pg_repo.write(INVALID_DATA, TABLE_NAME)
        print("   [FAILED] Exception was not raised.")

    except psycopg2.Error as e:
        # 3. 検証: 期待通りのPostgreSQLエラーか確認
        print(f"   [SUCCESS] Successfully caught a psycopg2.Error: {e}")
    except Exception as e:
        print(f"   [FAILED] Caught an unexpected exception: {type(e).__name__}: {e}")

    finally:
        # 4. 後片付け: テーブルを削除
        print("4. Cleaning up...")
        drop_table(pg_repo, TABLE_NAME)


if __name__ == "__main__":
    test_influx_write_with_invalid_type()
    test_http_write_with_invalid_type()
    test_pg_write_with_invalid_type()
