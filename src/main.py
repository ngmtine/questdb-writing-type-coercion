import os
from datetime import datetime, timezone

from src.repository.QuestDBRepositoryPostgreSQLImpl import QuestDBRepositoryPostgreSQLImpl

# from src.repository.QuestDBRepositoryHTTPImpl import QuestDBRepositoryHTTPImpl
# from src.repository.QuestDBRepositoryInfluxImpl import QuestDBRepositoryInfluxImpl


def main():
    """
    メイン処理
    """
    # --- PostgreSQLワイヤプロトコルでの接続 ---
    # compose.ymlで設定したポートとQuestDBのデフォルト設定
    pg_repo = QuestDBRepositoryPostgreSQLImpl(
        host=os.getenv("QUESTDB_HOST", "localhost"),
        port=int(os.getenv("QUESTDB_PG_PORT", 18812)),
        dbname=os.getenv("QUESTDB_DB", "qdb"),
        user=os.getenv("QUESTDB_USER", "admin"),
        password=os.getenv("QUESTDB_PASSWORD", "quest"),
    )

    table_name = "my_metrics"

    # テーブル作成 (存在しない場合のみ)
    print("Creating table if not exists...")
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp TIMESTAMP,
        device_id STRING,
        temperature DOUBLE,
        humidity DOUBLE
    ) timestamp(timestamp) PARTITION BY DAY;
    """
    # readメソッドをテーブル作成クエリの実行に利用
    # DDLは結果を返さないので、try-exceptで囲む
    try:
        pg_repo.read(create_table_query)
    except Exception as e:
        # psycopg2.ProgrammingError: no results to fetch
        # DDL実行時はこのエラーが正常
        print(f"Table creation command executed. (Info: {e})")

    # 書き込みデータの準備
    data_to_write = [
        {
            "timestamp": datetime.now(timezone.utc),
            "device_id": "device_A",
            "temperature": 25.5,
            "humidity": 60.1,
        },
        {
            "timestamp": datetime.now(timezone.utc),
            "device_id": "device_B",
            "temperature": 26.1,
            "humidity": 58.8,
        },
    ]

    # データの書き込み
    print("\nWriting data...")
    pg_repo.write(data_to_write, table_name)
    print("Data written successfully.")

    # データの読み込み
    print("\nReading data...")
    select_query = f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT 10;"
    read_data = pg_repo.read(select_query)

    print("Read data:")
    for row in read_data:
        print(row)

    # --- 他の実装を利用する場合の例 ---
    # http_repo = QuestDBRepositoryHTTPImpl(host="localhost", port=19000)
    # http_repo.write(data_to_write, table_name)
    # read_data_http = http_repo.read(select_query)
    # print(read_data_http)

    # influx_repo = QuestDBRepositoryInfluxImpl(host="localhost", port=19009)
    # influx_repo.write(data_to_write, table_name)
    # influx_repo.read(select_query) # NotImplementedError


if __name__ == "__main__":
    main()
