import sqlite3
import pandas as pd
import requests
import time
import os

# Constants
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
LIMIT = 1000
DATABASE_DIR = "./data/raw/sqlite/"
DATABASE_FILE = "btc_data_binance_full.db"
DATABASE_PATH = os.path.join(DATABASE_DIR, DATABASE_FILE)
CSV_DIR = "./data/raw/csv/"

# Create necessary directories if they don't exist
os.makedirs(DATABASE_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Database connection and table creation
def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS btc_hourly (
        datetime TEXT UNIQUE,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL
    )
    """)
    conn.commit()
    return conn, cursor

# Fetch Binance data
def get_binance_klines(symbol, interval, limit, start_time=None):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time:
        params["startTime"] = start_time

    try:
        response = requests.get(BINANCE_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return []

# Process and clean data
def process_data(data):
    df = pd.DataFrame(data, columns=[
                      "timestamp", "open", "high", "low", "close", "volume", "_", "_", "_", "_", "_", "_"])
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.drop(columns=["timestamp"], inplace=True)
    df = df.drop_duplicates(subset=["datetime"], keep="last")
    return df

# Save data to CSV
def save_data_to_csv(df):
    df["year"] = df["datetime"].dt.year
    for year, year_data in df.groupby("year"):
        file_name = f"btc_data_{year}.csv"
        file_path = os.path.join(CSV_DIR, file_name)

        try:
            year_data.to_csv(file_path, mode='a',
                             header=not os.path.exists(file_path), index=False)
            print(f"{year}년 데이터 CSV 저장 완료!")
        except Exception as e:
            print(f"CSV 저장 중 오류 발생: {e}")

# Save data to SQLite
def save_data_to_sqlite(df, conn, cursor):
    for year, year_data in df.groupby(df["datetime"].dt.year):
        table_name = f"btc_hourly_{year}"

        try:
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                datetime TEXT UNIQUE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """)
            conn.commit()

            # 데이터 삽입 (중복된 datetime을 방지)
            year_data = year_data.drop(columns=["year"])  # "year" 컬럼을 제거
            year_data.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"{year}년 데이터 {len(year_data)}개 SQLite 저장 완료!")

        except sqlite3.DatabaseError as e:
            print(f"DB 오류 발생: {e}")
        except Exception as e:
            print(f"예상치 못한 오류 발생: {e}")


# Fetch full data and save
def fetch_and_save_data(conn, cursor, days=365 * 5):
    start_time = int((time.time() - days * 24 * 60 * 60) * 1000)

    while True:
        print(f"Fetching data from: {pd.to_datetime(start_time, unit='ms')}")
        data = get_binance_klines(SYMBOL, INTERVAL, LIMIT, start_time)

        if not data:
            print("더 이상 가져올 데이터가 없습니다!")
            break

        df = process_data(data)

        # Save to CSV and SQLite
        save_data_to_csv(df)
        save_data_to_sqlite(df, conn, cursor)

        # Update start_time for next batch
        start_time = data[-1][0] + 1
        time.sleep(1)


# Main execution
if __name__ == "__main__":
    conn, cursor = get_db_connection()
    fetch_and_save_data(conn, cursor, days=365 * 5)
    conn.close()
    print("모든 데이터 저장 완료!")
