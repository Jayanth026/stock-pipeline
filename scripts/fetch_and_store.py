import os
import sys
import logging
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

API_URL = "https://www.alphavantage.co/query"


def get_env_var(name: str, default=None, required: bool = False):
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value


def fetch_stock_data(symbol: str) -> dict:
    """Fetch daily stock data for the given symbol from Alpha Vantage."""
    api_key = get_env_var("ALPHAVANTAGE_API_KEY", required=True)

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
    }

    try:
        logging.info("Requesting data from Alpha Vantage for symbol=%s", symbol)
        resp = requests.get(API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # Alpha Vantage returns error info in JSON, not HTTP code
        if "Error Message" in data:
            raise RuntimeError(f"API error: {data['Error Message']}")
        if "Time Series (Daily)" not in data:
            raise RuntimeError("Unexpected API response: 'Time Series (Daily)' not found")

        return data["Time Series (Daily)"]

    except requests.exceptions.Timeout as e:
        logging.error("Timeout while fetching data: %s", e)
        raise
    except requests.exceptions.RequestException as e:
        logging.error("Request error while fetching data: %s", e)
        raise
    except ValueError as e:
        logging.error("Failed to parse JSON: %s", e)
        raise


def parse_time_series(symbol: str, time_series: dict):
    """
    Convert Alpha Vantage's time series into a list of rows for DB insertion.
    Each row: (symbol, date, open, high, low, close, volume)
    """
    rows = []
    for date_str, daily_data in time_series.items():
        try:
            trade_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            open_price = float(daily_data["1. open"])
            high_price = float(daily_data["2. high"])
            low_price = float(daily_data["3. low"])
            close_price = float(daily_data["4. close"])
            volume = int(float(daily_data["5. volume"]))  # sometimes string numeric

            rows.append(
                (
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                )
            )
        except (KeyError, ValueError) as e:
            # Skip badly formatted rows but continue processing
            logging.warning(
                "Skipping date %s due to parsing error: %s", date_str, e
            )
    return rows


def get_db_connection():
    user = get_env_var("POSTGRES_USER", required=True)
    password = get_env_var("POSTGRES_PASSWORD", required=True)
    db = get_env_var("POSTGRES_DB", required=True)
    host = get_env_var("POSTGRES_HOST", "postgres")  # service name in docker-compose
    port = get_env_var("POSTGRES_PORT", "5432")

    try:
        conn = psycopg2.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        logging.error("Failed to connect to Postgres: %s", e)
        raise


def ensure_table_exists(conn):
    create_sql = """
    CREATE TABLE IF NOT EXISTS daily_stock_prices (
        symbol TEXT NOT NULL,
        trade_date DATE NOT NULL,
        open NUMERIC(18, 6),
        high NUMERIC(18, 6),
        low NUMERIC(18, 6),
        close NUMERIC(18, 6),
        volume BIGINT,
        last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (symbol, trade_date)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)


def upsert_stock_data(conn, rows):
    if not rows:
        logging.warning("No rows to insert.")
        return

    insert_sql = """
        INSERT INTO daily_stock_prices
        (symbol, trade_date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, trade_date) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            last_updated = NOW();
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)
        conn.commit()
        logging.info("Inserted/updated %d rows successfully.", len(rows))
    except psycopg2.Error as e:
        conn.rollback()
        logging.error("Failed to upsert data: %s", e)
        raise


def main():
    symbol = get_env_var("STOCK_SYMBOL", "MSFT")
    logging.info("Starting stock pipeline for symbol=%s", symbol)

    try:
        time_series = fetch_stock_data(symbol)
        rows = parse_time_series(symbol, time_series)

        conn = get_db_connection()
        try:
            ensure_table_exists(conn)
            upsert_stock_data(conn, rows)
        finally:
            conn.close()

        logging.info("Pipeline completed successfully for symbol=%s", symbol)

    except Exception as e:
        # Generic catch-all so the task can fail cleanly in Airflow
        logging.error("Pipeline failed: %s", e)
        raise


if __name__ == "__main__":
    main()
