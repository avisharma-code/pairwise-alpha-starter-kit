import requests
import pandas as pd
import time

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

def fetch_ohlcv(symbol, interval, start_time_ms, end_time_ms):
    all_candles = []
    limit = 1000
    current_time = start_time_ms

    print(f"\n📥 Starting download for {symbol} ({interval})...")

    while current_time < end_time_ms:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": current_time,
            "endTime": end_time_ms,
            "limit": limit
        }

        response = requests.get(BINANCE_API_URL, params=params)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

        data = response.json()
        if not data:
            print("No more data returned.")
            break

        all_candles.extend(data)

        start_ts = pd.to_datetime(data[0][0], unit="ms")
        end_ts = pd.to_datetime(data[-1][0], unit="ms")
        print(f"Fetched {len(data)} rows: {start_ts} to {end_ts}")

        current_time = data[-1][0] + 1
        time.sleep(0.2)

    if not all_candles:
        raise Exception(f"No data returned for {symbol} from {start_time_ms} to {end_time_ms}")

    # Convert to DataFrame
    df = pd.DataFrame(all_candles, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

    return df


def fetch_all(symbols_with_timeframes, start_time, end_time):
    start_ms = int(pd.Timestamp(start_time).timestamp() * 1000)
    end_ms = int(pd.Timestamp(end_time).timestamp() * 1000)
    all_data = {}

    for name, (symbol, tf) in symbols_with_timeframes.items():
        df = fetch_ohlcv(symbol, tf, start_ms, end_ms)

        # Clip exactly to end_time for strict compliance
        df = df[df["timestamp"] < pd.to_datetime(end_time)]

        all_data[name] = df
        print(f"✅ Finished: {symbol} → {len(df)} rows retained (before {end_time})\n")

    return all_data


if __name__ == "__main__":
    # Define symbols and timeframes
    symbols_with_timeframes = {
    "target_ltc": ("LTCUSDT", "4h"),
    "anchor_btc": ("BTCUSDT", "4h"),
    "anchor_eth": ("ETHUSDT", "4h"),
    "anchor_sol": ("SOLUSDT", "4h"),
    }


    # Define strict time window
    START_DATE = "2025-01-01 00:00:00"
    END_DATE = "2025-05-09 00:00:00"  # do not exceed

    # Fetch and save
    data = fetch_all(symbols_with_timeframes, START_DATE, END_DATE)

    data['target_ltc'].to_csv("LTC_4H.csv", index=False)
    data['anchor_btc'].to_csv("BTC_4H.csv", index=False)
    data['anchor_eth'].to_csv("ETH_4H.csv", index=False)
    data['anchor_sol'].to_csv("SOL_4H.csv", index=False)
    print("Data saved")
