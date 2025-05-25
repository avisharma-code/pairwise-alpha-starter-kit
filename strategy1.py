import pandas as pd
import numpy as np


def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Lagged Anchor Alpha v1.2: Restore & Cap
    - Original lagged anchor trigger logic
    - Fixed take-profit (+5%) and stop-loss (-3%)
    - Max hold 6 bars
    - No volatility filter
    """
    try:
        candles_target = candles_target.rename(columns={"close": "close_LTC"})
        df = candles_target[["timestamp", "close_LTC"]].copy()
        df = df.merge(candles_anchor, on="timestamp", how="inner")

        df["ret_btc"] = df["close_BTC"].pct_change()
        df["ret_eth"] = df["close_ETH"].pct_change()
        df["ret_ltc"] = df["close_LTC"].pct_change()

        df["anchor_pump"] = (df["ret_btc"].shift(1) > 0.01) | (df["ret_eth"].shift(1) > 0.01)
        df["anchor_dump"] = (df["ret_btc"].shift(1) < -0.01) | (df["ret_eth"].shift(1) < -0.01)

        df["lagged_pump"] = df["anchor_pump"] & (df["ret_ltc"].shift(1) < 0.002)
        df["lagged_dump"] = df["anchor_dump"] & (df["ret_ltc"].shift(1) > -0.002)

        df["signal"] = "HOLD"
        holding = 0
        last_signal = "HOLD"
        entry_price = None
        MAX_HOLD = 6

        for i in range(len(df)):
            price_now = df.at[i, "close_LTC"]

            if holding > 0:
                holding += 1
                change = price_now / entry_price - 1

                if last_signal == "BUY" and (change >= 0.05 or change <= -0.03):
                    df.at[i, "signal"] = "SELL"
                    holding = 0
                    last_signal = "HOLD"
                    continue
                elif last_signal == "SELL" and (-change >= 0.05 or -change <= -0.03):
                    df.at[i, "signal"] = "BUY"
                    holding = 0
                    last_signal = "HOLD"
                    continue

                if holding >= MAX_HOLD:
                    df.at[i, "signal"] = "SELL" if last_signal == "BUY" else "BUY"
                    holding = 0
                    last_signal = "HOLD"
                else:
                    df.at[i, "signal"] = "HOLD"
            else:
                if df.at[i, "lagged_pump"]:
                    df.at[i, "signal"] = "BUY"
                    entry_price = price_now
                    last_signal = "BUY"
                    holding = 1
                elif df.at[i, "lagged_dump"]:
                    df.at[i, "signal"] = "SELL"
                    entry_price = price_now
                    last_signal = "SELL"
                    holding = 1

        return df[["timestamp", "signal"]]

    except Exception as e:
        raise RuntimeError(f"Error in generate_signals: {e}")


def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": "LTC", "timeframe": "1H"},
        "anchors": [
            {"symbol": "BTC", "timeframe": "1H"},
            {"symbol": "ETH", "timeframe": "1H"}
        ]
    }
