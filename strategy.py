import pandas as pd
import numpy as np


def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Momentum Breakout Strategy:
    - Breakout from tight LTC range
    - BTC/ETH used only as market filter
    - Trailing stop via fixed holding
    """
    try:
        # Compute LTC range before renaming close column
        candles_target["ltc_range"] = (candles_target["high"] - candles_target["low"]) / candles_target["close"].shift(1)

        # Rename LTC close column
        candles_target = candles_target.rename(columns={"close": "close_LTC"})

        # Prepare merged dataframe
        df = candles_target[["timestamp", "close_LTC", "ltc_range"]].copy()
        df = df.merge(candles_anchor, on="timestamp", how="inner")

        # Calculate required features
        df["ltc_return"] = df["close_LTC"].pct_change().shift(1)
        df["range_rolling"] = df["ltc_range"].rolling(6).mean().shift(1)

        df["ret_btc"] = df["close_BTC"].pct_change().shift(1)
        df["ret_eth"] = df["close_ETH"].pct_change().shift(1)

        # Signal logic
        df["signal"] = "HOLD"
        holding = 0
        last_signal = "HOLD"

        for i in range(len(df)):
            if holding > 0:
                df.at[i, "signal"] = "HOLD"
                holding -= 1
                if holding == 0:
                    df.at[i, "signal"] = "SELL" if last_signal == "BUY" else "BUY"
                    last_signal = "HOLD"
            else:
                # Market filter: both BTC and ETH moving
                if abs(df.at[i, "ret_btc"]) > 0.003 and abs(df.at[i, "ret_eth"]) > 0.003:
                    # LTC breakout from quiet period
                    if abs(df.at[i, "ltc_return"]) > 0.015 and df.at[i, "range_rolling"] < 0.01:
                        if df.at[i, "ltc_return"] > 0:
                            df.at[i, "signal"] = "BUY"
                            last_signal = "BUY"
                            holding = 3
                        else:
                            df.at[i, "signal"] = "SELL"
                            last_signal = "SELL"
                            holding = 3

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
