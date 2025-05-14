import pandas as pd

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy:
    - BUY LTC if BTC is pumping (> 0.75%) and LTC is lagging (< 0.1%) AND BTC volatility is low
    - SELL LTC if BTC is dumping (< -0.75%) and LTC is lagging (> -0.1%) AND BTC volatility is low
    - HOLD for 3 candles after entry
    """
    try:
        candles_anchor = candles_anchor.rename(columns={"close": "close_BTC"})
        candles_target = candles_target.rename(columns={"close": "close_LTC"})

        df = pd.merge(
            candles_target[["timestamp", "close_LTC"]],
            candles_anchor[["timestamp", "close_BTC"]],
            on="timestamp",
            how="inner"
        )

        df["btc_return"] = df["close_BTC"].pct_change().shift(1)
        df["ltc_return"] = df["close_LTC"].pct_change().shift(1)

        # Rolling volatility (standard deviation) of BTC
        df["btc_volatility"] = df["close_BTC"].pct_change().rolling(6).std().shift(1)

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
                # Filter on BTC volatility < 1.5%
                if pd.notnull(df.at[i, "btc_volatility"]) and df.at[i, "btc_volatility"] < 0.015:
                    if df.at[i, "btc_return"] > 0.0075 and df.at[i, "ltc_return"] < 0.001:
                        df.at[i, "signal"] = "BUY"
                        last_signal = "BUY"
                        holding = 2
                    elif df.at[i, "btc_return"] < -0.0075 and df.at[i, "ltc_return"] > -0.001:
                        df.at[i, "signal"] = "SELL"
                        last_signal = "SELL"
                        holding = 2

        return df[["timestamp", "signal"]]

    except Exception as e:
        raise RuntimeError(f"Error in generate_signals: {e}")


def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": "LTC", "timeframe": "1H"},
        "anchors": [{"symbol": "BTC", "timeframe": "1H"}]
    }
